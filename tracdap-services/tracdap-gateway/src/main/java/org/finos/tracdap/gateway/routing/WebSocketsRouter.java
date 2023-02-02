/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.gateway.routing;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.config.GwProtocol;
import org.finos.tracdap.gateway.exec.Route;
import org.finos.tracdap.gateway.proxy.grpc.GrpcProtocol;
import org.finos.tracdap.gateway.proxy.grpc.GrpcProxyBuilder;
import org.finos.tracdap.gateway.proxy.http.HttpProtocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class WebSocketsRouter extends CoreRouter {

    private static final String WEBSOCKETS_PROTOCOL = "websockets";
    private static final Duration CLOSE_ON_ERROR_TIMEOUT = Duration.ofSeconds(5);
    private static final String TRAC_HEADER_PREFIX = "trac_";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String upgradeUri;
    private HttpHeaders upgradeHeaders;
    private int routeId = -1;

    private boolean upgradeComplete = false;
    private boolean firstMessageReceived = false;
    private boolean closeFrameSent = false;

    public WebSocketsRouter(List<Route> routes, int connId) {
        super(routes, connId, WEBSOCKETS_PROTOCOL);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // NETTY CHANNEL DUPLEX HANDLER API
    // -----------------------------------------------------------------------------------------------------------------


    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {

            var handshake = (WebSocketServerProtocolHandler.HandshakeComplete) evt;

            upgradeUri = handshake.requestUri();
            upgradeHeaders = handshake.requestHeaders();
            upgradeComplete = true;

            log.info("conn = {}, websockets handshake complete, sub-protocol = [{}]", connId, handshake.selectedSubprotocol());

            if (log.isTraceEnabled()) {
                log.trace("conn = {}, handshake headers: {}", connId, handshake.requestHeaders().toString());
            }
        }
        else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        try {

            // Do not touch any messages until the HTTP upgrade process is complete
            if (!upgradeComplete) {
                ReferenceCountUtil.retain(msg);
                super.channelRead(ctx, msg);
                return;
            }

            // Strict rules -this server is speaking web sockets, there shouldn't be random messages from other protocols
            if (!(msg instanceof WebSocketFrame)) {
                log.error("conn = {}, Unexpected message of type [{}]", connId, msg.getClass().getSimpleName());
                throw new EUnexpected();
            }

            var frame = (WebSocketFrame) msg;

            if (log.isTraceEnabled()) {

                log.trace("conn = {}, inbound websockets frame, size = [{}], type = [{}]",
                        connId, frame.content().readableBytes(), frame.getClass().getSimpleName());
            }

            // Handling for each known frame type

            if (frame instanceof BinaryWebSocketFrame) {
                processInboundBinaryFrame(ctx, (BinaryWebSocketFrame) frame);
            }

            else if (frame instanceof TextWebSocketFrame) {
                processInboundTextFrame(ctx);
            }

            else if (frame instanceof ContinuationWebSocketFrame) {
                processInboundContinuationFrame(ctx, (ContinuationWebSocketFrame) frame);
            }

            else if (frame instanceof PingWebSocketFrame) {
                processInboundPingFrame();
            }

            else if (frame instanceof PongWebSocketFrame) {
                processInboundPongFrame();
            }

            else if (frame instanceof CloseWebSocketFrame) {
                processInboundCloseFrame(ctx, (CloseWebSocketFrame) frame);
            }

            else {
                throw new EUnexpected();
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        try {

            // Do not touch any messages until the HTTP upgrade process is complete
            if (!upgradeComplete) {
                ReferenceCountUtil.retain(msg);
                super.write(ctx, msg, promise);
                return;
            }

            // Strict rules -this server is speaking web sockets, there shouldn't be random messages from other protocols
            if (!(msg instanceof WebSocketFrame)) {
                log.error("conn = {}, Unexpected message of type [{}]", connId, msg.getClass().getSimpleName());
                throw new EUnexpected();
            }

            // This is an outbound WS frame that came from the back end pipeline

            var frame = (WebSocketFrame) msg;

            if (log.isTraceEnabled()) {

                log.trace("conn = {}, outbound websockets frame, size = [{}], type = [{}]",
                        connId, frame.content().readableBytes(), frame.getClass().getSimpleName());
            }

            if (frame instanceof CloseWebSocketFrame) {

                // Explicit handling of the close sequence is required

                var closeFrame = (CloseWebSocketFrame) frame;
                processOutboundCloseFrame(ctx, closeFrame, promise);
            }

            else {

                // Assume outbound frames are good to relay, since they come from the gateway core

                // Make sure not to send any frames after the close frame is sent!
                // Google Chrome in particular will fail the entire request if the close sequence is wrong

                if (!closeFrameSent) {
                    ReferenceCountUtil.retain(msg);
                    ctx.write(frame, promise);
                }
            }
        }
        finally {
            destroyAssociation(msg);
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {

        // In the HTTP routers we can try to keep the connection alive if there are problems with one route
        // But the websocket router is very linear! If there's a problem, it doesn't make sense to stay open

        // Treating all exceptions that propagate to this handler as "unhandled"
        // It may be that some more sophisticated handling is possible / necessary
        var websocketsCode = WebSocketCloseStatus.INTERNAL_SERVER_ERROR;

        reportErrorAndClose(ctx, error.getMessage(), websocketsCode, error);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // TRAC CORE ROUTER API
    // -----------------------------------------------------------------------------------------------------------------


    @Override
    protected ChannelInitializer<Channel> initializeProxyRoute(
            ChannelHandlerContext ctx, CoreRouterLink link,
            Route routeConfig) {

        if (routeConfig.getConfig().getRouteType() != GwProtocol.GRPC)
            throw new EUnexpected();

        return new GrpcProxyBuilder(
                routeConfig.getConfig(), link, connId,
                HttpProtocol.WEBSOCKETS,
                GrpcProtocol.GRPC_WEBSOCKETS);
    }

    @Override
    protected void reportProxyRouteError(ChannelHandlerContext ctx, Throwable error, boolean direction) {

        // We could try to shut down just the back end connection that is affected
        // But since grpc-websockets is on request per connection anyway, there isn't much need

        // Treating all exceptions that propagate to this handler as "unhandled"
        // It may be that some more sophisticated handling is possible / necessary
        var websocketsCode = WebSocketCloseStatus.INTERNAL_SERVER_ERROR;

        reportErrorAndClose(ctx, error.getMessage(), websocketsCode, error);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // INBOUND FRAME HANDLING
    // -----------------------------------------------------------------------------------------------------------------


    private void processInboundBinaryFrame(ChannelHandlerContext ctx, BinaryWebSocketFrame frame) {

        if (!firstMessageReceived) {

            var uri = URI.create(upgradeUri);
            var route = lookupRoute(uri, HttpMethod.POST, 1);

            // Report an error in websockets if the route is not found
            if (route == null) {
                var statusCode = WebSocketCloseStatus.ENDPOINT_UNAVAILABLE;
                var message = String.format("No route found for [%s]", upgradeUri);
                reportErrorAndClose(ctx, message, statusCode, /* cause = */ null);
                return;
            }

            // If the target is not active, create a new connection (For WS it will always be a new connection)
            // This should always succeed inline, errors may be reported later
            getOrCreateTarget(ctx, route);
            routeId = route.getIndex();
            firstMessageReceived = true;

            // The first frame in grpc-websockets holds encoded HTTP headers
            // We want to add the headers from the upgrade request, particularly path
            var firstFrame = addUpgradeHeaders(frame);
            
            relayFrame(ctx, firstFrame, true);
        }
        else {

            relayFrame(ctx, frame, false);
        }
    }

    private void processInboundTextFrame(ChannelHandlerContext ctx) {

        // Don't allow web sockets TEXT frames, we don't support them so best to raise an error right away
        // Try to get some information back to the client using a close frame, otherwise kill the connection

        var statusCode = WebSocketCloseStatus.INVALID_MESSAGE_TYPE;
        var message = "Web socket text frames are not currently supported";

        reportErrorAndClose(ctx, message, statusCode, /* cause = */ null);
    }

    private void processInboundContinuationFrame(ChannelHandlerContext ctx, ContinuationWebSocketFrame frame) {

        // No continuation frames before the first message!
        if (!firstMessageReceived) {
            var statusCode = WebSocketCloseStatus.INVALID_MESSAGE_TYPE;
            var message = String.format("Invalid stream for [%s] (continuation before start)", upgradeUri);
            reportErrorAndClose(ctx, message, statusCode, /* cause = */ null);
            return;
        }
        
        relayFrame(ctx, frame, false);
    }

    private void processInboundPingFrame() {

        // No need to pong, websockets codec does it automatically

        if (log.isTraceEnabled()) {
            log.trace("conn = {}, inbound ping frame", connId);
        }
    }

    private void processInboundPongFrame() {

        // No need for further processing

        if (log.isTraceEnabled()) {
            log.trace("conn = {}, inbound pong frame", connId);
        }
    }

    private void processInboundCloseFrame(ChannelHandlerContext ctx, CloseWebSocketFrame frame) {

        // If both sides have sent a close frame already, then ok to close immediately
        if (closeFrameSent) {

            log.info("conn = {}, received close response, code = [{}]: {}",
                    connId, frame.statusCode(), frame.reasonText());

            ctx.close();
        }

        // Otherwise this is a new request, send the response frame before closing the channel
        else {

            log.info("conn = {}, received close request, code = [{}]: {}",
                    connId, frame.statusCode(), frame.reasonText());

            var closeResponse = new CloseWebSocketFrame(frame.statusCode(), frame.reasonText());
            var closePromise = ctx.newPromise();

            log.info("conn = {}, sending close response, code = [{}]: {}",
                    connId, frame.statusCode(), frame.reasonText());

            closeFrameSent = true;

            ctx.write(closeResponse, closePromise);
            ctx.flush();

            closePromise.addListener(x -> ctx.close());
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // OUTBOUND FRAME HANDLING
    // -----------------------------------------------------------------------------------------------------------------

    private void processOutboundCloseFrame(ChannelHandlerContext ctx, CloseWebSocketFrame frame, ChannelPromise promise) {

        // An outbound close frame comes from the proxy back end, it is not a response to a client request
        // We will always need to wait for the client to respond
        // If a client request had been received, the response would be processed already

        if (closeFrameSent) {

            log.warn("conn = {}, ignoring duplicate outbound close request [{}]: {}",
                    connId, frame.statusCode(), frame.reasonText());
        }

        else {

            log.info("conn = {}, sending close request, code = [{}]: {}",
                    connId, frame.statusCode(), frame.reasonText());

            ReferenceCountUtil.retain(frame);
            closeFrameSent = true;

            ctx.write(frame, promise);
            ctx.flush();
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS / UTILS
    // -----------------------------------------------------------------------------------------------------------------


    private BinaryWebSocketFrame addUpgradeHeaders(BinaryWebSocketFrame firstFrame) {

        // Passing through a very restricted set of headers from the upgrade request
        // This is all that is needed currently for the platform services to work
        // In particular the auth token header will be passed through
        // The list could be expanded if needed in future

        var extraHeaders = new StringBuilder();

        extraHeaders.append(":method: POST\r\n");
        extraHeaders.append(":scheme: http\r\n");
        extraHeaders.append(":path: ").append(upgradeUri).append("\r\n");

        for (var header : upgradeHeaders) {

            var headerName = header.getKey().toLowerCase();

            if (headerName.startsWith(TRAC_HEADER_PREFIX)) {
                extraHeaders.append(header.getKey().toLowerCase());
                extraHeaders.append(": ");
                extraHeaders.append(header.getValue());
                extraHeaders.append("\r\n");
            }
        }

        var extraHeaderBytes = Unpooled.copiedBuffer(extraHeaders, StandardCharsets.US_ASCII);

        var combinedHeaders = Unpooled.compositeBuffer();
        combinedHeaders.addComponent(extraHeaderBytes);
        combinedHeaders.addComponent(firstFrame.content().retain());

        combinedHeaders.writerIndex(
                extraHeaderBytes.writerIndex() +
                firstFrame.content().writerIndex());

        return new BinaryWebSocketFrame(combinedHeaders);
    }
    
    private void relayFrame(ChannelHandlerContext ctx, WebSocketFrame frame, boolean needRelease) {

        try {

            var target = getTarget(routeId);

            // This could happen if the proxy route has closed while messages are still coming in
            // There is no way to recover, so we have to close the client connection
            if (target == null) {
                var statusCode = WebSocketCloseStatus.ENDPOINT_UNAVAILABLE;
                var message = String.format("Proxy connection has been closed for [%s]", upgradeUri);
                reportErrorAndClose(ctx, message, statusCode, /* cause = */ null);
                return;
            }

            // We don't know when the incoming stream finishes, without digging into the contents of the stream
            // Simplify by flushing for each message, the proxy queues will buffer anyway

            relayMessage(target, frame.retain());
            flushMessages(target);
        }
        catch (Throwable e) {
            if (needRelease)
                ReferenceCountUtil.release(frame);
            throw e;
        }
    }

    private void reportErrorAndClose(
            ChannelHandlerContext ctx, String message,
            WebSocketCloseStatus websocketsCode,
            Throwable cause) {

        if (cause != null) {

            log.error("conn = {}, sending close request with code [{}, {}]: {}",
                    connId,
                    websocketsCode.code(),
                    websocketsCode.reasonText(),
                    message,
                    cause);
        }
        else {

            log.error("conn = {}, sending close request with code [{}, {}]: {}",
                    connId,
                    websocketsCode.code(),
                    websocketsCode.reasonText(),
                    message);
        }

        var closePromise = ctx.newPromise();
        var closeRequest = new CloseWebSocketFrame(websocketsCode, message);
        ctx.writeAndFlush(closeRequest, closePromise);

        ctx.executor().schedule(
                () -> { if (!closePromise.isDone()) ctx.close(); },
                CLOSE_ON_ERROR_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

}