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

package org.finos.tracdap.gateway;

import org.finos.tracdap.common.auth.external.Http1AuthHandler;
import org.finos.tracdap.common.auth.external.IAuthProvider;
import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.config.GatewayConfig;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http2.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class ProtocolNegotiator extends ChannelInitializer<SocketChannel> {

    private static final String PROTOCOL_SELECTOR_HANDLER = "protocol_selector";

    private static final String HTTP_1_INITIALIZER = "http_1_initializer";
    private static final String HTTP_1_CODEC = "http_1_codec";
    private static final String HTTP_1_KEEPALIVE = "http_1_keepalive";
    private static final String HTTP_1_TIMEOUT = "http_1_timeout";
    private static final String HTTP_1_AUTH = "http_1_auth";

    private static final String HTTP_2_CODEC = "http_2_codec";

    private static final String WS_INITIALIZER = "ws_initializer";
    private static final String WS_COMPRESSION = "ws_compression";
    private static final String WS_FRAME_CODEC = "ws_frame_codec";

    private static final int MAX_TIMEOUT = 3600;
    private static final int DEFAULT_TIMEOUT = 60;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final GatewayConfig config;
    private final int idleTimeout;
    private final IAuthProvider authProvider;
    private final JwtProcessor jwtProcessor;

    private final ProtocolSetup<?> http1Handler;
    private final ProtocolSetup<?> http2Handler;
    private final ProtocolSetup<WebSocketServerProtocolConfig> webSocketsHandler;

    private final AtomicInteger connId = new AtomicInteger();

    public ProtocolNegotiator(
            GatewayConfig config, IAuthProvider authProvider, JwtProcessor jwtProcessor,
            ProtocolSetup<?> http1Handler, ProtocolSetup<?> http2Handler,
            ProtocolSetup<WebSocketServerProtocolConfig> webSocketsHandler) {

        this.config = config;

        this.idleTimeout = config.getIdleTimeout() > 0
                ? config.getIdleTimeout()
                : DEFAULT_TIMEOUT;

        this.authProvider = authProvider;
        this.jwtProcessor = jwtProcessor;

        this.http1Handler = http1Handler;
        this.http2Handler = http2Handler;
        this.webSocketsHandler = webSocketsHandler;
    }

    @Override
    protected void initChannel(SocketChannel channel) {

        var remoteSocket = channel.remoteAddress();

        log.info("New connection from [{}]", remoteSocket);

        var httpCodec = new HttpServerCodec();
        var http1Init = new Http1Initializer();
        var http2Init = new Http2Initializer();

        // Upgrade factory, gives out upgrade codec if HTTP upgrade is requested
        var upgradeFactory = new UpgradeCodecFactory();

        // Upgrade handler, reads the initial HTTP request and decides whether to call the upgrade factory
        // The alternate version (class below) has different behavior for failed upgrades
        var upgradeHandler = new HttpServerUpgradeHandler(httpCodec, upgradeFactory);

        // The clear text handler handles prior knowledge upgrades to HTTP/2 on clear text (i.e. PRI)
        var upgradeHandlerAux = new CleartextHttp2ServerUpgradeHandler(httpCodec, upgradeHandler, http2Init);

        // Initial pipeline has two steps:
        // - The upgrade handler, that will set up the pipeline for any upgrade protocols
        // - The init handler for HTTP/1, which will be called if no upgrade occurs
        var pipeline = channel.pipeline();
        pipeline.addLast(PROTOCOL_SELECTOR_HANDLER, upgradeHandlerAux);
        pipeline.addLast(HTTP_1_INITIALIZER, http1Init);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // UPGRADE MECHANISM
    // -----------------------------------------------------------------------------------------------------------------


    // Upgrade codec factory is responsible for giving out upgrade codecs for supported protocols
    // If no codec is available, HTTP spec says the request should continue on the original protocol
    // In practice this often leads to errors for gRPC or web sockets, it might be better to send an error

    private class UpgradeCodecFactory implements HttpServerUpgradeHandler.UpgradeCodecFactory {

        @Override
        public HttpServerUpgradeHandler.UpgradeCodec newUpgradeCodec(CharSequence protocol) {

            log.info("Request for protocol upgrade: [{}]", protocol);

            if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {

                var http2Codec = Http2FrameCodecBuilder.forServer().build();
                return new Http2ServerUpgradeCodec(http2Codec, new Http2Initializer());
            }

            if (AsciiString.contentEquals(Http2CodecUtil.TLS_UPGRADE_PROTOCOL_NAME, protocol)) {

                var http2Codec = Http2FrameCodecBuilder.forServer().build();
                return new Http2ServerUpgradeCodec(http2Codec, new Http2Initializer());
            }


            if (AsciiString.contentEquals("websocket", protocol)) {

                // Web sockets support may not be enabled
                if (webSocketsHandler != null)
                    return new WebsocketUpgradeCodec();
            }

            log.warn("Upgrade not available for protocol: " + protocol);

            return null;
        }
    }


    // Upgrade codec for websockets

    // The WebSocketProtocolHandler and HttpServerUpgradeHandler do not work on the same principle
    // WebSocketProtocolHandler includes everything for handling a web sockets connection, including the upgrade
    // HttpServerUpgradeHandler assumes the codec sends the upgrade response, before handing off to the new protocol
    // In order to get the right response from the upgrade handler we would need to take the handshake logic
    // out of the web socket handler, which would be very messy and break the internal state of that handler

    // The work-around solution here is to intercept and discard the upgrade response sent by the upgrade handler
    // The web socket handler will send its own upgrade response later, which is the one to reach the client
    // This allows the web socket initializer to be installed by the HTTP upgrade handler mechanism

    // The alternative, also a work-around, would be to ignore the HTTP upgrade handler altogether for web sockets
    // Then we would need to check the first inbound message in the HTTP/1 channel initializer
    // If a web sockets upgrade requests is seen, the HTTP/1 initializer would then defer to the WS initializer

    private static final String WEBSOCKETS_UPGRADE_INTERCEPT_HEADER = "X-Websockets-Upgrade-Intercept";
    private static final String WEBSOCKETS_UPGRADE_INTERCEPT_MAGIC = "dRThBVai/as3b&ex.==";

    private class WebsocketUpgradeCodec implements HttpServerUpgradeHandler.UpgradeCodec {

        @Override
        public Collection<CharSequence> requiredUpgradeHeaders() {
            return List.of();
        }

        @Override
        public boolean prepareUpgradeResponse(ChannelHandlerContext ctx, FullHttpRequest upgradeRequest, HttpHeaders upgradeHeaders) {

            // The upgrade headers prepared here are what gets sent back to the client by the HTTP upgrade handler
            // These do not include the required web sockets fields, such as sec-websocket-key
            // Calling the web socket handshake logic here would (a) be messy and (b) break the web sockets handler
            // Instead, we install an interceptor to catch and discard the generated headers
            // The special header added is used as the signal to the interceptor

            var currentHandler = ctx.name();
            ctx.pipeline().addBefore(currentHandler, null, new WebSocketUpgradeInterceptor());

            upgradeHeaders.set(WEBSOCKETS_UPGRADE_INTERCEPT_HEADER, WEBSOCKETS_UPGRADE_INTERCEPT_MAGIC);

            return true;
        }

        @Override
        public void upgradeTo(ChannelHandlerContext ctx, FullHttpRequest upgradeRequest) {

            var currentHandler = ctx.name();
            ctx.pipeline().addAfter(currentHandler, WS_INITIALIZER, new WebSocketInitializer());
        }
    }

    private static class WebSocketUpgradeInterceptor extends ChannelOutboundHandlerAdapter {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

            // Catch and discard the outbound response if the special intercept header is detected
            // Once the intercept is performed, this handler can be removed

            if (msg instanceof HttpResponse) {

                var resp = (HttpResponse) msg;
                var headers = resp.headers();
                var intercept = headers.get(WEBSOCKETS_UPGRADE_INTERCEPT_HEADER);

                if (intercept != null && intercept.equals(WEBSOCKETS_UPGRADE_INTERCEPT_MAGIC)) {
                    ctx.pipeline().remove(this);
                    ReferenceCountUtil.release(msg);
                    return;
                }
            }

            super.write(ctx, msg, promise);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PROTOCOL INITIALIZERS
    // -----------------------------------------------------------------------------------------------------------------


    private class Http1Initializer extends ChannelInboundHandlerAdapter {

        // Set up the HTTP/1 pipeline in response to the first inbound message
        // This initializer is called if there was no attempt at HTTP upgrade (or the upgrade attempt failed)

        @Override
        public void channelRead(ChannelHandlerContext ctx, @Nonnull Object msg) {

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();
            var protocol = "HTTP/1";
            var conn = connId.getAndIncrement();

            log.info("Selected protocol: {} {}", remoteSocket, protocol);

            // Keep alive handler will close connections not marked as keep-alive when a request is complete
            pipeline.addAfter(HTTP_1_INITIALIZER, HTTP_1_KEEPALIVE, new HttpServerKeepAliveHandler());

            // For connections that are kept alive, we need to handle timeouts
            // This idle state handler will trigger idle events after the configured timeout
            // The main Http1Router is responsible for handling the idle events
            var idleHandler = new IdleStateHandler(MAX_TIMEOUT, MAX_TIMEOUT, idleTimeout, TimeUnit.SECONDS);
            pipeline.addAfter(HTTP_1_KEEPALIVE, HTTP_1_TIMEOUT, idleHandler);

            // auth processor asks for two auth providers, a browse-based one and an api-based one
            // Currently we are only passing in a browser-based provider
            // E.g. this can redirect the user to federated auth services
            // Different approaches are needed for system-to-system auth

            var authHandler = new Http1AuthHandler(
                    config.getAuthentication(),
                    Http1AuthHandler.FRONT_FACING,
                    conn, jwtProcessor,
                    authProvider, null);

            pipeline.addAfter(HTTP_1_TIMEOUT, HTTP_1_AUTH, authHandler);

            // The main HTTP/1 handler
            pipeline.addLast(http1Handler.create(conn));

            // Since this handler is not based on ChannelInitializer
            // We need to remove it explicitly and re-trigger the first message
            pipeline.remove(this);
            ctx.fireChannelRead(msg);
        }
    }

    private class Http2Initializer extends ChannelInboundHandlerAdapter {

        // Set up the HTTP/2 pipeline in response to an HTTP upgrade event
        // This will be called after the HTTP upgrade is processed, and before the first inbound HTTP/2 message

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            if (!(evt instanceof HttpServerUpgradeHandler.UpgradeEvent)) {
                super.userEventTriggered(ctx, evt);
                return;
            }

            var upgrade = (HttpServerUpgradeHandler.UpgradeEvent) evt;

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();
            var protocol = upgrade.protocol();

            log.info("Selected protocol: {} {}", remoteSocket, protocol);

            // Depending on how HTTP/2 is set up, the codec may or may not have been installed
            if (pipeline.get(Http2FrameCodec.class) == null) {
                var http2InitName = pipeline.context(this).name();
                var http2Codec = Http2FrameCodecBuilder.forServer().build();
                pipeline.addAfter(http2InitName, HTTP_2_CODEC, http2Codec);
            }

            // The main HTTP/2 handler
            pipeline.addLast(http2Handler.create(connId.getAndIncrement()));

            pipeline.remove(this);
            pipeline.remove(HTTP_1_INITIALIZER);

            // The HTTP/2 pipeline does not expect to see the original HTTP/1 upgrade request
            // This has already been responded to by the upgrade handler
            // Now the pipeline is reconfigured, we can discard this event and wait for the first HTTP/2 message

            ReferenceCountUtil.release(evt);
        }
    }

    private class WebSocketInitializer extends ChannelInboundHandlerAdapter {

        // Set up the HTTP/2 pipeline in response to an HTTP upgrade event
        // This will be called after the HTTP upgrade is processed, and before the first inbound HTTP/2 message

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            if (!(evt instanceof HttpServerUpgradeHandler.UpgradeEvent)) {
                super.userEventTriggered(ctx, evt);
                return;
            }

            var upgrade = (HttpServerUpgradeHandler.UpgradeEvent) evt;
            var req = upgrade.upgradeRequest();

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();
            var protocol = upgrade.protocol();

            log.info("Selected protocol: {} {}", remoteSocket, protocol);

            pipeline.addAfter(WS_INITIALIZER, HTTP_1_CODEC, new HttpServerCodec());
            pipeline.addAfter(HTTP_1_CODEC, WS_COMPRESSION, new WebSocketServerCompressionHandler());

            // Configure the WS protocol handler - path must match the URI in the upgrade request

            // Do not auto-reply to close frames as this can lead to protocol errors
            // Chrome in particular is very fussy and will fail a whole request if the close sequence is wrong
            // The close sequence is managed with the client explicitly in WebSocketsRouter

            var wsConfig = webSocketsHandler.config()
                    .toBuilder()
                    .websocketPath(req.uri())
                    .handleCloseFrames(false)
                    .build();

            pipeline.addAfter(WS_COMPRESSION, WS_FRAME_CODEC, new WebSocketServerProtocolHandler(wsConfig));

            // Ådd the main handler - this should be the WebTransportRouter when the full service is running
            pipeline.addLast(webSocketsHandler.create(connId.getAndIncrement()));

            pipeline.remove(this);
            pipeline.remove(HTTP_1_INITIALIZER);

            // During the upgrade process, the original upgrade response was intercepted and discarded
            // The websocket protocol handler expects to see the upgrade request and respond to it
            // So, re-fire the original upgrade request into the reconfigured pipeline

            ctx.fireChannelRead(req);
        }
    }

}