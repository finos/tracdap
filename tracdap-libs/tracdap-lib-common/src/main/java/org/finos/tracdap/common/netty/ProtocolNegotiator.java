/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.netty;

import org.finos.tracdap.common.middleware.NettyConcern;
import org.finos.tracdap.common.middleware.SupportedProtocol;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http2.*;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;


public class ProtocolNegotiator extends ChannelInitializer<SocketChannel> {

    private static final String UPGRADE_HANDLER = "upgrade_handler";
    private static final String HTTP_1_INITIALIZER = "http_1_initializer";
    private static final String HTTP_1_CODEC = "http_1_codec";
    private static final String HTTP_2_CODEC = "http_2_codec";
    private static final String WS_INITIALIZER = "ws_initializer";
    private static final String WS_FRAME_CODEC = "ws_frame_codec";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final boolean h2Enabled;
    private final boolean h2cEnabled;
    private final boolean wsEnabled;

    private final ProtocolHandler mainHandler;
    private final NettyConcern commonConcerns;

    private final ConnectionId connectionId = new ConnectionId();

    public ProtocolNegotiator(ProtocolHandler mainHandler, NettyConcern commonConcerns) {

        this.h2Enabled = mainHandler.http2Supported();
        this.h2cEnabled = mainHandler.http2Supported();
        this.wsEnabled = mainHandler.websocketSupported();

        this.mainHandler = mainHandler;
        this.commonConcerns = commonConcerns;
    }

    @Override
    protected final void initChannel(SocketChannel channel) {

        connectionId.assign(channel);

        if (log.isTraceEnabled())
            log.trace("BaseProtocolNegotiator initChannel: conn = {}, {}", ConnectionId.get(channel), channel.remoteAddress());

        var pipeline = channel.pipeline();

        var httpCodec = new HttpServerCodec();
        var http1Init = new Http1Initializer();

        // Upgrade factory, gives out upgrade codec if HTTP upgrade is requested
        var upgradeFactory = new UpgradeCodecFactory();

        // Upgrade handler, reads the initial HTTP request and decides whether to call the upgrade factory
        // The alternate version (class below) has different behavior for failed upgrades
        var upgradeHandler = new HttpServerUpgradeHandler(httpCodec, upgradeFactory);

        if (h2cEnabled) {

            // The clear text handler handles prior knowledge upgrades to HTTP/2 on clear text (i.e. PRI)
            var http2Init = new Http2Initializer();
            var priUpgradeHandler = new CleartextHttp2ServerUpgradeHandler(httpCodec, upgradeHandler, http2Init);

            pipeline.addLast(UPGRADE_HANDLER, priUpgradeHandler);
        }
        else {

            pipeline.addLast(UPGRADE_HANDLER, upgradeHandler);
        }

        // Add the init handler for HTTP/1, which will be called if no upgrade occurs
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

            if (log.isTraceEnabled())
                log.trace("UpgradeCodecFactory newUpgradeCodec: protocol = {}", protocol);

            if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {

                if (h2cEnabled) {
                    var http2Codec = Http2FrameCodecBuilder.forServer().build();
                    return new Http2ServerUpgradeCodec(http2Codec, new Http2Initializer());
                }
            }

            if (AsciiString.contentEquals(Http2CodecUtil.TLS_UPGRADE_PROTOCOL_NAME, protocol)) {

                if (h2Enabled) {
                    var http2Codec = Http2FrameCodecBuilder.forServer().build();
                    return new Http2ServerUpgradeCodec(http2Codec, new Http2Initializer());
                }
            }

            if (AsciiString.contentEquals("websocket", protocol)) {

                if (wsEnabled) {
                    return new WebsocketUpgradeCodec();
                }
            }

            log.warn("HTTP upgrade not available for protocol: [{}]", protocol);

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

            if (log.isTraceEnabled())
                log.trace("WebsocketUpgradeCodec prepareUpgradeResponse: conn = {}", ConnectionId.get(ctx.channel()));

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

            if (log.isTraceEnabled())
                log.trace("WebsocketUpgradeCodec upgradeTo: conn = {}", ConnectionId.get(ctx.channel()));

            ctx.pipeline().addAfter(ctx.name(), WS_INITIALIZER, new WebSocketInitializer());
        }
    }

    private class WebSocketUpgradeInterceptor extends ChannelOutboundHandlerAdapter {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

            if (log.isTraceEnabled())
                log.trace("WebSocketUpgradeInterceptor write: conn = {}, msg = {}", ConnectionId.get(ctx.channel()), msg);

            // Catch and discard the outbound response if the special intercept header is detected
            // Once the intercept is performed, this handler can be removed

            if (msg instanceof HttpResponse) {

                var response = (HttpResponse) msg;
                var headers = response.headers();
                var intercept = headers.get(WEBSOCKETS_UPGRADE_INTERCEPT_HEADER);

                if (intercept != null && intercept.equals(WEBSOCKETS_UPGRADE_INTERCEPT_MAGIC)) {
                    ReferenceCountUtil.release(msg);
                    ctx.pipeline().remove(this);
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
        public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

            if (log.isTraceEnabled())
                log.trace("Http1Initializer channelRead: conn = {}, msg = {}", ConnectionId.get(ctx.channel()), msg);

            var channel = ctx.channel();
            var pipeline = channel.pipeline();

            if (msg instanceof HttpRequest)
                logNewConnection(channel, ((HttpRequest) msg).protocolVersion().toString());
            else
                logNewConnection(channel, "HTTP/1.1");

            // Depending on which upgrades have been set up, the HTTP codec may or may not be installed
            var httpCodec = pipeline.get(HttpServerCodec.class);

            if (httpCodec == null)
                pipeline.addAfter(ctx.name(), HTTP_1_CODEC, new HttpServerCodec());

            // Use common framework for cross-cutting concerns
            commonConcerns.configureInboundChannel(pipeline, SupportedProtocol.HTTP);

            // The main HTTP/1 handler
            var primaryHandler = mainHandler.createHttpHandler();
            pipeline.addLast(primaryHandler);

            pipeline.remove(this);
            ctx.fireChannelRead(msg);
        }
    }

    private class Http2Initializer extends ChannelInboundHandlerAdapter {

        // Set up the HTTP/2 pipeline in response to an HTTP upgrade event
        // This will be called after the HTTP upgrade is processed, and before the first inbound HTTP/2 message

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            try {

                if (log.isTraceEnabled())
                    log.trace("Http2Initializer userEventTriggered: conn = {}, evt = {}", ConnectionId.get(ctx.channel()), evt);

                if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {

                    var channel = ctx.channel();
                    var pipeline = ctx.pipeline();
                    var upgrade = (HttpServerUpgradeHandler.UpgradeEvent) evt;

                    logNewUpgradeConnection(channel, "HTTP/2.0", upgrade);

                    // Depending on how HTTP/2 is set up, the codec may or may not have been installed
                    var http2Codec = pipeline.get(Http2FrameCodec.class);

                    if (http2Codec == null)
                        pipeline.addAfter(ctx.name(), HTTP_2_CODEC, Http2FrameCodecBuilder.forServer().build());

                    // Use common framework for cross-cutting concerns
                    commonConcerns.configureInboundChannel(pipeline, SupportedProtocol.HTTP_2);

                    // The main HTTP/2 handler
                    var primaryHandler = mainHandler.createHttp2Handler();
                    pipeline.addLast(primaryHandler);

                    pipeline.remove(this);
                    pipeline.remove(HTTP_1_INITIALIZER);

                    // The HTTP/2 pipeline does not expect to see the original HTTP/1 upgrade request
                    // This has already been responded to by the upgrade handler
                    // Now the pipeline is reconfigured, we can discard this event and wait for the first HTTP/2 message
                }
                else {

                    ReferenceCountUtil.retain(evt);
                    super.userEventTriggered(ctx, evt);
                }
            }
            finally {
                ReferenceCountUtil.release(evt);
            }
        }
    }

    private class WebSocketInitializer extends ChannelInboundHandlerAdapter {

        // Set up the Web Sockets pipeline in response to an HTTP upgrade event

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            try {

                if (log.isTraceEnabled())
                    log.trace("WebSocketInitializer userEventTriggered: conn = {}, evt = {}", ConnectionId.get(ctx.channel()), evt);

                if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {

                    var channel = ctx.channel();
                    var pipeline = ctx.pipeline();
                    var upgrade = (HttpServerUpgradeHandler.UpgradeEvent) evt;
                    var request = upgrade.upgradeRequest();

                    logNewUpgradeConnection(channel, "WebSocket", upgrade);

                    // HTTP codec is needed to bootstrap WS protocol
                    // It is not set up automatically by the upgrade codec
                    pipeline.addAfter(ctx.name(), HTTP_1_CODEC, new HttpServerCodec());

                    // Configure the WS protocol handler - path must match the URI in the upgrade request
                    // Do not auto-reply to close frames as this can lead to protocol errors
                    // Chrome in particular is very fussy and will fail a whole request if the close sequence is wrong
                    // The close sequence is managed with the client explicitly in WebSocketsRouter
                    var wsProtocolConfig = mainHandler.createWebSocketConfig(request);
                    var wsProtocolHandler = new WebSocketServerProtocolHandler(wsProtocolConfig);
                    pipeline.addAfter(HTTP_1_CODEC, WS_FRAME_CODEC, wsProtocolHandler);

                    // Use common framework for cross-cutting concerns
                    commonConcerns.configureInboundChannel(pipeline, SupportedProtocol.WEB_SOCKETS);

                    // The main WebSockets handler
                    var primaryHandler = mainHandler.createWebsocketHandler();
                    pipeline.addLast(primaryHandler);

                    pipeline.remove(this);
                    pipeline.remove(HTTP_1_INITIALIZER);

                    // During the upgrade process, the original upgrade response was intercepted and discarded
                    // The websocket protocol handler expects to see the upgrade request and respond to it
                    // So, re-fire the original upgrade request into the reconfigured pipeline

                    ReferenceCountUtil.retain(request);
                    ctx.fireChannelRead(request);
                }
                else {

                    ReferenceCountUtil.retain(evt);
                    super.userEventTriggered(ctx, evt);
                }
            }
            finally {
                ReferenceCountUtil.release(evt);
            }
        }
    }

    private void logNewConnection(Channel channel, String protocol) {

        log.info("NEW CONNECTION: conn = {}, {} {}", ConnectionId.get(channel), channel.remoteAddress(), protocol);

        channel.closeFuture().addListener(close -> logCloseConnection((ChannelFuture) close));
    }

    private void logNewUpgradeConnection(Channel channel, String protocol, HttpServerUpgradeHandler.UpgradeEvent upgrade) {

        log.info("NEW CONNECTION: conn = {}, {} {} ({})", ConnectionId.get(channel), channel.remoteAddress(), protocol, upgrade.protocol());

        channel.closeFuture().addListener(close -> logCloseConnection((ChannelFuture) close));
    }

    private void logCloseConnection(ChannelFuture close) {

        log.info("CLOSE CONNECTION: conn = {}", ConnectionId.get(close.channel()));
    }
}
