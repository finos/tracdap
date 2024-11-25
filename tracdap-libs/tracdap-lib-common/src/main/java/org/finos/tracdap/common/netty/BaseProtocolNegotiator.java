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

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
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


public abstract class BaseProtocolNegotiator extends ChannelInitializer<SocketChannel> {

    private static final String UPGRADE_HANDLER = "upgrade_handler";
    private static final String TIMEOUT_HANDLER = "timeout_handler";

    private static final String HTTP_1_INITIALIZER = "http_1_initializer";
    private static final String HTTP_1_CODEC = "http_1_codec";
    private static final String HTTP_1_AUTH = "http_1_auth";
    private static final String HTTP_1_KEEPALIVE = "http_1_keepalive";

    private static final String HTTP_2_CODEC = "http_2_codec";
    private static final String HTTP_2_AUTH = "http_2_AUTH";

    private static final String WS_INITIALIZER = "ws_initializer";
    private static final String WS_FRAME_CODEC = "ws_frame_codec";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final boolean h2Enabled;
    private final boolean h2cEnabled;
    private final boolean wsEnabled;
    private final int idleTimeout;

    public BaseProtocolNegotiator(boolean h2Enabled, boolean h2cEnabled, boolean wsEnabled, int idleTimeout) {

        this.h2Enabled = h2Enabled;
        this.h2cEnabled = h2cEnabled;
        this.wsEnabled = wsEnabled;
        this.idleTimeout = idleTimeout;
    }

    protected abstract ChannelInboundHandler http1AuthHandler();
    protected abstract ChannelInboundHandler http2AuthHandler();

    protected abstract ChannelHandler http1PrimaryHandler();
    protected abstract ChannelHandler http2PrimaryHandler();
    protected abstract ChannelHandler wsPrimaryHandler();

    protected abstract WebSocketServerProtocolConfig wsProtocolConfig();


    @Override
    protected final void initChannel(SocketChannel channel) {

        var remoteSocket = channel.remoteAddress();

        log.info("New connection from [{}]", remoteSocket);

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

            log.info("Request for protocol upgrade: [{}]", protocol);

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

            log.warn("Upgrade not available for protocol: {}", protocol);

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

            ctx.pipeline().addAfter(ctx.name(), WS_INITIALIZER, new WebSocketInitializer());
        }
    }

    private static class WebSocketUpgradeInterceptor extends ChannelOutboundHandlerAdapter {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

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

            var channel = ctx.channel();
            var pipeline = channel.pipeline();
            var remoteSocket = channel.remoteAddress();
            var protocol = "HTTP/1";

            log.info("Selected protocol: {} {}", remoteSocket, protocol);

            // Auth handler goes at the front of the pipeline (HTTP codec is already installed)
            // This is a regular HTTP auth, modified to redirect browsers to the auth service on failure
            var authHandler = http1AuthHandler();
            pipeline.addAfter(HTTP_1_INITIALIZER, HTTP_1_AUTH, authHandler);

            // Keep alive handler will close connections not marked as keep-alive when a request is complete
            pipeline.addAfter(HTTP_1_AUTH, HTTP_1_KEEPALIVE, new HttpServerKeepAliveHandler());

            // For connections that are kept alive, we need to handle timeouts
            // This idle state handler will trigger idle events after the configured timeout
            // The CoreRouter class is responsible for handling the idle events
            var idleHandler = new IdleStateHandler(idleTimeout, idleTimeout, idleTimeout, TimeUnit.SECONDS);
            pipeline.addAfter(HTTP_1_KEEPALIVE, TIMEOUT_HANDLER, idleHandler);

            // The main HTTP/1 handler
            var primaryHandler = http1PrimaryHandler();
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

                if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {

                    var upgrade = (HttpServerUpgradeHandler.UpgradeEvent) evt;

                    var pipeline = ctx.pipeline();
                    var remoteSocket = ctx.channel().remoteAddress();
                    var protocol = upgrade.protocol();

                    log.info("Selected protocol: {} {}", remoteSocket, protocol);

                    // Depending on how HTTP/2 is set up, the codec may or may not have been installed
                    Http2FrameCodec http2Codec = pipeline.get(Http2FrameCodec.class);
                    String http2CodecName;

                    if (http2Codec == null) {
                        http2Codec = Http2FrameCodecBuilder.forServer().build();
                        http2CodecName = HTTP_2_CODEC;
                        pipeline.addAfter(ctx.name(), http2CodecName, http2Codec);
                    }
                    else {
                        http2CodecName = pipeline.context(http2Codec).name();
                    }

                    // Auth handler comes immediately after the codec
                    var authHandler = http2AuthHandler();
                    pipeline.addAfter(http2CodecName, HTTP_2_AUTH, authHandler);

                    // The main HTTP/2 handler
                    var primaryHandler = http2PrimaryHandler();
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

                if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {

                    var upgrade = (HttpServerUpgradeHandler.UpgradeEvent) evt;
                    var request = upgrade.upgradeRequest();

                    var pipeline = ctx.pipeline();
                    var remoteSocket = ctx.channel().remoteAddress();
                    var protocol = upgrade.protocol();

                    log.info("Selected protocol: {} {}", remoteSocket, protocol);

                    pipeline.addAfter(ctx.name(), HTTP_1_CODEC, new HttpServerCodec());

                    // Auth handler goes immediately after the HTTP codec
                    // Since WS piggybacks on HTTP/1, use the HTTP/1 auth handler

                    var authHandler = http1AuthHandler();
                    pipeline.addAfter(HTTP_1_CODEC, HTTP_1_AUTH, authHandler);

                    // WebSockets connections also need to use idle handler
                    // E.g. buggy client code might forget to send the EOS signal or close the connection
                    // The CoreRouter class is responsible for handling the idle events
                    var idleHandler = new IdleStateHandler(idleTimeout, idleTimeout, idleTimeout, TimeUnit.SECONDS);
                    pipeline.addAfter(HTTP_1_AUTH, TIMEOUT_HANDLER, idleHandler);


                    // Do not include compression codec at the WS level
                    // Compression happens at the gRPC level for individual message blocks
                    // Those message flow through different hops and protocols, including WS and HTTP/2
                    // Compressing / decompressing on each hop is particularly inefficient since the payload is compressed
                    // Quick testing shows a roughly 10% performance gain in Chrome from removing WS-level compression


                    // Configure the WS protocol handler - path must match the URI in the upgrade request

                    // Do not auto-reply to close frames as this can lead to protocol errors
                    // Chrome in particular is very fussy and will fail a whole request if the close sequence is wrong
                    // The close sequence is managed with the client explicitly in WebSocketsRouter

                    var wsProtocolConfig = wsProtocolConfig();
                    var wsProtocolHandler = new WebSocketServerProtocolHandler(wsProtocolConfig);
                    pipeline.addAfter(TIMEOUT_HANDLER, WS_FRAME_CODEC, wsProtocolHandler);

                    var primaryHandler = wsPrimaryHandler();
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

}