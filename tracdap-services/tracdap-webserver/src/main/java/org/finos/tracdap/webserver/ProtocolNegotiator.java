/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.webserver;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;

import org.finos.tracdap.common.auth.internal.JwtValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class ProtocolNegotiator extends ChannelInitializer<SocketChannel> {

    private static final String PROTOCOL_SELECTOR_HANDLER = "protocol_selector";
    private static final String CLIENT_TIMEOUT = "client_timeout";

    private static final String HTTP_1_INITIALIZER = "http_1_initializer";
    private static final String HTTP_1_KEEPALIVE = "http_1_keepalive";

    private static final String HTTP_2_CODEC = "http_2_codec";

    private static final int MAX_TIMEOUT = 3600;
    private static final int DEFAULT_TIMEOUT = 60;

    private static final Logger log = LoggerFactory.getLogger(ProtocolNegotiator.class);

    private final JwtValidator jwtValidator;
    private final Supplier<Http1Server> http1Server;
    private final Supplier<Http2Server> http2Server;

    public ProtocolNegotiator(
            JwtValidator jwtValidator,
            Supplier<Http1Server> http1Server,
            Supplier<Http2Server> http2Server) {

        this.jwtValidator = jwtValidator;
        this.http1Server = http1Server;
        this.http2Server = http2Server;
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

            log.warn("Upgrade not available for protocol: " + protocol);

            return null;
        }
    }


    private class Http1Initializer extends ChannelInboundHandlerAdapter {

        // Set up the HTTP/1 pipeline in response to the first inbound message
        // This initializer is called if there was no attempt at HTTP upgrade (or the upgrade attempt failed)

        @Override
        public void channelRead(ChannelHandlerContext ctx, @Nonnull Object msg) {

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();
            var protocol = "HTTP/1";

            log.info("Selected protocol: {} {}", remoteSocket, protocol);

            // Keep alive handler will close connections not marked as keep-alive when a request is complete
            pipeline.addAfter(HTTP_1_INITIALIZER, HTTP_1_KEEPALIVE, new HttpServerKeepAliveHandler());

            // For connections that are kept alive, we need to handle timeouts
            // This idle state handler will trigger idle events after the configured timeout
            // The CoreRouter class is responsible for handling the idle events
            var idleHandler = new IdleStateHandler(MAX_TIMEOUT, MAX_TIMEOUT, DEFAULT_TIMEOUT, TimeUnit.SECONDS);
            pipeline.addAfter(HTTP_1_KEEPALIVE, CLIENT_TIMEOUT, idleHandler);

            // The main HTTP/1 handler
            pipeline.addLast(http1Server.get());

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
            pipeline.addLast(http2Server.get());

            pipeline.remove(this);
            pipeline.remove(HTTP_1_INITIALIZER);

            // The HTTP/2 pipeline does not expect to see the original HTTP/1 upgrade request
            // This has already been responded to by the upgrade handler
            // Now the pipeline is reconfigured, we can discard this event and wait for the first HTTP/2 message

            ReferenceCountUtil.release(evt);
        }
    }
}
