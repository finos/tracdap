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
import org.finos.tracdap.config.GatewayConfig;
import org.finos.tracdap.gateway.exec.Route;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AsciiString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;


public class HttpProtocolNegotiator extends ChannelInitializer<SocketChannel> {

    private static final String PROTOCOL_SELECTOR_HANDLER = "protocol_selector";
    private static final String HTTP_1_INITIALIZER = "http_1_initializer";
    private static final String HTTP_1_KEEPALIVE = "http_1_keepalive";
    private static final String HTTP_1_TIMEOUT = "http_1_timeout";

    private static final int MAX_TIMEOUT = 3600;
    private static final int DEFAULT_TIMEOUT = 60;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int idleTimeout;

    private final AtomicInteger nextConnectionId = new AtomicInteger();
    private final Supplier<ChannelInboundHandlerAdapter> http1Handler;
    private final Supplier<ChannelInboundHandlerAdapter> http2Handler;


    public HttpProtocolNegotiator(GatewayConfig config, List<Route> routes) {

        int idleTimeout;

        try {
            idleTimeout = config.getIdleTimeout();
        }
        catch (Exception e) {
            idleTimeout = DEFAULT_TIMEOUT;
        }

        this.idleTimeout = idleTimeout;

        this.http1Handler = () -> new Http1Router(routes, nextConnectionId.getAndIncrement());
        this.http2Handler = () -> new Http2Router(config.getRoutesList());
    }

    @Override
    protected void initChannel(SocketChannel channel) {

        var httpCodec = new HttpServerCodec();
        var http1Init = new Http1Initializer();
        var http2Init = new Http2Initializer();

        var upgradeFactory = new UpgradeCodecFactory();  // Provides http2Handler
        var upgradeHandler = new HttpServerUpgradeHandler(httpCodec, upgradeFactory);
        var cleartextUpgradeHandler = new CleartextHttp2ServerUpgradeHandler(httpCodec, upgradeHandler, http2Init);

        var pipeline = channel.pipeline();
        pipeline.addLast(PROTOCOL_SELECTOR_HANDLER, cleartextUpgradeHandler);
        pipeline.addLast(HTTP_1_INITIALIZER, http1Init);

        pipeline.remove(this);
    }

    private class Http1Initializer extends SimpleChannelInboundHandler<HttpMessage> {

        // Using an inbound handler instead of a regular initializer lets us see the first message on the channel
        // We can get the HTTP protocol version from that

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpMessage msg) {

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();
            var httpVersion = msg.protocolVersion();

            log.info("New connection: {} {} CLEARTEXT", remoteSocket, httpVersion);

            // Keep alive handler will close connections not marked as keep-alive when a request is complete
            pipeline.addAfter(HTTP_1_INITIALIZER, HTTP_1_KEEPALIVE, new HttpServerKeepAliveHandler());

            // For connections that are kept alive, we need to handle timeouts
            // This idle state handler will trigger idle events after the configured timeout
            // The main Http1Router is responsible for handling the idle events
            var idleHandler = new IdleStateHandler(MAX_TIMEOUT, MAX_TIMEOUT, idleTimeout, TimeUnit.SECONDS);
            pipeline.addAfter(HTTP_1_KEEPALIVE, HTTP_1_TIMEOUT, idleHandler);

            // The main Http1Router instance
            pipeline.addLast(http1Handler.get());

            // Since this handler is not based on ChannelInitializer,
            // We need to remove it explicitly and re-trigger the first message
            pipeline.remove(this);
            ctx.fireChannelRead(msg);
        }
    }

    private class Http2Initializer extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();
            var httpVersion = "HTTP/2.0";

            log.info("New connection: {} {} CLEARTEXT", remoteSocket, httpVersion);

            if (pipeline.get(Http2FrameCodec.class) == null) {

                var http2Codec = Http2FrameCodecBuilder.forServer().build();
                var http2InitName = pipeline.context(this).name();
                pipeline.addAfter(http2InitName, null, http2Codec);

                var http2CodecName = pipeline.context(Http2FrameCodec.class).name();
                pipeline.addAfter(http2CodecName, null, http2Handler.get());
            }
            else {

                var http2InitName = pipeline.context(this).name();
                pipeline.addAfter(http2InitName, null, http2Handler.get());
            }

            pipeline.remove(this);
            pipeline.remove(HTTP_1_INITIALIZER);

            ctx.fireChannelRead(msg);
        }
    }

    private class UpgradeCodecFactory implements HttpServerUpgradeHandler.UpgradeCodecFactory {

        @Override
        public HttpServerUpgradeHandler.UpgradeCodec newUpgradeCodec(CharSequence protocol) {

            if (!AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol))
                throw new EUnexpected();

            var http2Codec = Http2FrameCodecBuilder.forServer().build();
            return new Http2ServerUpgradeCodec(http2Codec, new Http2Initializer());
        }
    }

}
