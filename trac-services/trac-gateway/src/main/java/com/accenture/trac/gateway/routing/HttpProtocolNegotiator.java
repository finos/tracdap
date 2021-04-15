/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.gateway.routing;

import com.accenture.trac.common.exception.EUnexpected;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.*;
import io.netty.util.AsciiString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;


public class HttpProtocolNegotiator extends ChannelInitializer<SocketChannel> {

    private static final String PROTOCOL_SELECTOR_HANDLER = "protocol_selector";
    private static final String HTTP_1_INITIALIZER = "http_1_initializer";
    private static final String HTTP_1_KEEPALIVE = "http_1_keepalive";
    private static final String HTTP_2_INITIALIZER = "http_2_initializer";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Supplier<ChannelInboundHandlerAdapter> http1Handler;
    private final Supplier<ChannelInboundHandlerAdapter> http2Handler;

    public HttpProtocolNegotiator(
            Supplier<ChannelInboundHandlerAdapter> http1Handler,
            Supplier<ChannelInboundHandlerAdapter> http2Handler) {

        this.http1Handler = http1Handler;
        this.http2Handler = http2Handler;
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

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpMessage msg) throws Exception {

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();
            var httpVersion = msg.protocolVersion();

            log.info("{} {} CLEARTEXT", remoteSocket, httpVersion);

            pipeline.addAfter(HTTP_1_INITIALIZER, HTTP_1_KEEPALIVE, new HttpServerKeepAliveHandler());
            pipeline.addAfter(HTTP_1_KEEPALIVE, null, http1Handler.get());
            pipeline.remove(this);

            ctx.fireChannelRead(msg);
        }
    }

    private class Http2Initializer extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

            var pipeline = ctx.pipeline();
            var remoteSocket = ctx.channel().remoteAddress();

            log.info("{} {} CLEARTEXT", remoteSocket, "HTTP/2");

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
