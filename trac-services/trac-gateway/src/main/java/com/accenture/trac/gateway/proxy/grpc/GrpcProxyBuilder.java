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

package com.accenture.trac.gateway.proxy.grpc;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.gateway.proxy.http.Http1RouterLink;
import com.accenture.trac.gateway.proxy.http.Http1to2Framing;
import io.netty.channel.*;
import io.netty.handler.codec.http2.*;
import io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcProxyBuilder extends ChannelInitializer<Channel> {

    private static final String HTTP2_PREFACE_HANDLER = "HTTP2_PREFACE";
    private static final String HTTP2_FRAME_CODEC = "HTTP2_FRAME_CODEC";
    private static final String GRPC_PROXY_HANDLER = "GRPC_PROXY_HANDLER";
    private static final String GRPC_WEB_PROXY_HANDLER = "GRPC_WEB_PROXY_HANDLER";
    private static final String HTTP_1_TO_2_FRAMING = "HTTP_1_TO_2_FRAMING";
    private static final String HTTP_1_ROUTER_LINK = "HTTP_1_ROUTER_LINK";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ChannelHandlerContext routerCtx;
    private final ChannelPromise routeActivePromise;

    public GrpcProxyBuilder(ChannelHandlerContext routerCtx, ChannelPromise routeActivePromise) {
        this.routerCtx = routerCtx;
        this.routeActivePromise = routeActivePromise;
    }

    @Override
    protected void initChannel(Channel channel) {

        log.info("Init gRPC proxy channel");

        var pipeline = channel.pipeline();
        pipeline.addLast("OBJECT_LOGGER", new ObjectLogger());

        var initialSettings = new Http2Settings()
                .maxFrameSize(16 * 1024);

        log.info(initialSettings.toString());

        var http2Codec = Http2FrameCodecBuilder.forClient()
                .frameLogger(new Http2FrameLogger(LogLevel.INFO))
                .initialSettings(initialSettings)
                .autoAckSettingsFrame(true)
                .autoAckPingFrame(true)
                .build();

        pipeline.addLast(HTTP2_FRAME_CODEC, http2Codec);
        pipeline.addLast(GRPC_PROXY_HANDLER, new GrpcProxy());
        pipeline.addLast(GRPC_WEB_PROXY_HANDLER, new GrpcWebProxy());

        if (true /* HTTP 1 */) {

            pipeline.addLast(HTTP_1_TO_2_FRAMING, new Http1to2Framing());
            pipeline.addLast(HTTP_1_ROUTER_LINK, new Http1RouterLink(routerCtx, routeActivePromise));
        }
//        else if (false /* HTTP 2 */) {
//
//            pipeline.addLast(new Http2Sequencer(outerCtx, routeActivePromise));
//        }
        else
            throw new EUnexpected();
    }

    private static class ObjectLogger extends ChannelDuplexHandler {

        private final Logger log = LoggerFactory.getLogger(getClass());

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            log.info("LOGGER: handlerAdded");
            super.handlerAdded(ctx);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            log.info("LOGGER: handlerRemoved");
            super.handlerRemoved(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("LOGGER: channelActive");
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("LOGGER: channelInactive");
            super.channelInactive(ctx);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

            log.info("Writing an object of class {}", msg.getClass().getSimpleName());

            super.write(ctx, msg, promise);
        }
    }
}
