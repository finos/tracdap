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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2ChannelDuplexHandler;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcWebProxy extends Http2ChannelDuplexHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (msg instanceof Http2HeadersFrame) {

            log.info("Translating headers for message of type {}", msg.getClass().getSimpleName());

            var headersFrame = (Http2HeadersFrame) msg;
            var contentType = headersFrame.headers().get("content-type");

            if (contentType.toString().startsWith("application/grpc-web")) {

                var grpcContentType = contentType.toString().replace("grpc-web", "grpc");
                headersFrame.headers().remove("content-type");
                headersFrame.headers().add("content-type", grpcContentType);
            }
        }

        ctx.write(msg, promise);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
    }
}
