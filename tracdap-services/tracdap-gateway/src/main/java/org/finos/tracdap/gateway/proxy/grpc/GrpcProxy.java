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

package org.finos.tracdap.gateway.proxy.grpc;

import io.grpc.Status;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.*;
import org.finos.tracdap.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;


public class GrpcProxy extends Http2ChannelDuplexHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int connId;

    public GrpcProxy(int connId) {
        this.connId = connId;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        // gRPC proxy layer expects all messages to be HTTP/2 frames
        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        var frame = (Http2Frame) msg;

        if (frame instanceof Http2HeadersFrame) {

            var headersFrame = (Http2HeadersFrame) frame;

            if (headersFrame.headers().contains(":path"))
                logRequestStart(headersFrame, promise);
        }

        super.write(ctx, msg, promise);
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        // gRPC proxy layer expects all messages to be HTTP/2 frames
        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        var frame = (Http2Frame) msg;

        if (frame instanceof Http2HeadersFrame) {

            var headersFrame = (Http2HeadersFrame) frame;

            if (headersFrame.headers().contains("grpc-status"))
                logResponseComplete(headersFrame);
        }

        super.channelRead(ctx, msg);
    }

    private void logRequestStart(Http2HeadersFrame headersFrame, ChannelPromise promise) {

        var headers = headersFrame.headers();
        var path = headers.path();

        promise.addListener(f -> f.addListener(f2 -> log.info("conn = {}, stream = {}, REQUEST {}",
                connId, headersFrame.stream().id(), path)));
    }

    private void logResponseComplete(Http2HeadersFrame headersFrame) {

        var headers = headersFrame.headers();
        var grpcStatusCode = headers.getInt("grpc-status");
        var grpcStatus = Status.fromCodeValue(grpcStatusCode);

        if (headers.contains("grpc-message")) {

            var grpcMessage = headers.get("grpc-message").toString();

            log.info("conn = {}, stream = {}, RESPONSE CODE = {} ({}), MESSAGE = {}",
                    connId, headersFrame.stream().id(),
                    grpcStatus.getCode().name(), grpcStatusCode, grpcMessage);
        }

        else {

            log.info("conn = {}, stream = {}, RESPONSE CODE = {} ({})",
                    connId, headersFrame.stream().id(),
                    grpcStatus.getCode().name(), grpcStatusCode);
        }
    }
}
