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

package org.finos.tracdap.gateway.proxy.grpc;

import io.grpc.Status;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.*;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.LoggingHelpers;
import org.slf4j.Logger;

import javax.annotation.Nonnull;


public class GrpcProxy extends Http2ChannelDuplexHandler {

    private static final ThreadLocal<Logger> logMap = new ThreadLocal<>();
    private final Logger log = LoggingHelpers.threadLocalLogger(this, logMap);

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

        ctx.write(msg, promise);
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

        ctx.fireChannelRead(msg);
    }

    private void logRequestStart(Http2HeadersFrame headersFrame, ChannelPromise promise) {

        var headers = headersFrame.headers();
        var path = headers.path();

        promise.addListener(f ->
                log.info("GRPC REQUEST: conn = {}, stream = {}, {}",
                connId, headersFrame.stream().id(), path));
    }

    private void logResponseComplete(Http2HeadersFrame headersFrame) {

        var headers = headersFrame.headers();
        var grpcStatusCode = headers.getInt("grpc-status");
        var grpcStatus = Status.fromCodeValue(grpcStatusCode);

        if (headers.contains("grpc-message")) {

            var grpcMessage = headers.get("grpc-message").toString();

            log.info("GRPC RESPONSE: conn = {}, stream = {}, code = {} ({}), {}",
                    connId, headersFrame.stream().id(),
                    grpcStatus.getCode().name(), grpcStatusCode, grpcMessage);
        }

        else {

            log.info("GRPC RESPONSE: conn = {}, stream = {}, code = {} ({})",
                    connId, headersFrame.stream().id(),
                    grpcStatus.getCode().name(), grpcStatusCode);
        }
    }
}
