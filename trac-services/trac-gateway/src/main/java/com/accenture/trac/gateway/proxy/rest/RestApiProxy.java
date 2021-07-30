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

package com.accenture.trac.gateway.proxy.rest;

import com.accenture.trac.api.TracMetadataApiGrpc;
import com.accenture.trac.common.exception.EUnexpected;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.grpc.stub.AbstractFutureStub;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class RestApiProxy extends Http2ChannelDuplexHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String grpcHost;
    private final short grpcPort;

    private ManagedChannel serviceChannel;
    private AbstractFutureStub<? extends AbstractFutureStub<?>> stub;

    private final Map<Http2FrameStream, RestApiCallState> callStateMap;


    public RestApiProxy(String grpcHost, short grpcPort) {
        this.grpcHost = grpcHost;
        this.grpcPort = grpcPort;

        this.callStateMap = new HashMap<>();
    }

    @Override
    protected void handlerAdded0(ChannelHandlerContext ctx) {

        serviceChannel = ManagedChannelBuilder.forAddress(grpcHost, grpcPort)
                .userAgent("TRAC/Gateway")
                .usePlaintext()
                .disableRetry()
                .executor(ctx.executor())
                .build();

        stub = TracMetadataApiGrpc.newFutureStub(serviceChannel);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        // REST proxy layer expects all messages to be HTTP/2 frames
        if (!(msg instanceof Http2Frame))
            throw new EUnexpected();

        var frame = (Http2Frame) msg;

        if (frame instanceof Http2HeadersFrame) {

            var headers = (Http2HeadersFrame) frame;
            var stream = headers.stream();

            if (!callStateMap.containsKey(stream)) {

                log.info("New outbound frame in REST API proxy");

                var callState = new RestApiCallState();
                callStateMap.put(stream, callState);
            }
        }

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

    }

    private class RestApiCallState {

    }
}
