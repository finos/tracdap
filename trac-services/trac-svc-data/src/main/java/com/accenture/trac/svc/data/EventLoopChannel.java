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

package com.accenture.trac.svc.data;

import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class EventLoopChannel extends ManagedChannel {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ManagedChannel baseChannel;
    private final Channel virtualChannel;

    private final Map<String, ManagedChannel> eventLoopChannels;


    public static ManagedChannel wrapChannel(
            NettyChannelBuilder baseChannelBuilder,
            EventExecutorGroup eventLoopGroup) {

        return new EventLoopChannel(baseChannelBuilder, eventLoopGroup);
    }

    public EventLoopChannel(
            NettyChannelBuilder baseChannelBuilder,
            EventExecutorGroup eventLoopGroup) {

        this.baseChannel = baseChannelBuilder.build();

        this.eventLoopChannels = new HashMap<>();
        eventLoopGroup.forEach(el -> registerEventLoop(el, baseChannelBuilder));

        this.virtualChannel = ClientInterceptors.intercept(baseChannel, new EventLoopInterceptor());
    }

    private void registerEventLoop(EventExecutor eventLoop, NettyChannelBuilder baseChannelBuilder) {

        try {
            eventLoop.submit(() -> {

                var key = Thread.currentThread().getName();

                var channel = baseChannelBuilder
                        .eventLoopGroup((EventLoopGroup) eventLoop)
                        .build();

                eventLoopChannels.put(key, channel);

            }).await();
        }
        catch (InterruptedException e) {

            Thread.currentThread().interrupt();
        }
    }

    private class EventLoopInterceptor implements ClientInterceptor {

        @Override public <ReqT, RespT>
        ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions,
                Channel baseChannel) {

            var eventLoopKey = Thread.currentThread().getName();
            var channel = eventLoopChannels.get(eventLoopKey);

            if (channel != null)
                return channel.newCall(method, callOptions);

            log.warn("Client call running outside of parent event loop");

            return baseChannel.newCall(method, callOptions);
        }
    }

    @Override
    public ManagedChannel shutdown() {

        eventLoopChannels.values().forEach(ManagedChannel::shutdown);
        baseChannel.shutdown();

        return this;
    }

    @Override
    public boolean isShutdown() {

        if (!baseChannel.isShutdown())
            return false;

        for (var ch : eventLoopChannels.values())
            if (!ch.isShutdown())
                return false;

        return true;
    }

    @Override
    public boolean isTerminated() {

        if (!baseChannel.isTerminated())
            return false;

        for (var ch : eventLoopChannels.values())
            if (!ch.isTerminated())
                return false;

        return true;
    }

    @Override
    public ManagedChannel shutdownNow() {

        eventLoopChannels.values().forEach(ManagedChannel::shutdownNow);
        baseChannel.shutdownNow();

        return this;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {

        var timeoutStart = Instant.now();
        boolean ok = true;

        for (var ch : eventLoopChannels.values()) {

            var elapsed = Duration.between(timeoutStart, Instant.now());
            var remaining = elapsed.get(unit.toChronoUnit());

            var result = ch.awaitTermination(remaining, unit);

            ok = ok & result;
        }

        var elapsed = Duration.between(timeoutStart, Instant.now());
        var remaining = elapsed.get(unit.toChronoUnit());

        var result = baseChannel.awaitTermination(remaining, unit);

        return ok & result;
    }

    @Override
    public <RequestT, ResponseT>
    ClientCall<RequestT, ResponseT> newCall(
            MethodDescriptor<RequestT, ResponseT> methodDescriptor,
            CallOptions callOptions) {

        return virtualChannel.newCall(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
        return baseChannel.authority();
    }
}
