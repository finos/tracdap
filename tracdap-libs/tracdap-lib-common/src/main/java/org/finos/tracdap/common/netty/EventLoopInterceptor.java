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

import org.finos.tracdap.common.exception.ETracInternal;

import io.grpc.*;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.OrderedEventExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Interceptor to put gRPC client call responses onto the same Netty EL as the caller.
 * <br/>
 *
 * This is for async services running in the event loop using directExecutor(), that want
 * to make client calls and have the response come back on the same EL. If the response is
 * on a different EL, this intercept will post the response task to the calling EL.
 * <br/>
 *
 * You can also use EventLoopScheduler to try and assign the client call to the service EL
 * before the call starts. If successful, this will avoid the need for context switching
 * altogether. The scheduler is not guaranteed to be successful all the time, so the
 * interceptor is still needed.
 */
public class EventLoopInterceptor implements ClientInterceptor {

    private final EventLoopResolver eventLoopResolver;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final boolean strict;
    private final boolean warning;

    public EventLoopInterceptor(EventExecutorGroup eventLoopGroup) {
        this(eventLoopGroup, true, true);
    }

    public EventLoopInterceptor(EventExecutorGroup eventLoopGroup, boolean strict, boolean warning) {
        this(new EventLoopResolver(eventLoopGroup), strict, warning);
    }

    public EventLoopInterceptor(EventLoopResolver eventLoopResolver) {
        this(eventLoopResolver, true, true);
    }

    public EventLoopInterceptor(EventLoopResolver eventLoopResolver, boolean strict, boolean warning) {
        this.eventLoopResolver = eventLoopResolver;
        this.strict = strict;
        this.warning = warning;
    }

    @Override public <ReqT, RespT>
    ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions options,
            Channel channel) {

        // Enforce strict requirement on the event loop
        // Client interceptor is used when the data service makes calls to other TRAC services
        // Those calls must come back on the same EL, so processing is not split across ELs

        var eventLoop = eventLoopResolver.currentEventLoop(false);

        if (eventLoop == null) {

            var message = "gRPC client call is running outside the registered event loop group";

            if (strict) {
                log.error(message);
                throw new ETracInternal(message);
            }

            if (warning) {
                log.warn(message);
            }
        }

        var nextCall = channel.newCall(method, options);

        return new EventLoopCall<>(eventLoop, nextCall);
    }

    private static class EventLoopCall< ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall< ReqT, RespT > {

        private final OrderedEventExecutor eventLoop;

        public EventLoopCall(OrderedEventExecutor eventLoop, ClientCall<ReqT, RespT> delegate) {
            super(delegate);
            this.eventLoop = eventLoop;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            var listener = new EventLoopListener<>(eventLoop, responseListener);
            delegate().start(listener, headers);
        }
    }

    private static class EventLoopListener<RespT> extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT> {

        private final OrderedEventExecutor eventLoop;

        public EventLoopListener(OrderedEventExecutor eventLoop, ClientCall.Listener<RespT> delegate) {
            super(delegate);
            this.eventLoop = eventLoop;
        }

        @Override
        public void onMessage(RespT message) {

            if (eventLoop == null || eventLoop.inEventLoop())
                delegate().onMessage(message);
            else
                eventLoop.execute(() -> delegate().onMessage(message));
        }

        @Override
        public void onHeaders(Metadata headers) {

            if (eventLoop == null || eventLoop.inEventLoop())
                delegate().onHeaders(headers);
            else
                eventLoop.execute(() -> delegate().onHeaders(headers));
        }

        @Override
        public void onClose(Status status, Metadata trailers) {

            if (eventLoop == null || eventLoop.inEventLoop())
                delegate().onClose(status, trailers);
            else
                eventLoop.execute(() -> delegate().onClose(status, trailers));
        }

        @Override
        public void onReady() {

            if (eventLoop == null || eventLoop.inEventLoop())
                delegate().onReady();
            else
                eventLoop.execute(() -> delegate().onReady());
        }
    }
}
