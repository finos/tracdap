/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.netty;

import io.grpc.*;
import io.netty.util.concurrent.OrderedEventExecutor;


public class EventLoopInterceptor implements ClientInterceptor {

    private final EventLoopRegister register;

    public EventLoopInterceptor(EventLoopRegister register) {
        this.register = register;
    }

    @Override public <ReqT, RespT>
    ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions options,
            Channel channel) {

        // Enforce strict requirement on the event loop
        // Client interceptor is used when the data service makes calls to other TRAC services
        // Those calls must come back on the same EL, so processing is not split across ELs

        var eventLoop =register.currentEventLoop(/* strict = */ true);
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

            if (eventLoop.inEventLoop())
                delegate().onMessage(message);
            else
                eventLoop.execute(() -> delegate().onMessage(message));
        }

        @Override
        public void onHeaders(Metadata headers) {

            if (eventLoop.inEventLoop())
                delegate().onHeaders(headers);
            else
                eventLoop.execute(() -> delegate().onHeaders(headers));
        }

        @Override
        public void onClose(Status status, Metadata trailers) {

            if (eventLoop.inEventLoop())
                delegate().onClose(status, trailers);
            else
                eventLoop.execute(() -> delegate().onClose(status, trailers));
        }

        @Override
        public void onReady() {

            if (eventLoop.inEventLoop())
                delegate().onReady();
            else
                eventLoop.execute(() -> delegate().onReady());
        }
    }
}
