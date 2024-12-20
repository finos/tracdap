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

package org.finos.tracdap.common.grpc;

import io.grpc.*;


public class DelayedExecutionInterceptor implements ServerInterceptor {

    // Delay interceptors to run on the same stack as the first inbound message
    // For unary calls, this puts the interceptors on the same stack as the main handler

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        var delayedCall = new DelayedExecutionCall<>(call);

        return new DelayedExecutionListener<>(delayedCall, headers, next);
    }

    protected static class DelayedExecutionCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {

        private long totalRequested;

        public DelayedExecutionCall(ServerCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public void request(int numMessages) {

            // In the listener, onReady() sends out an extra request() call to pull the first message
            // When the first request() comes from the main handler, it is a duplicate
            // This method ignores the second request in the stream, to discard that duplicate

            var priorRequested = totalRequested;

            totalRequested += numMessages;

            if (priorRequested >= 2) {
                delegate().request(numMessages);
            }
            else if (priorRequested == 1) {
                if (numMessages > 1)
                    delegate().request(numMessages - 1);
            }
            else {
                if (numMessages == 1) {
                    delegate().request(numMessages);
                }
                else {
                    delegate().request(numMessages - 1);
                }
            }
        }
    }

    protected static class DelayedExecutionListener<ReqT, RespT> extends ForwardingServerCallListener<ReqT> {

        private final ServerCall<ReqT, RespT> call;
        private final Metadata headers;
        private final ServerCallHandler<ReqT, RespT> next;

        private ServerCall.Listener<ReqT> delegate;

        public DelayedExecutionListener(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {

            this.call = call;
            this.headers = headers;
            this.next = next;
        }

        @Override
        protected ServerCall.Listener<ReqT> delegate() {

            if (delegate == null)
                startCall();

            return delegate;
        }

        private void startCall() {

            delegate = next.startCall(call, headers);
        }

        @Override
        public void onReady() {

            // Do not trigger startCall() until the first real message is received

            if (delegate == null)
                call.request(1);
            else
                delegate.onReady();
        }
    }
}
