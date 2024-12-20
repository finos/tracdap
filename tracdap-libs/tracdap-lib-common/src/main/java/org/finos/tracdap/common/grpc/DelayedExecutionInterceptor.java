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

        return new DelayedExecutionListener<>(call, headers, next);
    }

    private static class DelayedExecutionListener<ReqT, RespT> extends ForwardingServerCallListener<ReqT> {

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
