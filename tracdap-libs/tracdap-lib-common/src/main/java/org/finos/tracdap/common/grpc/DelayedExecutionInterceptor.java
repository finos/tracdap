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

        return new  DelayedExecutionListener<>(delayedCall, headers, next);
    }

    protected static class DelayedExecutionCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {

        private boolean firstRequest = true;

        public DelayedExecutionCall(ServerCall<ReqT, RespT> delegate) {

            super(delegate);

            // Since startCall() is delayed, request one message straight away to start the pipeline
            delegate.request(1);
        }

        @Override
        public void request(int numMessages) {

            // Ignore the first request in the pipeline (do not send a duplicate)
            if (firstRequest) {
                firstRequest = false;
                if (numMessages > 1)
                    delegate().request(numMessages - 1);
            }
            else {
                delegate().request(numMessages);
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

        protected void startCall() {

            // Do not start the call twice if this method is called explicitly

            if (delegate == null)
                delegate = next.startCall(call, headers);
        }

        @Override
        public void onReady() {

            // Do not trigger startCall() until the first real message is received

            if (delegate != null)
                delegate.onReady();
        }

        @Override
        public void onCancel() {

            // Do not trigger startCall() if the request is cancelled before it starts

            if (delegate != null)
                delegate.onCancel();
        }

        @Override
        public void onHalfClose() {

            // Do not trigger startCall() if the request is closed before it starts

            if (delegate != null)
                delegate.onHalfClose();
        }
    }
}
