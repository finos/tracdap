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

package org.finos.tracdap.common.grpc;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoggingClientInterceptor implements ClientInterceptor {

    private final Logger log;

    public LoggingClientInterceptor(Class<?> serviceClass) {
        log = LoggerFactory.getLogger(serviceClass);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        var nextCall = next.newCall(method, callOptions);
        var methodName = methodDisplayName(method);

        return new LoggingClientCall<>(nextCall, methodName, method.getType());
    }

    private String methodDisplayName(MethodDescriptor<?, ?> method) {

        var serviceName = method.getServiceName();
        var shortServiceName = serviceName == null ? null : serviceName.substring(serviceName.lastIndexOf(".") + 1);
        var methodName = method.getBareMethodName();

        return String.format("%s.%s()", shortServiceName, methodName);
    }


    private class LoggingClientCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

        private final String methodName;
        private final MethodDescriptor.MethodType methodType;

        LoggingClientCall(ClientCall<ReqT, RespT> delegate, String methodName, MethodDescriptor.MethodType methodType) {
            super(delegate);
            this.methodName = methodName;
            this.methodType = methodType;
        }

        @Override
        public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {

            if (methodType == MethodDescriptor.MethodType.UNARY) {
                log.info("CLIENT CALL START: {}", methodName);
            }
            else {
                log.info("CLIENT CALL START: {} ({})", methodName, methodType);
            }

            var loggingResponseListener = new LoggingClientCallListener<>(responseListener, methodName);

            delegate().start(loggingResponseListener, headers);
        }
    }

    private class LoggingClientCallListener<RespT> extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT> {

        private final String methodName;

        LoggingClientCallListener(ClientCall.Listener<RespT> delegate, String methodName) {
            super(delegate);
            this.methodName = methodName;
        }

        @Override
        public void onClose(Status status, Metadata trailers) {

            if (status.isOk()) {
                log.info("CLIENT CALL SUCCEEDED: {}", methodName);
            }
            else {

                // There is no GrpcErrorMapping.processError, because:
                // 1) grpcError is always StatusRuntimeException
                // 2) GrpcErrorMapping.processError passes through StatusRuntimeException

                var grpcError = status.asRuntimeException();

                log.error(
                        "CLIENT CALL FAILED: {} {}",
                        methodName,
                        grpcError.getMessage(),
                        grpcError
                );
            }

            delegate().onClose(status, trailers);
        }

    }
}