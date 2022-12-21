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

import javax.annotation.Nullable;


public class LoggingClientInterceptor implements ClientInterceptor {
    private final Logger log;

    public LoggingClientInterceptor(Class<?> serviceClass) {
        log = LoggerFactory.getLogger(serviceClass);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next
    ) {

        var nextCall = next.newCall(method, callOptions);
        var methodName = methodDisplayName(method);

        return new LoggingClientCall<>(log, methodName, nextCall);
    }

    private String methodDisplayName(MethodDescriptor<?, ?> method) {
        var serviceName = method.getServiceName();
        var shortServiceName = serviceName == null ? null : serviceName.substring(serviceName.lastIndexOf(".") + 1);
        var methodName = method.getBareMethodName();

        return String.format("%s.%s()", shortServiceName, methodName);
    }
}


class LoggingClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
    private final Logger log;
    private final String methodName;
    private final ClientCall<ReqT, RespT> nextCall;

    LoggingClientCall(Logger log, String methodName, ClientCall<ReqT, RespT> nextCall) {
        this.log = log;
        this.methodName = methodName;
        this.nextCall = nextCall;
    }

    @Override
    public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
        log.info("CLIENT CALL START: [{}]", methodName);

        var loggingResponseListener = new LoggingClientCallListener<>(log, methodName, responseListener);

        nextCall.start(loggingResponseListener, headers);
    }

    @Override
    public void request(int numMessages) {
        nextCall.request(numMessages);
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
        if (cause != null) {
            var grpcError = GrpcErrorMapping.processError(cause);
            log.error(
                    "CLIENT CALL FAILED: [{}] {}",
                    methodName,
                    grpcError.getMessage(),
                    grpcError
            );
        }

        nextCall.cancel(message, cause);
    }

    @Override
    public void halfClose() {
        nextCall.halfClose();
    }

    @Override
    public void sendMessage(ReqT message) {
        nextCall.sendMessage(message);
    }
}

class LoggingClientCallListener<RespT> extends ClientCall.Listener<RespT> {
    private final Logger log;
    private final String methodName;
    final private ClientCall.Listener<RespT> listener;

    LoggingClientCallListener(Logger log, String methodName, ClientCall.Listener<RespT> listener) {
        this.log = log;
        this.methodName = methodName;
        this.listener = listener;
    }

    @Override
    public void onHeaders(Metadata headers) {
        listener.onHeaders(headers);
    }

    @Override
    public void onMessage(RespT message) {
        listener.onMessage(message);
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
        if (status.isOk()) {
            log.info("CLIENT CALL SUCCEEDED: [{}]", methodName);
        }
        else {
            var grpcError = status.asRuntimeException();
            log.error(
                    "CLIENT CALL FAILED: [{}] {}",
                    methodName,
                    grpcError.getMessage(),
                    grpcError
            );
        }

        listener.onClose(status, trailers);
    }

    @Override
    public void onReady() {
        listener.onReady();
    }
}