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
import org.finos.tracdap.common.auth.AuthConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class LoggingServerInterceptor implements ServerInterceptor {
    private final Logger log;

    public LoggingServerInterceptor(Class<?> apiClass) {
        log = LoggerFactory.getLogger(apiClass);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        var loggingCall = new LoggingServerCall<>(log, call);
        var listener = next.startCall(loggingCall, headers);
        return new LoggingListener<>(log, listener, call.getMethodDescriptor());
    }
}

class LoggingServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
    private final Logger log;
    private final ServerCall<ReqT, RespT> call;

    public LoggingServerCall(Logger log, ServerCall<ReqT, RespT> call) {
        this.log = log;
        this.call = call;
    }

    @Override
    public void request(int numMessages) {
        call.request(numMessages);
    }

    @Override
    public void sendHeaders(Metadata headers) {
        call.sendHeaders(headers);
    }

    @Override
    public void sendMessage(RespT message) {
        call.sendMessage(message);
    }

    @Override
    public boolean isReady() {
        return call.isReady();
    }

    @Override
    public void close(Status status, Metadata trailers) {
        call.close(status, trailers);

        var method = getMethodDescriptor();

        if (status.isOk()) {
            log.info("API CALL SUCCEEDED: [{}]", method.getBareMethodName());
        }
        else {
            var grpcError = status.asRuntimeException();
            // There is no GrpcErrorMapping.processError, because:
            // 1) grpcError is always StatusRuntimeException
            // 2) GrpcErrorMapping.processError passes through StatusRuntimeException

            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
        }
    }

    @Override
    public boolean isCancelled() {
        return call.isCancelled();
    }

    @Override
    @ExperimentalApi("https://github.com/grpc/grpc-java/issues/1704")
    public void setMessageCompression(boolean enabled) {
        call.setMessageCompression(enabled);
    }

    @Override
    @ExperimentalApi("https://github.com/grpc/grpc-java/issues/1704")
    public void setCompression(String compressor) {
        call.setCompression(compressor);
    }

    @Override
    @ExperimentalApi("https://github.com/grpc/grpc-java/issues/4692")
    public SecurityLevel getSecurityLevel() {
        return call.getSecurityLevel();
    }

    @Override
    @ExperimentalApi("https://github.com/grpc/grpc-java/issues/1779")
    public Attributes getAttributes() {
        return call.getAttributes();
    }

    @Override
    @ExperimentalApi("https://github.com/grpc/grpc-java/issues/2924")
    @Nullable
    public String getAuthority() {
        return call.getAuthority();
    }

    @Override
    public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
        return call.getMethodDescriptor();
    }
}

class LoggingListener<ReqT> extends ServerCall.Listener<ReqT> {
    private final Logger log;
    private final ServerCall.Listener<ReqT> listener;
    private final MethodDescriptor<ReqT, ?> method;

    public LoggingListener(Logger log, ServerCall.Listener<ReqT> listener, MethodDescriptor<ReqT, ?> method) {

        this.log = log;
        this.listener = listener;
        this.method = method;
    }

    @Override
    public void onMessage(ReqT message) {
        var userInfo = AuthConstants.USER_INFO_KEY.get();

        log.info("API CALL START: [{}] [{} <{}>] ({})",
                method.getBareMethodName(),
                userInfo.getDisplayName(),
                userInfo.getUserId(),
                method.getType());

        listener.onMessage(message);
    }

    @Override
    public void onHalfClose() {
        listener.onHalfClose();
    }

    @Override
    public void onCancel() {
        listener.onCancel();
    }

    @Override
    public void onComplete() {
        listener.onComplete();
    }

    @Override
    public void onReady() {
        listener.onReady();
    }
}