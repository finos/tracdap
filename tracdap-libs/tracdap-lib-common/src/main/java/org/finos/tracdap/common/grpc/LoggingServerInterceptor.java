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

public class LoggingServerInterceptor implements ServerInterceptor {

    private final Logger log;

    public LoggingServerInterceptor(Class<?> apiClass) {
        log = LoggerFactory.getLogger(apiClass);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        var method = call.getMethodDescriptor();
        var userInfo = AuthConstants.USER_INFO_KEY.get();

        log.info("API CALL START: [{}] [{} <{}>] ({})",
                method.getBareMethodName(),
                userInfo.getDisplayName(),
                userInfo.getUserId(),
                method.getType());

        var loggingCall = new LoggingServerCall<>(call);

        return next.startCall(loggingCall, headers);
    }

    private class LoggingServerCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {

        public LoggingServerCall(ServerCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public void close(Status status, Metadata trailers) {

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

            delegate().close(status, trailers);
        }
    }
}