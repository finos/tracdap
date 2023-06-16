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
import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingServerInterceptor implements ServerInterceptor {

    private final Logger log;

    public LoggingServerInterceptor(Class<?> apiClass) {
        log = LoggerFactory.getLogger(apiClass);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        var method = call.getMethodDescriptor();

        if (method.getType() == MethodDescriptor.MethodType.UNARY) {

            log.info("API CALL START: {}() [{}]",
                    method.getBareMethodName(),
                    AuthHelpers.printCurrentUser());
        }
        else {

            log.info("API CALL START: {}() [{}] ({})",
                    method.getBareMethodName(),
                    AuthHelpers.printCurrentUser(),
                    method.getType());
        }

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

                log.info("API CALL SUCCEEDED: {}()", method.getBareMethodName());
            }
            else if (status.getCause() != null) {

                // Error mapping has already happened in ErrorMappingInterceptor

                log.error("API CALL FAILED: {}() {}",
                        method.getBareMethodName(),
                        status.getDescription(),
                        status.getCause());
            }
            else {

                // When a failed status has no cause, it is not possible to get a helpful stack trace
                // Calling status.asRuntimeException() will make a stack trace for this logging interceptor
                // Typically this happens when gRPC status exceptions are created by directly by application code
                // The solution is to always raise an ETrac* error in application error handling

                log.error("API CALL FAILED: {}() {}", method.getBareMethodName(), status.getDescription());
                log.error("(stack trace not available)");
            }

            delegate().close(status, trailers);
        }
    }
}