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

import com.google.protobuf.Message;
import io.grpc.*;
import org.finos.tracdap.common.util.LoggingProtoTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoggingInterceptor implements ServerInterceptor {

    private final Logger log;
    private final LoggingProtoTranslator logTranslator;

    public LoggingInterceptor(Class<?> apiClass) {
        this(apiClass, LoggingProtoTranslator.createDefault().build());
    }

    public LoggingInterceptor(Class<?> apiClass, LoggingProtoTranslator logTranslator) {
        this.log = LoggerFactory.getLogger(apiClass);
        this.logTranslator = logTranslator;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        var method = call.getMethodDescriptor();

        if (method.getType() == MethodDescriptor.MethodType.UNARY) {

            log.info("API CALL START: {}() [{}]",
                    method.getBareMethodName(),
                    logCurrentUser());
        }
        else {

            log.info("API CALL START: {}() [{}] ({})",
                    method.getBareMethodName(),
                    logCurrentUser(),
                    method.getType());
        }

        var loggingCall = new LoggingServerCall<>(call);
        var nextCall = next.startCall(loggingCall, headers);

        return new LoggingServerCallListener<>(nextCall);
    }

    private class LoggingServerCallListener<ReqT> extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

        private boolean firstMessage;

        public LoggingServerCallListener(ServerCall.Listener<ReqT> delegate) {
            super(delegate);
            this.firstMessage = true;
        }

        @Override
        public void onMessage(ReqT req) {

            if (firstMessage) {

                var message = (Message) req;
                var logMessage = logTranslator.formatMessage(message);

                if (logMessage != null) {
                    log.info("API CALL REQUEST: {}", logMessage);
                }
            }

            firstMessage = false;

            delegate().onMessage(req);
        }
    }

    private class LoggingServerCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {

        private boolean firstMessage;

        public LoggingServerCall(ServerCall<ReqT, RespT> delegate) {
            super(delegate);
            this.firstMessage = true;
        }

        @Override
        public void sendMessage(RespT resp) {

            if (firstMessage) {

                var message = (Message) resp;
                var logMessage = logTranslator.formatMessage(message);

                if (logMessage != null) {
                    log.info("API CALL RESPONSE: {}", logMessage);
                }
            }

            firstMessage = false;

            delegate().sendMessage(resp);
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

    private String logCurrentUser() {

        var userMetadata = UserMetadata.get(Context.current());

        if (userMetadata.hasDelegate())
            return String.format("%s <%s> on behalf of %s <%s>",
                    userMetadata.userName(), userMetadata.userId(),
                    userMetadata.delegateName(), userMetadata.delegateId());

        else
            return String.format("%s <%s>",
                    userMetadata.userName(), userMetadata.userId());
    }
}