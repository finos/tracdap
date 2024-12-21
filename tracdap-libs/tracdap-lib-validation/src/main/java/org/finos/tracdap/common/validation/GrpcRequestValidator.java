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

package org.finos.tracdap.common.validation;

import org.finos.tracdap.common.exception.EValidation;
import org.finos.tracdap.common.grpc.DelayedExecutionInterceptor;
import org.finos.tracdap.common.grpc.GrpcErrorMapping;
import org.finos.tracdap.common.grpc.GrpcServiceRegister;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcRequestValidator extends DelayedExecutionInterceptor {

    // Validation requires delayed execution, because it has to wait for the first onMessage() call
    // Use delayed interceptor as a base, which has the logic to manage the request sequence

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final GrpcServiceRegister serviceRegister;
    private final Validator validator;

    private final boolean loggingEnabled;

    public GrpcRequestValidator(GrpcServiceRegister serviceRegister) {
        this(serviceRegister, true);
    }

    public GrpcRequestValidator(GrpcServiceRegister serviceRegister, boolean loggingEnabled) {
        this.serviceRegister = serviceRegister;
        this.validator = new Validator();
        this.loggingEnabled = loggingEnabled;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT>
    interceptCall(
            ServerCall<ReqT, RespT> serverCall, Metadata metadata,
            ServerCallHandler<ReqT, RespT> nextHandler) {

        var validationCall = new DelayedExecutionCall<>(serverCall);

        return new ValidationListener<>(validationCall, metadata, nextHandler, loggingEnabled);
    }

    private class ValidationListener<ReqT, RespT> extends DelayedExecutionListener<ReqT, RespT> {

        private final ServerCall<ReqT, RespT> serverCall;
        private final boolean loggingEnabled;

        private final Descriptors.MethodDescriptor methodDescriptor;
        private boolean validated = false;

        public ValidationListener(
                ServerCall<ReqT, RespT> serverCall,
                Metadata metadata,
                ServerCallHandler<ReqT, RespT> nextHandler,
                boolean loggingEnabled) {

            super(serverCall, metadata, nextHandler);

            this.serverCall = serverCall;
            this.loggingEnabled = loggingEnabled;

            // Look up the descriptor for this call, to use for validation
            var grpcDescriptor = serverCall.getMethodDescriptor();
            var grpcMethodName = grpcDescriptor.getFullMethodName();

            this.methodDescriptor = serviceRegister.getMethodDescriptor(grpcMethodName);
        }

        @Override
        public void onMessage(ReqT req) {

            if (!validated) {

                try {

                    var message = (Message) req;

                    validator.validateFixedMethod(message, methodDescriptor);
                    validated = true;

                    // Allow delayed interceptor to start the normal flow of events
                    startCall();
                    delegate().onMessage(req);
                }
                catch (EValidation validationError) {

                    var mappedError = GrpcErrorMapping.processError(validationError);
                    var status  = mappedError.getStatus();

                    // Allow logging to be turned on / off to avoid duplicate logs
                    // E.g. if this interceptor is behind the logging interceptor
                    if (loggingEnabled) {

                        log.error("VALIDATION FAILED: {}() {}",
                                methodDescriptor.getName(),
                                status.getDescription(),
                                status.getCause());
                    }

                    serverCall.close(status, mappedError.getTrailers());
                }
            }
            else {

                delegate().onMessage(req);
            }
        }
    }
}
