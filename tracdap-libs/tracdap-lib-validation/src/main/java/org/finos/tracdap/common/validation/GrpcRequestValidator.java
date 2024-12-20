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


public class GrpcRequestValidator extends DelayedExecutionInterceptor {

    // Validation requires delayed execution, because it has to wait for the first onMessage() call
    // Use delayed interceptor as a base, which has the logic to manage the request sequence

    private final GrpcServiceRegister serviceRegister;
    private final Validator validator;

    public GrpcRequestValidator(GrpcServiceRegister serviceRegister) {
        this.serviceRegister = serviceRegister;
        this.validator = new Validator();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT>
    interceptCall(
            ServerCall<ReqT, RespT> serverCall, Metadata metadata,
            ServerCallHandler<ReqT, RespT> nextHandler) {

        var validationCall = new DelayedExecutionCall<>(serverCall);

        return new ValidationListener<>(validationCall, metadata, nextHandler);
    }

    private class ValidationListener<ReqT, RespT> extends DelayedExecutionListener<ReqT, RespT> {

        private final ServerCall<ReqT, RespT> serverCall;
        private final Descriptors.MethodDescriptor methodDescriptor;

        private boolean validated = false;

        public ValidationListener(
                ServerCall<ReqT, RespT> serverCall,
                Metadata metadata,
                ServerCallHandler<ReqT, RespT> nextHandler) {

            // Using setReady(false) will prevent delayed interceptor from calling startCall()

            super(serverCall, metadata, nextHandler);
            super.setReady(false);

            var grpcDescriptor = serverCall.getMethodDescriptor();
            var grpcMethodName = grpcDescriptor.getFullMethodName();

            this.serverCall = serverCall;
            this.methodDescriptor = serviceRegister.getMethodDescriptor(grpcMethodName);
        }

        @Override
        public void onMessage(ReqT req) {

            if (!validated) {

                try {

                    var message = (Message) req;

                    validator.validateFixedMethod(message, methodDescriptor);
                    validated = true;

                    // Allow delayed interceptor to call startCAll() and start the normal flow of events
                    setReady(true);
                }
                catch (EValidation validationError) {

                    var mappedError = GrpcErrorMapping.processError(validationError);
                    serverCall.close(mappedError.getStatus(), mappedError.getTrailers());
                }
            }

            // If validation has not succeeded, messages are sent to a no-op sink
            delegate().onMessage(req);
        }
    }
}
