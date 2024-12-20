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
import org.finos.tracdap.common.grpc.GrpcErrorMapping;
import org.finos.tracdap.common.grpc.GrpcServiceRegister;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.*;


public class GrpcRequestValidator implements ServerInterceptor {

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

        return new ValidationListener<>(serverCall, metadata, nextHandler);
    }

    private class ValidationListener<ReqT, RespT> extends ForwardingServerCallListener<ReqT> {

        private final ServerCall<ReqT, RespT> serverCall;
        private final Metadata metadata;
        private final ServerCallHandler<ReqT, RespT> nextHandler;

        private final Descriptors.MethodDescriptor methodDescriptor;

        private ServerCall.Listener<ReqT> delegate = null;
        private boolean validated = false;

        public ValidationListener(
                ServerCall<ReqT, RespT> serverCall,
                Metadata metadata,
                ServerCallHandler<ReqT, RespT> nextHandler) {

            this.serverCall = serverCall;
            this.metadata = metadata;
            this.nextHandler = nextHandler;

            var grpcDescriptor = serverCall.getMethodDescriptor();
            var grpcMethodName = grpcDescriptor.getFullMethodName();

            this.methodDescriptor = serviceRegister.getMethodDescriptor(grpcMethodName);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected ServerCall.Listener<ReqT> delegate() {

            if (delegate != null)
                return delegate;

            return (ServerCall.Listener<ReqT>) NOOP_SINK;
        }

        @Override
        public void onReady() {

            if (delegate == null)
                serverCall.request(1);
            else
                delegate.onReady();
        }

        @Override
        public void onMessage(ReqT req) {

            if (!validated) {

                try {

                    var message = (Message) req;

                    validator.validateFixedMethod(message, methodDescriptor);
                    validated = true;

                    // Start the server call, messages will be passed on instead of going to the no-op sink
                    delegate = nextHandler.startCall(serverCall, metadata);
                }
                catch (EValidation validationError) {

                    var mappedError = GrpcErrorMapping.processError(validationError);
                    serverCall.close(mappedError.getStatus(), mappedError.getTrailers());
                }
            }

            // If validation has not succeeded, messages are sent to the no-op sink
            delegate().onMessage(req);
        }
    }

    private static final ServerCall.Listener<?> NOOP_SINK= new ServerCall.Listener<>() {};
}
