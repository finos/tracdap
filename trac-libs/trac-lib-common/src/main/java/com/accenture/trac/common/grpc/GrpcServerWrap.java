/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.grpc;

import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.function.Supplier;


public class GrpcServerWrap {

    private final Logger log;

    public GrpcServerWrap(Class<?> apiClass) {
        this.log = LoggerFactory.getLogger(apiClass);
    }

    public <TRequest, TResponse>
    void unaryCall(
            MethodDescriptor<TRequest, TResponse> method,
            TRequest request, StreamObserver<TResponse> response,
            Function<TRequest, CompletionStage<TResponse>> methodImpl) {

        try {

            log.info("API CALL START: [{}]", method.getBareMethodName());

            methodImpl.apply(request).handle((result, error) -> {

                if (result != null) {

                    log.info("API CALL SUCCEEDED: [{}]", method.getBareMethodName());

                    response.onNext(result);
                    response.onCompleted();
                }
                else {

                    var grpcError = GrpcErrorMapping.processError(error);

                    log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
                    response.onError(grpcError);
                }

                return null;
            });
        }
        catch (Exception error) {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
            response.onError(grpcError);
        }
    }

    public <TRequest, TResponse>
    void serverStreaming(
            MethodDescriptor<TRequest, TResponse> method,
            TRequest request, StreamObserver<TResponse> response,
            Function<TRequest, Flow.Publisher<TResponse>> methodImpl) {

        try {

            log.info("API CALL START: [{}]", method.getBareMethodName());

            var resultPublisher = methodImpl.apply(request);

            // TODO: Move logging to here

            var resultSubscriber = new GrpcServerResponseStream<>(method, response);
            resultPublisher.subscribe(resultSubscriber);

        }
        catch (Exception error) {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
            response.onError(grpcError);
        }
    }

}
