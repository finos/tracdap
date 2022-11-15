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

import org.finos.tracdap.common.auth.AuthConstants;
import org.finos.tracdap.common.concurrent.Flows;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;


public class GrpcServerWrap {

    private final Logger log;

    public GrpcServerWrap(Class<?> apiClass) {
        this.log = LoggerFactory.getLogger(apiClass);
    }

    public <TRequest, TResponse>
    void unaryCall(
            MethodDescriptor<TRequest, TResponse> method,
            TRequest request, StreamObserver<TResponse> responseObserver,
            Function<TRequest, CompletionStage<TResponse>> methodImpl) {

        try {

            var userInfo = AuthConstants.USER_INFO_KEY.get();

            log.info("API CALL START: [{}] [{} <{}>]",
                    method.getBareMethodName(),
                    userInfo.getDisplayName(),
                    userInfo.getUserId());

            methodImpl.apply(request).handle((result, error) ->
                    handleResult(method, responseObserver, result, error));
        }
        catch (Exception error) {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
            responseObserver.onError(grpcError);
        }
    }

    public <TRequest, TResponse>
    void serverStreaming(
            MethodDescriptor<TRequest, TResponse> method,
            TRequest request, StreamObserver<TResponse> responseObserver,
            Function<TRequest, Flow.Publisher<TResponse>> methodImpl) {

        try {

            var userInfo = AuthConstants.USER_INFO_KEY.get();

            log.info("API CALL START: [{}] [{} <{}>] (server streaming)",
                    method.getBareMethodName(),
                    userInfo.getDisplayName(),
                    userInfo.getUserId());

            var resultPublisher = methodImpl.apply(request);

            var resultLogger = Flows.interceptResult(resultPublisher,
                    (result, error) -> logResult(method, error));

            var resultSubscriber = new GrpcServerResponseStream<>(responseObserver);
            resultLogger.subscribe(resultSubscriber);
        }
        catch (Exception error) {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
            responseObserver.onError(grpcError);
        }
    }

    public <TRequest, TResponse>
    StreamObserver<TRequest> clientStreaming(
            MethodDescriptor<TRequest, TResponse> method,
            StreamObserver<TResponse> responseObserver,
            Function<Flow.Publisher<TRequest>, CompletionStage<TResponse>> methodImpl) {

        try {

            var userInfo = AuthConstants.USER_INFO_KEY.get();

            log.info("API CALL START: [{}] [{} <{}>] (client streaming)",
                    method.getBareMethodName(),
                    userInfo.getDisplayName(),
                    userInfo.getUserId());

            var requestStream = Flows.<TRequest>passThrough();

            methodImpl.apply(requestStream).handle((result, error) ->
                    handleResult(method, responseObserver, result, error));

            return new GrpcServerRequestStream<>(requestStream);
        }
        catch (Exception error) {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
            responseObserver.onError(grpcError);

            return null;
        }
    }

    private <TResponse>
    Void handleResult(
            MethodDescriptor<?, TResponse> method,
            StreamObserver<TResponse> responseObserver,
            TResponse result, Throwable error) {

        if (error == null) {

            log.info("API CALL SUCCEEDED: [{}]", method.getBareMethodName());

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
        else {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
            responseObserver.onError(grpcError);
        }

        return null;
    }

    private <TResponse>
    void logResult(MethodDescriptor<?, TResponse> method, Throwable error) {

        if (error == null) {

            log.info("API CALL SUCCEEDED: [{}]", method.getBareMethodName());
        }
        else {

            var grpcError = GrpcErrorMapping.processError(error);
            log.error("API CALL FAILED: [{}] {}", method.getBareMethodName(), grpcError.getMessage(), grpcError);
        }
    }

}
