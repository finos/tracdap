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

import io.grpc.stub.StreamObserver;

import java.util.function.Function;


public class GrpcServerWrap {

    public <TRequest, TResponse>
    void unaryCall(
            TRequest request, StreamObserver<TResponse> responseObserver,
            Function<TRequest, TResponse> methodImpl) {

        try {
            var result = methodImpl.apply(request);
            handleResult(responseObserver, result, null);
        }
        catch (Exception error) {
            handleResult(responseObserver, null, error);
        }
    }

    private <TResponse>
    void handleResult(
            StreamObserver<TResponse> responseObserver,
            TResponse result, Throwable error) {

        if (error == null) {
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
        else {
            responseObserver.onError(error);
        }
    }
}
