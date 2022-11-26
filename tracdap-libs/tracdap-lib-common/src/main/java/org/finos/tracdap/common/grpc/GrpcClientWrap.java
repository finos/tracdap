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

import org.finos.tracdap.common.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;


public class GrpcClientWrap {

    private final Logger log;

    public GrpcClientWrap(Class<?> serviceClass) {
        this.log = LoggerFactory.getLogger(serviceClass);
    }

    public <TRequest, TResponse>
    CompletionStage<TResponse> unaryAsync(
            MethodDescriptor<TRequest, TResponse> method, TRequest request,
            Function<TRequest, ListenableFuture<TResponse>> methodImpl) {

        try {

            log.info("CLIENT CALL START: [{}]", methodDisplayName(method));

            return Futures
                    .javaFuture(methodImpl.apply(request))
                    .handle((result, error) -> handleResult(method, result, error));
        }
        catch (Exception error) {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("CLIENT CALL FAILED: [{}] {}",
                    methodDisplayName(method),
                    grpcError.getMessage(), grpcError);

            return CompletableFuture.failedFuture(grpcError);
        }
    }

    private <TResponse>
    TResponse handleResult(MethodDescriptor<?, ?> method, TResponse result, Throwable error) {

        if (error == null) {

            log.info("CLIENT CALL SUCCEEDED: [{}]", methodDisplayName(method));

            return result;
        }
        else {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("CLIENT CALL FAILED: [{}] {}",
                    methodDisplayName(method),
                    grpcError.getMessage(), grpcError);

            throw grpcError;
        }
    }

    private String methodDisplayName(MethodDescriptor<?, ?> method) {

        var serviceName = method.getServiceName();
        var shortServiceName = serviceName == null ? null : serviceName.substring(serviceName.lastIndexOf(".") + 1);
        var methodName = method.getBareMethodName();

        return String.format("%s.%s()", shortServiceName, methodName);
    }
}
