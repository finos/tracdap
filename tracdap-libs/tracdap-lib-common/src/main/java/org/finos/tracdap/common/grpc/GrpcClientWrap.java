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

/**
 * Deprecated class.
 */
public class GrpcClientWrap {

    public GrpcClientWrap(Class<?> serviceClass) {}

    public <TRequest, TResponse>
    TResponse unaryCall(
            MethodDescriptor<TRequest, TResponse> method, TRequest request,
            Function<TRequest, TResponse> methodImpl) {
        return methodImpl.apply(request);
    }

    public <TRequest, TResponse>
    CompletionStage<TResponse> unaryAsync(
            MethodDescriptor<TRequest, TResponse> method, TRequest request,
            Function<TRequest, ListenableFuture<TResponse>> methodImpl) {
        return Futures.javaFuture(methodImpl.apply(request));
    }
}
