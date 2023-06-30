/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.gcp.storage;

import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ClientStreamingCallable;
import com.google.api.gax.rpc.UnaryCallable;
import io.grpc.CallOptions;

import java.util.concurrent.*;

public class GcpUtils {

    public static <TRequest, TResponse>
    CompletionStage<TResponse> unaryCall(
            UnaryCallable<TRequest, TResponse> callable,
            TRequest request,
            Executor executor) {

        var callOptions = CallOptions.DEFAULT
                .withExecutor(executor);

        var callCtx = GrpcCallContext.createDefault()
                .withCallOptions(callOptions);

        var apiCall = callable.futureCall(request, callCtx);

        return GcpUtils.gcpCallback(apiCall);
    }

    public static <TRequest, TResponse>
    CompletionStage<TResponse> clientStreamingCall(
            ClientStreamingCallable<TRequest, TResponse> callable,
            TRequest request,
            Executor executor) {

        var callOptions = CallOptions.DEFAULT
                .withExecutor(executor);

        var callCtx = GrpcCallContext.createDefault()
                .withCallOptions(callOptions);

        var requestStream = callable.clientStreamingCall(null, callCtx);
        requestStream.onNext(request);
        requestStream.onCompleted();

        var apiCall = callable.futureCall(request, callCtx);

        return GcpUtils.gcpCallback(apiCall);
    }

    public static <T>
    CompletionStage<T> gcpCallback(ApiFuture<T> gcpFuture) {

        return gcpCallback(gcpFuture, Runnable::run);
    }

    public static <T>
    CompletionStage<T> gcpCallback(ApiFuture<T> gcpFuture, Executor executor) {

        var javaFuture = new CompletableFuture<T>();

        gcpFuture.addListener(() -> gcpCallback(gcpFuture, javaFuture), executor);

        return javaFuture;
    }

    private static <T>
    void gcpCallback(ApiFuture<T> gcpFuture, CompletableFuture<T> javaFuture) {

        try {

            if (!gcpFuture.isDone())
                javaFuture.completeExceptionally(new IllegalStateException());

            else if (gcpFuture.isCancelled())
                javaFuture.cancel(true);

            else {
                var result = gcpFuture.get();
                javaFuture.complete(result);
            }
        }
        catch (InterruptedException | CancellationException e) {
            javaFuture.completeExceptionally(new IllegalStateException());
        }
        catch (ExecutionException e) {

            if (e.getCause() != null)
                javaFuture.completeExceptionally(e.getCause());
            else
                javaFuture.completeExceptionally(e);
        }
        catch (Throwable e) {
            javaFuture.completeExceptionally(e);
        }
    }
}
