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

package org.finos.tracdap.common.data;

import io.netty.util.concurrent.OrderedEventExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;


public interface IExecutionContext {

    OrderedEventExecutor eventLoopExecutor();

    default <TResult> CompletableFuture<TResult> toContext(CompletionStage<TResult> promise) {

        var ctxPromise = new CompletableFuture<TResult>();
        useContext(promise, ctxPromise);
        return ctxPromise;
    }

    default <TResult> CompletableFuture<TResult> fromContext(CompletableFuture<TResult> ctxPromise) {

        var promise = new CompletableFuture<TResult>();
        useContext(promise, ctxPromise);
        return promise;
    }

    private <TResult> void useContext(
            CompletionStage<TResult> promise,
            CompletableFuture<TResult> ctxPromise) {

        promise.whenComplete((result, error) -> {

            var wrappedError = (error != null && !(error instanceof CompletionException))
                    ? new CompletionException(error)
                    : error;

            if (eventLoopExecutor().inEventLoop()) {

                if (error != null)
                    ctxPromise.completeExceptionally(wrappedError);
                else
                    ctxPromise.complete(result);
            }
            else {

                if (error != null)
                    eventLoopExecutor().execute(() -> ctxPromise.completeExceptionally(wrappedError));
                else
                    eventLoopExecutor().execute(() -> ctxPromise.complete(result));
            }
        });
    }
}
