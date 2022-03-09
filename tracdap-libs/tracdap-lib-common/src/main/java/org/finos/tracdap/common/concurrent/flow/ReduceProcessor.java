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

package org.finos.tracdap.common.concurrent.flow;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ReduceProcessor<T, U> implements Flow.Subscriber<T> {

    private final BiFunction<U, T, U> func;
    private final CompletableFuture<U> result;

    private Flow.Subscription subscription;
    private Function<T, U> initFunc;
    private U acc;

    public ReduceProcessor(
            BiFunction<U, T, U> func,
            CompletableFuture<U> result,
            U acc) {

        this.func = func;
        this.result = result;

        this.initFunc = null;
        this.acc = acc;
    }

    public ReduceProcessor(
            BiFunction<U, T, U> func,
            CompletableFuture<U> result,
            Function<T, U> initFunc) {

        this.func = func;
        this.result = result;

        this.initFunc = initFunc;
        this.acc = null;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T x) {

        if (initFunc != null) {
            acc = initFunc.apply(x);
            initFunc = null;
        }
        else
            acc = func.apply(acc, x);

        subscription.request(1);
    }

    @Override
    public void onError(Throwable error) {

        var completionError = (error instanceof CompletionException)
                ? error : new CompletionException(error.getMessage(), error);

        result.completeExceptionally(completionError);
    }

    @Override
    public void onComplete() {
        result.complete(acc);
    }
}
