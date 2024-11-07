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

package org.finos.tracdap.common.async.flow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;

public class FutureFirstItemSubscriber<T> implements Flow.Subscriber<T> {

    private final CompletableFuture<T> firstFuture;
    private Flow.Subscription subscription;

    public FutureFirstItemSubscriber(CompletableFuture<T> firstFuture) {
        this.firstFuture = firstFuture;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T item) {
        firstFuture.complete(item);
        subscription.cancel();
    }

    @Override
    public void onError(Throwable error) {

        if (!firstFuture.isDone()) {

            var completionError = (error instanceof CompletionException)
                    ? error : new CompletionException(error.getMessage(), error);

            firstFuture.completeExceptionally(completionError);
        }
    }

    @Override
    public void onComplete() {

        if (!firstFuture.isDone())
            firstFuture.completeExceptionally(new IllegalStateException());
    }
}
