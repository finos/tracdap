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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;


public class DelayedSubscriber<T> implements Flow.Subscriber<T> {

    private final Flow.Subscriber<T> subscriber;
    private final CompletionStage<?> signal;
    private final AtomicBoolean subscribed;

    public DelayedSubscriber(Flow.Subscriber<T> subscriber, CompletionStage<?> signal) {
        this.subscriber = subscriber;
        this.signal = signal;
        this.subscribed = new AtomicBoolean(false);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        var firstSubscription = subscribed.compareAndSet(false, true);

        if (!firstSubscription)
            throw new IllegalStateException();

        signal.whenComplete((result, error) -> {

            if (error == null)
                subscriber.onSubscribe(subscription);

            else {
                subscription.cancel();
                subscriber.onError(error);
            }
        });
    }

    @Override
    public void onNext(T item) {
        subscriber.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
        subscriber.onError(throwable);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }
}
