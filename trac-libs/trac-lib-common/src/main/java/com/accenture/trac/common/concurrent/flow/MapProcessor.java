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

package com.accenture.trac.common.concurrent.flow;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class MapProcessor<T, U> implements Flow.Processor<T, U> {

    private final Function<T, U> mapping;

    private Flow.Subscriber<? super U> subscriber = null;
    private Flow.Subscription sourceSubscription = null;

    public MapProcessor(Function<T, U> mapping) {
        this.mapping = mapping;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super U> subscriber) {

        var targetSubscription = new Flow.Subscription() {

            @Override
            public void request(long n) {
                sourceSubscription.request(n);
            }

            @Override
            public void cancel() {
                sourceSubscription.cancel();
            }
        };

        this.subscriber = subscriber;
        subscriber.onSubscribe(targetSubscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.sourceSubscription = subscription;
    }

    @Override
    public void onNext(T item) {

        try {
            var mappedItem = mapping.apply(item);
            subscriber.onNext(mappedItem);
        }
        catch (Throwable e) {
            subscriber.onError(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {

        sourceSubscription.cancel();
        subscriber.onError(throwable);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }
}
