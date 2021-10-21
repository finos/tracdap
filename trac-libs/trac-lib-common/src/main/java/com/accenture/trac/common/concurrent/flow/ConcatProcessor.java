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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;


public class ConcatProcessor<T> implements Flow.Processor<T, T> {

    private final List<Flow.Publisher<T>> publishers;
    private Flow.Subscriber<? super T> subscriber;

    private Flow.Subscription sourceSubscription;
    private int sourceIndex;
    private int nPending;

    public ConcatProcessor(Flow.Publisher<T> first, Flow.Publisher<T> second) {
        this.publishers = new ArrayList<>(2);
        this.publishers.add(first);
        this.publishers.add(second);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {

        this.subscriber = subscriber;

        var targetSubscription = new Subscription();
        subscriber.onSubscribe(targetSubscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        this.sourceSubscription = subscription;

        if (nPending > 0)
            sourceSubscription.request(nPending);
    }

    @Override
    public void onNext(T item) {

        nPending -= 1;
        subscriber.onNext(item);
    }

    @Override
    public void onError(Throwable error) {

        this.sourceSubscription = null;

        var completionError = (error instanceof CompletionException)
                ? error : new CompletionException(error.getMessage(), error);

        subscriber.onError(completionError);
    }

    @Override
    public void onComplete() {

        this.sourceSubscription = null;
        this.sourceIndex += 1;

        if (this.sourceIndex < publishers.size())
            publishers.get(sourceIndex).subscribe(this);

        else
            subscriber.onComplete();

    }

    private class Subscription implements Flow.Subscription {

        @Override
        public void request(long n) {

            nPending += n;

            if (sourceSubscription != null)
                sourceSubscription.request(nPending);
        }

        @Override
        public void cancel() {

            if (sourceSubscription != null) {
                sourceSubscription.cancel();
                sourceSubscription = null;
            }
        }
    }
}
