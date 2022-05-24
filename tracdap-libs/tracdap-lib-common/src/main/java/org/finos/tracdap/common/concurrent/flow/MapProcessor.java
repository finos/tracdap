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

import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.function.Function;

public class MapProcessor<T, U> implements Flow.Processor<T, U> {

    // Map processor is NOT thread safe
    // The map operation happens on the stack, so it relies on the thread safety of source and/or target

    // Map processor does not insist on the order in which source/target are connected
    // If the target connects first, subscription requests are buffered

    private final Function<T, U> mapping;

    private Flow.Subscriber<? super U> subscriber = null;
    private Flow.Subscription sourceSubscription = null;

    private int bufferRequest;
    private boolean bufferCancel;

    public MapProcessor(Function<T, U> mapping) {
        this.mapping = mapping;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super U> subscriber) {

        this.subscriber = subscriber;

        var targetSubscription = new MapSubscription();
        subscriber.onSubscribe(targetSubscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        this.sourceSubscription = subscription;

        if (bufferCancel) {
            subscription.cancel();
            bufferCancel = false;
        }

        else if (bufferRequest > 0) {
            subscription.request(bufferRequest);
            bufferRequest = 0;
        }
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
    public void onError(Throwable error) {

        var completionError = error instanceof CompletionException
                ? error : new CompletionException(error.getMessage(), error);

        subscriber.onError(completionError);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }

    private class MapSubscription implements Flow.Subscription {

        @Override
        public void request(long n) {

            if (sourceSubscription != null)
                sourceSubscription.request(n);
            else
                bufferRequest += n;
        }

        @Override
        public void cancel() {

            if (sourceSubscription != null)
                sourceSubscription.cancel();
            else
                bufferCancel = true;
        }
    }
}
