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

import java.util.concurrent.Flow;
import java.util.function.Function;


public class MapProcessor<T, U> implements Flow.Processor<T, U> {

    // Map processor is NOT thread safe
    // The map operation happens on the stack, so it relies on the thread safety of source and/or target

    // Map processor does not insist on the order in which source/target are connected
    // If the target connects first, subscription requests are buffered

    private final Function<T, U> mapping;
    private final Flow.Publisher<T> initSource;
    private final Flow.Subscriber<? super U> initTarget;

    private Flow.Subscription sourceSubscription = null;
    private Flow.Subscriber<? super U> targetSubscriber = null;

    private int bufferRequest;
    private boolean bufferCancel;
    private Throwable bufferError;

    public MapProcessor(Function<T, U> mapping, Flow.Publisher<T> source) {
        this.mapping = mapping;
        this.initSource = source;
        this.initTarget = null;
    }

    public MapProcessor(Function<T, U> mapping, Flow.Subscriber<? super U> target) {
        this.mapping = mapping;
        this.initSource = null;
        this.initTarget = target;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super U> subscriber) {

        // This method connects the target, the source may or may not be connected already

        // Only allow a single target
        if (this.targetSubscriber != null) {
            subscriber.onError(new IllegalStateException("Duplicate subscription"));
            return;
        }

        // Store the target and send onSubscribe()
        this.targetSubscriber = subscriber;
        var targetSubscription = new MapSubscription();
        targetSubscriber.onSubscribe(targetSubscription);

        // If a source was supplied via the constructor, connect it now
        // This will mean the event ordering for the source is not changed by MapProcessor

        if (initSource != null)
            initSource.subscribe(this);

        // If a source was connected previously it may have sent an error signal
        // In this case, relay the error to the target

        if (bufferError != null)
            targetSubscriber.onError(bufferError);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        // This method signals connection from the source, the target may or may not be connected already

        // Only allow a single source connection
        if (this.sourceSubscription != null)
            throw new IllegalStateException("Duplicate subscription");

        this.sourceSubscription = subscription;

        // If a target was supplied via the constructor, connect it now
        // This will mean the event ordering for the target is not changed by MapProcessor

        if (initTarget != null)
            this.subscribe(initTarget);

        // If a target was already connected, it may have sent request or cancel messages
        // If either of these exist, relay them to the source subscription

        if (bufferCancel) {
            sourceSubscription.cancel();
            bufferCancel = false;
        }

        else if (bufferRequest > 0) {
            sourceSubscription.request(bufferRequest);
            bufferRequest = 0;
        }
    }

    @Override
    public void onNext(T item) {

        // onNext only happens after source and target are already connected
        // There must be a call to onSubscribe() and a request()

        try {
            var mappedItem = mapping.apply(item);
            targetSubscriber.onNext(mappedItem);
        }
        catch (Throwable e) {
            targetSubscriber.onError(e);
            sourceSubscription.cancel();
        }
    }

    @Override
    public void onError(Throwable error) {

        // On error occurs after the source is connected, but there might not be a target yet

        if (targetSubscriber != null)
            targetSubscriber.onError(error);
        else
            bufferError = error;
    }

    @Override
    public void onComplete() {

        // onComplete only happens after source and target are already connected
        // There must be a call to onSubscribe() and a request()

        targetSubscriber.onComplete();
    }

    private class MapSubscription implements Flow.Subscription {

        // Both request and cancel happen after the target is connected, but there might not be a source yet

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
