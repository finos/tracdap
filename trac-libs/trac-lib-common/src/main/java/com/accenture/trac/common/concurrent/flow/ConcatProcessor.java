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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;
import java.util.concurrent.Flow;

public class ConcatProcessor<T> implements Flow.Processor<T, T> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Vector<Flow.Publisher<T>> publishers;
    private Flow.Subscriber<? super T> subscriber;

    private Flow.Subscription sourceSubscription;
    private int sourceIndex;
    private int nPending;

    public ConcatProcessor(Flow.Publisher<T> first, Flow.Publisher<T> second) {
        this.publishers = new Vector<>(2);
        this.publishers.add(first);
        this.publishers.add(second);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {

        log.info("concat subscribe");

        this.subscriber = subscriber;

        var targetSubscription = new Subscription();
        subscriber.onSubscribe(targetSubscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        log.info("concat onSubscribe");

        this.sourceSubscription = subscription;

        if (nPending > 0)
            sourceSubscription.request(nPending);
    }

    @Override
    public void onNext(T item) {

        log.info("concat onNext");

        nPending -= 1;

        subscriber.onNext(item);
    }

    @Override
    public void onError(Throwable error) {

        log.info("concat onError");

        this.sourceSubscription = null;

        subscriber.onError(error);
    }

    @Override
    public void onComplete() {

        log.info("concat.onComplete");

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

            log.info("concat request");

            nPending += n;

            if (sourceSubscription != null)
                sourceSubscription.request(nPending);
        }

        @Override
        public void cancel() {

            log.info("concat cancel");

            if (sourceSubscription != null) {
                sourceSubscription.cancel();
                sourceSubscription = null;
            }
        }
    }
}
