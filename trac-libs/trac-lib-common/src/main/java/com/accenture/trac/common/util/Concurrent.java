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

package com.accenture.trac.common.util;

import com.accenture.trac.common.exception.ETracInternal;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.stream.Stream;


public class Concurrent {

    public static <T>
    Flow.Publisher<T> javaStreamPublisher(Stream<T> source) {

        return new Flow.Publisher<>() {

            private final Iterator<T> sourceItr = source.iterator();
            private boolean completeSent = false;

            @Override
            public void subscribe(Flow.Subscriber<? super T> subscriber) {

                var subscription = new Flow.Subscription() {

                    @Override
                    public void request(long n) {

                        for (int i = 0; i < n && sourceItr.hasNext(); i++)
                            subscriber.onNext(sourceItr.next());

                        if (!sourceItr.hasNext() && !completeSent) {
                            subscriber.onComplete();
                            completeSent = true;
                        }
                    }

                    @Override
                    public void cancel() {
                        // pass
                    }
                };

                subscriber.onSubscribe(subscription);
            }
        };
    }

    public static <T, U>
    Flow.Publisher<U> map(Flow.Publisher<T> source, Function<T, U> mapping) {

        return new Flow.Processor<T, U>() {

            private Flow.Subscriber<? super U> subscriber = null;
            private Flow.Subscription sourceSubscription = null;

            @Override
            public void subscribe(Flow.Subscriber<? super U> subscriber) {

                source.subscribe(this);

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
        };
    }


    public static <T>
    Flow.Processor<T, T> hub() {

        return new HubProcessor<>();
    }

    public static <T>
    CompletionStage<T> first(Flow.Publisher<T> publisher) {

        var firstFuture = new CompletableFuture<T>();

        var subscriber = new FirstFutureSubscriber<>(firstFuture);
        publisher.subscribe(subscriber);

        return firstFuture;
    }

    public static class FirstFutureSubscriber<T> implements Flow.Subscriber<T> {

        private final CompletableFuture<T> firstFuture;

        public FirstFutureSubscriber(CompletableFuture<T> firstFuture) {
            this.firstFuture = firstFuture;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(1);
        }

        @Override
        public void onNext(T item) {
            firstFuture.complete(item);
        }

        @Override
        public void onError(Throwable error) {
            if (!firstFuture.isDone())
                firstFuture.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            if (!firstFuture.isDone())
                firstFuture.completeExceptionally(new ETracInternal("No data on stream"));  // TODO: Error
        }
    }

    static class HubProcessor<T> implements Flow.Processor<T, T> {

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {

        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

        }

        @Override
        public void onNext(T item) {

        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onComplete() {

        }
    }


}
