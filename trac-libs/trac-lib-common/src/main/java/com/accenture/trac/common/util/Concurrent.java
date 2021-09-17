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
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
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
    Flow.Subscriber<T> grpcStreamSubscriber(StreamObserver<T> grpcObserver) {

        return new GrpcStreamSubscriber<>(grpcObserver);
    }

    static class GrpcStreamSubscriber<T> implements Flow.Subscriber<T> {

        private final StreamObserver<T> grpcObserver;

        private final AtomicBoolean subscribed = new AtomicBoolean(false);
        private Flow.Subscription subscription;

        public GrpcStreamSubscriber(StreamObserver<T> grpcObserver) {
            this.grpcObserver = grpcObserver;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

            var subscribedOk = this.subscribed.compareAndSet(false, true);

            if (!subscribedOk)
                throw new ETracInternal("Multiple subscriptions on gRPC observer wrapper");

            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(T item) {
            grpcObserver.onNext(item);
            subscription.request(1);
        }

        @Override
        public void onError(Throwable error) {
            subscription.cancel();
            grpcObserver.onError(error);
        }

        @Override
        public void onComplete() {
            grpcObserver.onCompleted();
        }
    }
}
