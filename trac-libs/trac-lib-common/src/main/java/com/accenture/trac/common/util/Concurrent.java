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
import com.accenture.trac.common.exception.EUnexpected;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
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
        private Flow.Subscription subscription;

        public FirstFutureSubscriber(CompletableFuture<T> firstFuture) {
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
            if (!firstFuture.isDone())
                firstFuture.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            if (!firstFuture.isDone())
                firstFuture.completeExceptionally(new ETracInternal("No data on stream"));  // TODO: Error
        }
    }

    public static class HubProcessor<T> implements Flow.Processor<T, T> {

        private Flow.Subscription sourceSubscription;
        private final Map<Flow.Subscriber<? super T>, HubTargetState> targets;

        private final LinkedList<T> messageBuffer;
        private final Consumer<T> releaseFunc;

        private long messageBufferStart;
        private long messageBufferEnd;
        private long sourceRequestIndex;
        private boolean completeFlag;

        HubProcessor(Consumer<T> releaseFunc) {

            this.targets = new HashMap<>();
            this.messageBuffer = new LinkedList<>();
            this.releaseFunc = releaseFunc;

            messageBufferStart = 0;
            messageBufferEnd = 0;
            sourceRequestIndex = 0;
        }

        HubProcessor() {
            this(null);
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {

            var subscription = new HubSubscription(subscriber);
            var state = new HubTargetState();
            state.subscription = subscription;

            targets.put(subscriber, state);

            subscriber.onSubscribe(subscription);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

            if (sourceSubscription != null)
                throw new ETracInternal("Hub processor subscribed to multiple upstream sources");

            if (targets.isEmpty())
                throw new ETracInternal("Hub processor connected to source before any targets");

            sourceSubscription = subscription;

            var maxRequest = targets.values().stream()
                    .map(state -> state.requestIndex)
                    .mapToLong(x -> x)
                    .max()
                    .getAsLong();

            sourceSubscription.request(maxRequest);
            sourceRequestIndex = maxRequest;
        }

        @Override
        public void onNext(T message) {

            messageBuffer.add(message);
            messageBufferEnd++;

            dispatchMessages();
        }

        @Override
        public void onError(Throwable error) {

            clearBuffer();

            for (var subscriber: targets.keySet())
                subscriber.onError(error);
        }

        @Override
        public void onComplete() {

            completeFlag = true;

            dispatchMessages();
        }

        private void dispatchMessages() {

            // If there are no subscribers, discard inbound messages
            // This should only happen after all targets have disconnected

            if (targets.isEmpty()) {
                clearBuffer();
                return;
            }

            for (int i = 0; i < (messageBufferEnd - messageBufferStart); i++) {

                var messageIndex = (long) i + messageBufferStart;
                var message = messageBuffer.get(i);

                for (var subscriberState: targets.entrySet()) {

                    var targetSubscriber = subscriberState.getKey();
                    var target = subscriberState.getValue();

                    if (messageIndex == target.receiveIndex && messageIndex < target.requestIndex) {
                        targetSubscriber.onNext(message);
                        target.receiveIndex++;
                    }
                }
            }

            if (completeFlag) {

                for (var subscriberState: targets.entrySet()) {

                    var targetSubscriber = subscriberState.getKey();
                    var target = subscriberState.getValue();

                    if (target.receiveIndex == messageBufferEnd && !target.completeFlag) {
                        targetSubscriber.onComplete();
                        target.completeFlag = true;
                    }
                }
            }

            if (targets.isEmpty()) {
                clearBuffer();
            }
            else {

                var minReceiveIndex = targets.values().stream()
                        .map(target -> target.receiveIndex)
                        .mapToLong(x -> x)
                        .min()
                        .getAsLong();

                while (minReceiveIndex > messageBufferStart) {
                    messageBuffer.pop();
                    messageBufferStart++;
                }
            }
        }

        private void requestTargetMessages(Flow.Subscriber<? super T> target, long n) {

            var state = targets.get(target);
            state.requestIndex += n;

            if (state.requestIndex > sourceRequestIndex && sourceSubscription != null) {

                sourceSubscription.request(state.requestIndex - sourceRequestIndex);
                sourceRequestIndex = state.requestIndex;
            }
        }

        private void cancelTargetSubscription(Flow.Subscriber<? super T> target) {

            targets.remove(target);

            if (targets.isEmpty()) {
                clearBuffer();
                sourceSubscription.cancel();
            }
        }

        private void clearBuffer() {

            if (releaseFunc != null)
                messageBuffer.forEach(releaseFunc);

            messageBuffer.clear();
            messageBufferStart = messageBufferEnd;
        }

        private class HubSubscription implements Flow.Subscription {

            private final Flow.Subscriber<? super T> target;

            private HubSubscription(Flow.Subscriber<? super T> target) {
                this.target = target;
            }

            @Override
            public void request(long n) {
                requestTargetMessages(target, n);
            }

            @Override
            public void cancel() {
                cancelTargetSubscription(target);
            }
        }

        private static class HubTargetState {

            Flow.Subscription subscription;
            int requestIndex;
            int receiveIndex;
            boolean completeFlag;
        }
    }

    public static <T>
    Flow.Publisher<T> publishOn(Flow.Publisher<T> publisher, ExecutorService executor) {

        var relay = new EventLoopProcessor<T>(executor);
        publisher.subscribe(relay);

        return relay;
    }

    private static class EventLoopProcessor<T> implements Flow.Processor<T, T> {

        private final ExecutorService executor;

        private Flow.Subscriber<? super T> target;
        private Flow.Subscription sourceSubscription;

        EventLoopProcessor(ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {
            this.target = subscriber;
            executor.execute(() -> target.onSubscribe(sourceSubscription));
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.sourceSubscription = subscription;
        }

        @Override
        public void onNext(T message) {
            executor.execute(() -> target.onNext(message));
        }

        @Override
        public void onError(Throwable error) {
            executor.execute(() -> target.onError(error));
        }

        @Override
        public void onComplete() {
            executor.execute(target::onComplete);
        }
    }
}
