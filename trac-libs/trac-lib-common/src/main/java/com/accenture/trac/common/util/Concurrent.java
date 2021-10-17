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

import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.exception.ETracInternal;
import io.netty.util.concurrent.OrderedEventExecutor;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;


public class Concurrent {

    public static <T>
    Flow.Publisher<T> publish(List<T> source) {

        return new SourcePublisher<>(source);
    }

    public static <T>
    Flow.Publisher<T> publish(Stream<T> source) {

        return new SourcePublisher<>(source);
    }

    public static <T>
    Flow.Publisher<T> publish(CompletionStage<T> source) {

        return new FuturePublisher<>(source);
    }

    public static <T, U>
    Flow.Publisher<U> map(Flow.Publisher<T> source, Function<T, U> mapping) {

        var map = new MapProcessor<>(mapping);
        source.subscribe(map);

        return map;
    }

    public static <T>
    CompletionStage<T> reduce(Flow.Publisher<T> source, BiFunction<T, T, T> func) {

        var result = new CompletableFuture<T>();
        var reduce = new ReduceProcessor<>(func, result, Function.identity());
        source.subscribe(reduce);

        return result;
    }

    public static<T, U>
    CompletionStage<U> fold(Flow.Publisher<T> source, BiFunction<U, T, U> func, U acc) {

        var result = new CompletableFuture<U>();
        var fold = new ReduceProcessor<>(func, result, acc);
        source.subscribe(fold);

        return result;
    }

    public static <T>
    Flow.Processor<T, T> hub(IExecutionContext execCtx) {

        return new HubProcessor<>(execCtx.eventLoopExecutor());
    }

    public static <T>
    CompletionStage<T> first(Flow.Publisher<T> publisher) {

        var firstFuture = new CompletableFuture<T>();

        var subscriber = new FirstFutureSubscriber<>(firstFuture);
        publisher.subscribe(subscriber);

        return firstFuture;
    }

    public static <T>
    Flow.Publisher<T> concat(CompletionStage<T> head, Flow.Publisher<T> tail) {

        var headStream = publish(head);
        return new ConcatProcessor<>(headStream, tail);
    }

    public static <T>
    CompletionStage<List<T>> toList(Flow.Publisher<T> source) {

        return fold(source, (xs, x) -> {xs.add(x); return xs;}, new ArrayList<>());
    }

    public static <T>
    Flow.Publisher<T> onEventLoop(Flow.Publisher<T> publisher, OrderedEventExecutor executor) {

        var relay = new EventLoopProcessor<T>(executor);
        publisher.subscribe(relay);

        return relay;
    }


    public static class SourcePublisher<T> implements Flow.Publisher<T> {

        private final Iterator<T> source;
        private final AutoCloseable closeable;
        private boolean done = false;

        public SourcePublisher(Iterable<T> source) {
            this.source = source.iterator();
            this.closeable = null;
        }

        public SourcePublisher(Stream<T> source) {
            this.source = source.iterator();
            this.closeable = source;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {

            var subscription = new Subscription(subscriber);
            subscriber.onSubscribe(subscription);
        }

        private class Subscription implements Flow.Subscription {

            Flow.Subscriber<? super T> subscriber;

            public Subscription(Flow.Subscriber<? super T> subscriber) {
                this.subscriber = subscriber;
            }

            @Override
            public void request(long n) {

                try {

                    for (int i = 0; i < n && source.hasNext(); i++)
                        subscriber.onNext(source.next());

                    if (!source.hasNext() && !done) {
                        subscriber.onComplete();
                        done = true;
                    }
                }
                catch (Exception e) {
                    subscriber.onError(e);
                    done = true;
                }
            }

            @Override
            public void cancel() {

                if (closeable != null) try {
                    closeable.close();
                }
                catch (Exception e) {
                    throw new ETracInternal(e.getMessage(), e);
                }
            }
        }
    }

    public static class FuturePublisher<T> implements Flow.Publisher<T> {

        private final CompletionStage<T> source;

        FuturePublisher(CompletionStage<T> source) {
            this.source = source;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {

            var subscription = new Subscription(subscriber);
            subscriber.onSubscribe(subscription);
        }

        private class Subscription implements Flow.Subscription {

            Flow.Subscriber<? super T> subscriber;

            public Subscription(Flow.Subscriber<? super T> subscriber) {
                this.subscriber = subscriber;
            }

            @Override
            public void request(long n) {

                // TODO
                throw new RuntimeException();
            }

            @Override
            public void cancel() {

                // TODO
            }
        }
    }

    public static class MapProcessor<T, U> implements Flow.Processor<T, U> {

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


    public static class ReduceProcessor<T, U> implements Flow.Subscriber<T> {

        private final BiFunction<U, T, U> func;
        private final CompletableFuture<U> result;

        private Flow.Subscription subscription;
        private Function<T, U> initFunc;
        private U acc;

        public ReduceProcessor(
                BiFunction<U, T, U> func,
                CompletableFuture<U> result,
                U acc) {

            this.func = func;
            this.result = result;

            this.initFunc = null;
            this.acc = acc;
        }

        public ReduceProcessor(
                BiFunction<U, T, U> func,
                CompletableFuture<U> result,
                Function<T, U> initFunc) {

            this.func = func;
            this.result = result;

            this.initFunc = initFunc;
            this.acc = null;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(T x) {

            if (initFunc != null) {
                acc = initFunc.apply(x);
                initFunc = null;
            }
            else
                acc = func.apply(acc, x);

            subscription.request(1);
        }

        @Override
        public void onError(Throwable error) {

            var unwrapped = unwrapConcurrentError(error);
            result.completeExceptionally(unwrapped);
        }

        @Override
        public void onComplete() {
            result.complete(acc);
        }
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
                firstFuture.completeExceptionally(error);  // TODO: Error
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
        private final OrderedEventExecutor eventLoop;

        private long messageBufferStart;
        private long messageBufferEnd;
        private long sourceRequestIndex;
        private boolean completeFlag;

        HubProcessor(OrderedEventExecutor eventLoop, Consumer<T> releaseFunc) {

            this.targets = new ConcurrentHashMap<>();

            this.messageBuffer = new LinkedList<>();
            this.releaseFunc = releaseFunc;
            this.eventLoop = eventLoop;

            messageBufferStart = 0;
            messageBufferEnd = 0;
            sourceRequestIndex = 0;
        }

        HubProcessor(OrderedEventExecutor eventLoop) {
            this(eventLoop, null);
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {

            var subscription = new HubSubscription(subscriber);
            var state = new HubTargetState();
            state.subscription = subscription;

            // Concurrent check - ensure each subscriber is only subscribed once
            // Targets is a concurrent map

            var priorState = targets.putIfAbsent(subscriber, state);

            if (priorState != null) {
                var err = new IllegalStateException("Duplicate subscription in hub processor (this is a bug)");
                subscriber.onError(err);
                return;
            }

            subscriber.onSubscribe(subscription);
        }

        private class HubSubscription implements Flow.Subscription {

            private final Flow.Subscriber<? super T> target;

            private HubSubscription(Flow.Subscriber<? super T> target) {
                this.target = target;
            }

            @Override
            public void request(long n) {
                eventLoop.submit(() -> requestTargetMessages(target, n));
            }

            @Override
            public void cancel() {
                eventLoop.submit(() -> cancelTargetSubscription(target));
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

            // todo: guard

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

            eventLoop.submit(() -> {

                messageBuffer.add(message);
                messageBufferEnd++;

                dispatchMessages();
            });
        }

        @Override
        public void onError(Throwable error) {

            eventLoop.submit(() -> {

                clearBuffer();

                // todo: wrap completion error

                for (var subscriber: targets.keySet())
                    subscriber.onError(error);
            });
        }

        @Override
        public void onComplete() {

            eventLoop.submit(() -> {

                completeFlag = true;
                dispatchMessages();
            });
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

            dispatchMessages();
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

        private static class HubTargetState {

            Flow.Subscription subscription;
            int requestIndex;
            int receiveIndex;
            boolean completeFlag;
        }
    }

    private static class ConcatProcessor<T> implements Flow.Processor<T, T> {

        private final Vector<Flow.Publisher<T>> publishers;

        public ConcatProcessor(Flow.Publisher<T> first, Flow.Publisher<T> second) {
            this.publishers = new Vector<>(2);
            this.publishers.add(first);
            this.publishers.add(second);
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {

            // TODO
            throw new RuntimeException();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

            // TODO
            throw new RuntimeException();
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

    private static class EventLoopProcessor<T> implements Flow.Processor<T, T> {

        private final OrderedEventExecutor executor;

        private Flow.Subscriber<? super T> target;
        private Flow.Subscription sourceSubscription;

        EventLoopProcessor(OrderedEventExecutor executor) {
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


    // Exceptions that are wrappers for errors that occurred during stream processing
    // These exceptions are unwrapped when a stream is resolved, i.e. their semantic value is discarded
    // Wrapper exceptions where the semantic value should be retained are not in this list
    // E.g. cancellation may be caused by an error, but the semantic value of cancellation is retained

    private static Throwable unwrapConcurrentError(Throwable error) {

        if (error.getCause() != null)
            for (var wrapping : WRAPPED_CONCURRENT_EXCEPTIONS)
                if (wrapping.isInstance(error))
                    return error.getCause();

        return error;
    }

    private static final List<Class<? extends Exception>> WRAPPED_CONCURRENT_EXCEPTIONS = List.of(
            ExecutionException.class,
            CompletionException.class,
            IllegalStateException.class);
}
