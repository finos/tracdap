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

import com.accenture.trac.common.exception.ETracInternal;
import io.netty.util.concurrent.OrderedEventExecutor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class HubProcessor<T> implements Flow.Processor<T, T> {

    // Hub processor is a thread-safe processor
    // This is achieved by deferring all operations to happen on the event loop

    // Subscribe and onSubscribe implement guard checks, which happen synchronously
    // Otherwise there is nowhere to report errors in onSubscribe
    // Once the checks are passed, these actions are also deferred to the event loop

    // Hub processor insists that at least one target is subscribed before connecting the source
    // Otherwise source messages would be discarded (including error/complete messages)

    private Flow.Subscription sourceSubscription;
    private final Map<Flow.Subscriber<? super T>, HubTargetState> targets;

    private final AtomicBoolean sourceGuard;
    private final ConcurrentMap<Flow.Subscriber<?>, Object> targetGuard;

    private final LinkedList<T> messageBuffer;
    private final Consumer<T> releaseFunc;
    private final OrderedEventExecutor eventLoop;

    private long messageBufferStart;
    private long messageBufferEnd;
    private long sourceRequestIndex;
    private boolean completeFlag;

    public HubProcessor(OrderedEventExecutor eventLoop, Consumer<T> releaseFunc) {

        this.targets = new HashMap<>();

        this.sourceGuard = new AtomicBoolean(false);
        this.targetGuard = new ConcurrentHashMap<>();

        this.messageBuffer = new LinkedList<>();
        this.releaseFunc = releaseFunc;
        this.eventLoop = eventLoop;

        messageBufferStart = 0;
        messageBufferEnd = 0;
        sourceRequestIndex = 0;
    }

    public HubProcessor(OrderedEventExecutor eventLoop) {
        this(eventLoop, null);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {

        var priorTarget = targetGuard.putIfAbsent(subscriber, new Object());

        if (priorTarget != null) {

            var err = new IllegalStateException("Duplicate subscription in hub processor (this is a bug)");
            eventLoop.execute(() -> subscriber.onError(err));

            return;
        }

        eventLoop.execute(() -> doNewSubscription(subscriber));
    }

    private void doNewSubscription(Flow.Subscriber<? super T> subscriber) {

        var subscription = new HubSubscription(subscriber);
        var state = new HubTargetState();
        state.subscription = subscription;

        targets.put(subscriber, state);

        subscriber.onSubscribe(subscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        var priorSourceOk = sourceGuard.compareAndSet(false, true);

        if (!priorSourceOk)
            throw new ETracInternal("Hub processor subscribed to multiple upstream sources");

        if (targetGuard.isEmpty())
            throw new ETracInternal("Hub processor connected to source before any targets");

        eventLoop.execute(() -> doSubscribe(subscription));
    }

    private void doSubscribe(Flow.Subscription subscription) {

        sourceSubscription = subscription;

        // Targets have definitely been registered because this is guarded in onSubscribe
        // There is a possibility all target subscriptions already got cancelled
        // In this case, cancel the source subscription as well

        if (targets.isEmpty()) {
            sourceSubscription.cancel();
            return;
        }

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

        eventLoop.submit(() -> doNext(message));
    }

    private void doNext(T message) {

        messageBuffer.add(message);
        messageBufferEnd++;

        eventLoop.submit(this::dispatchMessages);
    }

    @Override
    public void onError(Throwable error) {

        eventLoop.submit(() -> doError(error));
    }

    private void doError(Throwable error) {

        clearBuffer();

        var completionError = error instanceof CompletionException
                ? error : new CompletionException(error.getMessage(), error);

        for (var subscriber: targets.keySet())
            subscriber.onError(completionError);
    }

    @Override
    public void onComplete() {

        eventLoop.submit(this::doComplete);
    }

    private void doComplete() {

        completeFlag = true;
        eventLoop.submit(this::dispatchMessages);
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
                    target.receiveIndex++;
                    targetSubscriber.onNext(message);
                }
            }
        }

        if (completeFlag) {

            for (var subscriberState: targets.entrySet()) {

                var targetSubscriber = subscriberState.getKey();
                var target = subscriberState.getValue();

                if (target.receiveIndex == messageBufferEnd && !target.completeFlag) {
                    target.completeFlag = true;
                    targetSubscriber.onComplete();
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

        eventLoop.submit(this::dispatchMessages);
    }

    private void cancelTargetSubscription(Flow.Subscriber<? super T> target) {

        targets.remove(target);

        if (targets.isEmpty()) {

            clearBuffer();

            if (sourceSubscription != null)
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
            eventLoop.submit(() -> requestTargetMessages(target, n));
        }

        @Override
        public void cancel() {
            eventLoop.submit(() -> cancelTargetSubscription(target));
        }
    }

    private static class HubTargetState {

        Flow.Subscription subscription;
        int requestIndex;
        int receiveIndex;
        boolean completeFlag;
    }
}
