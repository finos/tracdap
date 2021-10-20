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

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.function.Consumer;


public class HubProcessor<T> implements Flow.Processor<T, T> {

    private Flow.Subscription sourceSubscription;
    private final Map<Flow.Subscriber<? super T>, HubTargetState> targets;

    private final LinkedList<T> messageBuffer;
    private final Consumer<T> releaseFunc;
    private final OrderedEventExecutor eventLoop;

    private long messageBufferStart;
    private long messageBufferEnd;
    private long sourceRequestIndex;
    private boolean completeFlag;

    public HubProcessor(OrderedEventExecutor eventLoop, Consumer<T> releaseFunc) {

        this.targets = new ConcurrentHashMap<>();

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
