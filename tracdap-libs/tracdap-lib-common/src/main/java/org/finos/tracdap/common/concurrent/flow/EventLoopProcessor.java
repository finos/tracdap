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

import io.netty.util.concurrent.OrderedEventExecutor;

import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;

public class EventLoopProcessor<T> implements Flow.Processor<T, T> {

    private final OrderedEventExecutor executor;

    private Flow.Subscriber<? super T> target;
    private Flow.Subscription sourceSubscription;

    public EventLoopProcessor(OrderedEventExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {

        this.target = subscriber;
        var targetSubscription = new EventLoopSubscription(sourceSubscription);

        executor.execute(() -> target.onSubscribe(targetSubscription));
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

        var completionError = (error instanceof CompletionException)
                ? error : new CompletionException(error.getMessage(), error);

        executor.execute(() -> target.onError(completionError));
    }

    @Override
    public void onComplete() {
        executor.execute(target::onComplete);
    }

    private class EventLoopSubscription implements Flow.Subscription {

        private final Flow.Subscription sourceSubscription;

        EventLoopSubscription(Flow.Subscription sourceSubscription) {
            this.sourceSubscription = sourceSubscription;
        }

        @Override
        public void request(long n) {
            executor.execute(() -> sourceSubscription.request(n));
        }

        @Override
        public void cancel() {
            executor.execute(sourceSubscription::cancel);
        }
    }
}
