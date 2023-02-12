/*
 * Copyright 2023 Accenture Global Solutions Limited
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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public class DelayedPublisher<T> implements Flow.Publisher<T> {

    private final Flow.Publisher<T> publisher;
    private final CompletionStage<?> signal;

    public DelayedPublisher(Flow.Publisher<T> publisher, CompletionStage<?> signal) {
        this.publisher = publisher;
        this.signal = signal;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {

        signal.whenComplete((result, error) -> {

            if (error == null)
                publisher.subscribe(subscriber);

            else {
                subscriber.onSubscribe(new FailedSignalSubscription());
                subscriber.onError(error);
            }
        });
    }

    private static class FailedSignalSubscription implements Flow.Subscription  {

        @Override
        public void request(long n) {

        }

        @Override
        public void cancel() {

        }
    }
}
