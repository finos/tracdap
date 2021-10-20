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

import com.accenture.trac.common.exception.EUnexpected;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public class FutureResultPublisher<T> implements Flow.Publisher<T> {

    private final AtomicReference<ResultState> state;
    private Flow.Subscriber<? super T> subscriber;

    public FutureResultPublisher(CompletionStage<T> source) {

        this.state = new AtomicReference<>(new ResultState());
        source.whenComplete(this::acceptResult);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {

        this.subscriber = subscriber;

        var subscription = new Subscription();
        subscriber.onSubscribe(subscription);
    }

    private class Subscription implements Flow.Subscription {

        @Override
        public void request(long n) {

            var priorState = state.getAndUpdate(s1 -> {
                var s2 = s1.clone();
                s2.requested = true;
                return s2;
            });

            if (!priorState.requested && !priorState.cancelled) {

                if (priorState.result != null) {
                    subscriber.onNext(priorState.result);
                    subscriber.onComplete();
                }

                if (priorState.error != null) {
                    subscriber.onError(priorState.error);
                }
            }
        }

        @Override
        public void cancel() {

            state.getAndUpdate(s1 -> {
                var s2 = s1.clone();
                s2.cancelled = true;
                return s2;
            });
        }
    }

    private void acceptResult(T result, Throwable error) {

        var prior = this.state.getAndUpdate(s1 -> {
            var s2 =s1.clone();
            s2.result = result;
            s2.error = error;
            return s2;
        });

        if (prior.requested && !prior.cancelled) {

            if (error == null) {
                this.subscriber.onNext(result);
                this.subscriber.onComplete();
            }
            else
                this.subscriber.onError(error);
        }
    }

    private class ResultState implements Cloneable {

        boolean requested;
        boolean cancelled;

        T result;
        Throwable error;

        @Override
        @SuppressWarnings("unchecked")
        public ResultState clone() {

            try {
                return (ResultState) super.clone();
            }
            catch (CloneNotSupportedException ex) {
                throw new EUnexpected();
            }
        }
    }
}
