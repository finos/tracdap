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

import java.util.concurrent.Flow;
import java.util.function.BiConsumer;

public class InterceptProcessor<T> implements Flow.Processor<T, T> {

    Flow.Subscriber<? super T> target;
    Flow.Subscription subscription;

    private final BiConsumer<T, Throwable> resultInterceptor;

    public InterceptProcessor(BiConsumer<T, Throwable> resultInterceptor) {

        this.resultInterceptor = resultInterceptor;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {

        if (target != null)
            throw new IllegalStateException("Multiple target subscriptions for intercept processor");

        target = subscriber;

        if (subscription != null)
            target.onSubscribe(subscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        if (this.subscription != null)
            throw new IllegalStateException("Multiple source subscriptions for intercept processor");

        this.subscription = subscription;

        if (target != null)
            target.onSubscribe(subscription);
    }

    @Override
    public void onNext(T item) {

        target.onNext(item);
    }

    @Override
    public void onError(Throwable error) {

        if (resultInterceptor != null)
            resultInterceptor.accept(null, error);

        target.onError(error);
    }

    @Override
    public void onComplete() {

        if (resultInterceptor != null)
            resultInterceptor.accept(null, null);

        target.onComplete();
    }
}
