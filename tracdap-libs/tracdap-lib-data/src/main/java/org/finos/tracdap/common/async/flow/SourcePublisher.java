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

package org.finos.tracdap.common.async.flow;

import org.finos.tracdap.common.exception.ETracInternal;

import java.util.Iterator;
import java.util.concurrent.Flow;
import java.util.stream.Stream;

public class SourcePublisher<T> implements Flow.Publisher<T> {

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
