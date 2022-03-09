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

package org.finos.tracdap.common.grpc;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.Flow;


public class GrpcServerRequestStream<T> implements StreamObserver<T> {

    private final Flow.Subscriber<T> subscriber;

    public GrpcServerRequestStream(Flow.Subscriber<T> subscriber) {

        this.subscriber = subscriber;

        var subscription = new Subscription();
        subscriber.onSubscribe(subscription);
    }

    @Override
    public void onNext(T value) {
        subscriber.onNext(value);
    }

    @Override
    public void onError(Throwable error) {

        subscriber.onError(error);
    }

    @Override
    public void onCompleted() {

        subscriber.onComplete();
    }

    // Subscription is ignored, using gRPC automatic flow control
    // Messages will be published as they come in

    private static class Subscription implements Flow.Subscription {
        @Override public void request(long n) {}
        @Override public void cancel() {}
    }
}
