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

package com.accenture.trac.test.grpc;

import com.accenture.trac.common.exception.*;
import com.accenture.trac.common.grpc.GrpcErrorMapping;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class GrpcTestStreams {

    public static <T>
    Flow.Subscriber<T> clientRequestStream(StreamObserver<T> observer) {

        return new ClientRequestStream<>(observer);
    }

    public static <T>
    StreamObserver<T> clientResponseStream(Flow.Subscriber<T> subscriber) {

        return new ClientResponseStream<>(subscriber);
    }

    public static <T>
    StreamObserver<T> clientResponseHandler(CompletableFuture<T> result) {

        return new ClientResultHandler<>(result);
    }

    public static class ClientResultHandler<T> implements StreamObserver<T> {

        private final CompletableFuture<T> resultFuture;
        private final AtomicReference<T> resultBuffer;

        public ClientResultHandler(CompletableFuture<T> result) {

            // Raise an error on the processing thread if the result is already set

            if (result.isDone())
                throw new EUnexpected();

            this.resultFuture = result;
            this.resultBuffer = new AtomicReference<>(null);
        }

        @Override
        public void onNext(T resultValue) {

            var bufferedOk = resultBuffer.compareAndSet(null, resultValue);

            // Raise an error on the processing thread if a result has already been received

            if (!bufferedOk)
                throw new EUnexpected();
        }

        @Override
        public void onError(Throwable error) {

            var errorOk = resultFuture.completeExceptionally(error);

            // Raise an error on the processing thread if the result is already set

            if (!errorOk)
                throw new EUnexpected();
        }

        @Override
        public void onCompleted() {

            var resultValue = resultBuffer.get();

            // Raise an error if no result has been recorded via onNext

            if (resultValue == null)
                throw new EUnexpected();

            var resultOk = resultFuture.complete(resultValue);

            // Raise an error if the result Future has already been completed

            if (!resultOk)
                throw new EUnexpected();
        }
    }

    public static class ClientResponseStream<T> implements StreamObserver<T> {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final Flow.Subscriber<T> subscriber;

        public static class Subscription implements Flow.Subscription {
            @Override public void request(long n) {}
            @Override public void cancel() {}
        }

        public ClientResponseStream(Flow.Subscriber<T> subscriber) {

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

            log.error("Server streaming failed in client: {}", error.getMessage(), error);

            subscriber.onError(error);
        }

        @Override
        public void onCompleted() {

            log.info("Server streaming succeeded in client");

            subscriber.onComplete();
        }
    }

    public static class ClientRequestStream<T> implements Flow.Subscriber<T> {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final StreamObserver<T> grpcObserver;

        private final AtomicBoolean subscribed = new AtomicBoolean(false);
        private Flow.Subscription subscription;

        public ClientRequestStream(StreamObserver<T> grpcObserver) {
            this.grpcObserver = grpcObserver;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {

            var subscribedOk = this.subscribed.compareAndSet(false, true);

            if (!subscribedOk)
                throw new ETracInternal("Multiple subscriptions on gRPC observer wrapper");

            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(T item) {
            grpcObserver.onNext(item);
            subscription.request(1);
        }

        @Override
        public void onError(Throwable error) {

            var grpcError = GrpcErrorMapping.processError(error);

            log.error("Client streaming failed in client: {}", grpcError.getMessage(), grpcError);

            grpcObserver.onError(grpcError);
            subscription.cancel();
        }

        @Override
        public void onComplete() {

            log.info("Client streaming succeeded in client");

            grpcObserver.onCompleted();
        }
    }
}
