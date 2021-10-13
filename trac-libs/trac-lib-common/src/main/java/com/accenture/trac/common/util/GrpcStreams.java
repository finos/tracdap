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

import com.accenture.trac.common.exception.EInputValidation;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;


public class GrpcStreams {

    public static <T>
    BiConsumer<T, Throwable> resultHandler(StreamObserver<T> grpcObserver) {

        return new GrpcResultHandler<>(grpcObserver);
    }

    public static <T>
    StreamObserver<T> unaryResult(CompletableFuture<T> result) {

        return new UnaryResultObserver<>(result);
    }

    public static <T>
    Flow.Subscriber<T> relay(StreamObserver<T> observer) {

        return new GrpcStreamSubscriber<>(observer);
    }

    public static <T>
    StreamObserver<T> relay(Flow.Subscriber<T> subscriber) {

        return new GrpcStreamPublisher<>(subscriber);
    }


    public static class GrpcResultHandler<T> implements BiConsumer<T, Throwable> {

        private final StreamObserver<T> grpcObserver;

        public GrpcResultHandler(StreamObserver<T> grpcObserver) {
            this.grpcObserver = grpcObserver;
        }

        @Override
        public void accept(T result, Throwable error) {

            if (error == null) {
                grpcObserver.onNext(result);
                grpcObserver.onCompleted();
                return;
            }

            // Unwrap future errors
            if (error instanceof CompletionException)
                error = error.getCause();

            if (error instanceof EInputValidation) {

                var status = Status.fromCode(Status.Code.INVALID_ARGUMENT)
                        .withDescription(error.getMessage())
                        .withCause(error);

                grpcObserver.onError(status.asRuntimeException());
                return;
            }

            var status = Status.fromCode(Status.Code.INTERNAL)
                    .withDescription(Status.INTERNAL.getDescription())
                    .withCause(error);

            grpcObserver.onError(status.asRuntimeException());

            // TODO: More error types and logging

        }
    }

    public static class UnaryResultObserver<T> implements StreamObserver<T> {

        private final CompletableFuture<T> resultFuture;
        private final AtomicReference<T> resultBuffer;

        public UnaryResultObserver(CompletableFuture<T> result) {

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

    public static class GrpcStreamSubscriber<T> implements Flow.Subscriber<T> {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final StreamObserver<T> grpcObserver;

        private final AtomicBoolean subscribed = new AtomicBoolean(false);
        private Flow.Subscription subscription;

        public GrpcStreamSubscriber(StreamObserver<T> grpcObserver) {
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

            log.error("gRPC inbound stream failed: {}", error.getMessage(), error);

            subscription.cancel();
            grpcObserver.onError(error);
        }

        @Override
        public void onComplete() {

            log.info("gRPC inbound stream complete");

            grpcObserver.onCompleted();
        }
    }

    public static class GrpcStreamPublisher<T> implements StreamObserver<T> {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final Flow.Subscriber<T> subscriber;

        public static class Subscription implements Flow.Subscription {
            @Override public void request(long n) {}
            @Override public void cancel() {}
        }

        public GrpcStreamPublisher(Flow.Subscriber<T> subscriber) {

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

            log.error("gRPC outbound stream failed: {}", error.getMessage(), error);

            subscriber.onError(error);
        }

        @Override
        public void onCompleted() {

            log.info("gRPC outbound stream complete");

            subscriber.onComplete();
        }
    }
}
