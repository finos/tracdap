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

import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;


public class GrpcStreams {

    public static <T>
    BiConsumer<T, Throwable> resultHandler(StreamObserver<T> grpcObserver) {

        return new GrpcResultHandler<>(grpcObserver);
    }

    public static <T>
    StreamObserver<T> resultObserver(CompletableFuture<T> result) {

        return new GrpcResultObserver<>(result);
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

            if (error != null) {

                LoggerFactory.getLogger(getClass()).error("Error escaped processing", error);

                if (error instanceof StatusRuntimeException || error instanceof StatusException)
                    grpcObserver.onError(error);
                else {

                    var statusCode = Status.Code.INTERNAL;

                    var errorMessage = error.getMessage();

                    var status = Status.fromCode(statusCode)
                            .withDescription(errorMessage)
                            .withCause(error);

                    grpcObserver.onError(status.asRuntimeException());
                }
            }
            else {
                grpcObserver.onNext(result);
                grpcObserver.onCompleted();
            }
        }
    }

    public static class GrpcResultObserver<T> implements StreamObserver<T> {

        private final CompletableFuture<T> result;

        public GrpcResultObserver(CompletableFuture<T> result) {

            // Raise an error on the processing thread if the result is already set

            if (result.isDone())
                throw new EUnexpected();

            this.result = result;
        }

        @Override
        public void onNext(T value) {

            var resultSet = result.complete(value);

            // Raise an error on the processing thread if the result is already set

            if (!resultSet)
                throw new EUnexpected();
        }

        @Override
        public void onError(Throwable error) {

            var errorSet = result.completeExceptionally(error);

            // Raise an error on the processing thread if the result is already set

            if (!errorSet)
                throw new EUnexpected();
        }

        @Override
        public void onCompleted() {

            // Raise an error on the processing thread if a result has not been set

            if (!result.isDone())
                throw new EUnexpected();
        }
    }

    public static class GrpcStreamSubscriber<T> implements Flow.Subscriber<T> {

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
            subscription.cancel();
            grpcObserver.onError(error);
        }

        @Override
        public void onComplete() {
            grpcObserver.onCompleted();
        }
    }

    public static class GrpcStreamPublisher<T> implements StreamObserver<T> {

        private final Flow.Subscriber<T> subscriber;
        private final Flow.Subscription subscription;

        public static class Subscription implements Flow.Subscription {
            @Override public void request(long n) {}
            @Override public void cancel() {}
        }

        public GrpcStreamPublisher(Flow.Subscriber<T> subscriber) {
            this.subscriber = subscriber;
            this.subscription = new Subscription();

            subscriber.onSubscribe(this.subscription);
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
    }
}
