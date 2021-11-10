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

import com.accenture.trac.common.exception.*;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
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
    BiConsumer<T, Throwable> serverResponseHandler(MethodDescriptor<?, T> method, StreamObserver<T> observer) {

        return new ServerResponseHandler<>(method, observer);
    }

    public static <T>
    Flow.Subscriber<T> serverResponseStream(MethodDescriptor<?, T> method, StreamObserver<T> observer) {

        return new ServerResponseStream<>(method, observer);
    }

    public static <T>
    StreamObserver<T> serverRequestStream(Flow.Subscriber<T> subscriber) {

        return new ServerRequestStream<>(subscriber);
    }

    public static <T>
    StreamObserver<T> clientResponseHandler(CompletableFuture<T> result) {

        return new ClientResultHandler<>(result);
    }

    public static <T>
    StreamObserver<T> clientResponseStream(Flow.Subscriber<T> subscriber) {

        return new ClientResponseStream<>(subscriber);
    }

    public static <T>
    Flow.Subscriber<T> clientRequestStream(StreamObserver<T> observer) {

        return new ClientRequestStream<>(observer);
    }


    public static class ServerResponseHandler<T> implements BiConsumer<T, Throwable> {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final MethodDescriptor<?, T> method;
        private final StreamObserver<T> grpcObserver;

        public ServerResponseHandler(MethodDescriptor<?, T> method, StreamObserver<T> grpcObserver) {
            this.method = method;
            this.grpcObserver = grpcObserver;
        }

        @Override
        public void accept(T result, Throwable error) {

            if (error == null) {

                log.info("CLIENT STREAMING CALL SUCCEEDED: [{}]", method.getBareMethodName());

                grpcObserver.onNext(result);
                grpcObserver.onCompleted();
            }
            else {

                var status = translateErrorStatus(error);
                var statusError = status.asRuntimeException();

                log.error("CLIENT STREAMING CALL FAILED: [{}] {}",
                        method.getBareMethodName(),
                        statusError.getMessage(),
                        statusError);

                grpcObserver.onError(statusError);
            }
        }
    }

    public static class ServerResponseStream<T> implements Flow.Subscriber<T> {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final MethodDescriptor<?, T> method;
        private final StreamObserver<T> grpcObserver;

        private final AtomicBoolean subscribed = new AtomicBoolean(false);
        private Flow.Subscription subscription;

        public ServerResponseStream(
                MethodDescriptor<?, T> method,
                StreamObserver<T> grpcObserver) {

            this.method = method;
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

            var status = translateErrorStatus(error);
            var statusError = status.asRuntimeException();

            log.error("SERVER STREAMING CALL FAILED: [{}] {}",
                    method.getBareMethodName(),
                    statusError.getMessage(),
                    statusError);

            grpcObserver.onError(statusError);
            subscription.cancel();
        }

        @Override
        public void onComplete() {

            log.info("SERVER STREAMING CALL SUCCEEDED: [{}]", method.getBareMethodName());

            grpcObserver.onCompleted();
        }
    }

    public static class ServerRequestStream<T> implements StreamObserver<T> {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final Flow.Subscriber<T> subscriber;

        public static class Subscription implements Flow.Subscription {
            @Override public void request(long n) {}
            @Override public void cancel() {}
        }

        public ServerRequestStream(Flow.Subscriber<T> subscriber) {

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

            log.error("Inbound server stream failed: {}", error.getMessage(), error);

            subscriber.onError(error);
        }

        @Override
        public void onCompleted() {

            log.info("Inbound server stream complete");

            subscriber.onComplete();
        }
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

            var status = translateErrorStatus(error);
            var statusError = status.asRuntimeException();

            log.error("Client streaming failed in client: {}", statusError.getMessage(), statusError);

            grpcObserver.onError(statusError);
            subscription.cancel();
        }

        @Override
        public void onComplete() {

            log.info("Client streaming succeeded in client");

            grpcObserver.onCompleted();
        }
    }

    private static Status translateErrorStatus(Throwable error) {

        // Unwrap future/streaming completion errors
        if (error instanceof CompletionException)
            error = error.getCause();

        // Status runtime exception is a gRPC exception that is already propagated in the event stream
        // This is most likely the result of an error when calling into another TRAC service
        // For now, pass these errors on directly
        // At some point there may (or may not) be benefit in wrapping/transforming upstream errors
        // E.g. to handle particular types of expected exception
        // However a lot of error response translate directly
        // E.g. for metadata not found or permission denied lower down the stack - there is little benefit to wrapping

        if (error instanceof StatusRuntimeException)
            return ((StatusRuntimeException) error).getStatus();

        if (error instanceof EInputValidation) {

            return Status.fromCode(Status.Code.INVALID_ARGUMENT)
                    .withDescription(error.getMessage())
                    .withCause(error);
        }

        if (error instanceof EVersionValidation) {

            return Status.fromCode(Status.Code.FAILED_PRECONDITION)
                    .withDescription(error.getMessage())
                    .withCause(error);
        }

        if (error instanceof EData) {

            return Status.fromCode(Status.Code.DATA_LOSS)
                    .withDescription(error.getMessage())
                    .withCause(error);
        }

        if (error instanceof EPluginNotAvailable) {

            return Status.fromCode(Status.Code.UNIMPLEMENTED)
                    .withDescription(error.getMessage())
                    .withCause(error);
        }

        // For anything unrecognized, fall back to an internal error

        return Status.fromCode(Status.Code.INTERNAL)
                .withDescription(Status.INTERNAL.getDescription())
                .withCause(error);
    }
}
