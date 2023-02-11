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

package org.finos.tracdap.common.data.pipeline;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;


public class GrpcUploadSource<TRequest, TResponse> {

    // Source stream class for handling file / data uploads
    // The pattern is one message with metadata followed by a stream of content
    // The first message may also contain content

    // This class is intended as a very simple pass-through,
    // from a gRPC event stream to the data pipeline event stream

    private final ServerCallStreamObserver<TResponse> response;
    private final StreamObserver<TRequest> request;
    private final CompletableFuture<TRequest> firstMessage;
    private Flow.Subscriber<? super TRequest> subscriber;
    private Flow.Subscription subscription;

    private boolean requestedFirst;
    private boolean sentFirst;
    private boolean grpcComplete;

    @SuppressWarnings("unused") // Request class is needed to infer types for the upload source
    public GrpcUploadSource(Class<TRequest> requestClass, StreamObserver<TResponse> response) {

        if (!(response instanceof ServerCallStreamObserver))
            throw new EUnexpected();

        this.response = (ServerCallStreamObserver<TResponse>) response;
        this.response.disableAutoInboundFlowControl();
        this.response.setCompression("gzip");
        this.response.setMessageCompression(true);
        this.response.setOnCancelHandler(this::apiOnCancel);

        this.request = new UploadRequestObserver();
        this.firstMessage = new CompletableFuture<>();
    }


    // Setup

    public StreamObserver<TRequest> start() {
        response.request(1);
        return request;
    }

    public void succeeded(TResponse result) {
        response.onNext(result);
        response.onCompleted();
    }

    public Void failed(Throwable error) {
        response.onError(error);
        return null;
    }

    public CompletionStage<TRequest> firstMessage() {
        return firstMessage;
    }

    public Flow.Publisher<ByteBuf> dataStream(Function<TRequest, ByteBuf> unwrapFunc) {
        return new UploadPublisher(unwrapFunc);
    }


    // Stream event handlers

    private void apiOnNext(TRequest value) {

        if (!firstMessage.isDone()) {

            firstMessage.complete(value);

            if (requestedFirst) {

                sentFirst = true;
                subscriber.onNext(value);

                if (grpcComplete)
                    subscriber.onComplete();
            }
        }
        else {

            subscriber.onNext(value);
        }
    }

    private void apiOnComplete() {

        if (!firstMessage.isDone()) {

            // Should never happen, gRPC will always send onNext before onComplete
            firstMessage.completeExceptionally(new EUnexpected());
        }
        else {

            // Sometimes the incoming stream has just one message
            // Then onComplete might fire before the data stream is set up
            // If this happens, record the complete signal to send with the first message

            if (subscriber != null)
                subscriber.onComplete();
            else
                grpcComplete = true;
        }
    }

    private void apiOnError(Throwable error) {

        if (!firstMessage.isDone()) {

            firstMessage.completeExceptionally(error);

            if (requestedFirst) {
                sentFirst = true;
                subscriber.onError(error);
            }
        }
        else {

            subscriber.onError(error);
        }
    }

    private void apiOnCancel() {
        // TODO: Cancel not implemented yet
    }

    private void pipelineSubscribe(Flow.Subscriber<? super TRequest> subscriber) {

        if (subscription != null)
            throw new ETracInternal("Upload source is already subscribed");

        this.subscription = new UploadSubscription();
        this.subscriber = subscriber;
        this.subscriber.onSubscribe(subscription);
    }

    private void pipelineRequest(long n) {

        // The first message has already been received when the pipe starts and may contain data content
        // It is also possible the stream completed already (if there was just a single message),
        // or that an error occurred before the first message was received

        if (!sentFirst && firstMessage.isDone()) {

            sentFirst = true;

            try {

                subscriber.onNext(firstMessage.getNow(null));

                if (grpcComplete)
                    subscriber.onComplete();
            }
            catch (Throwable e) {

                subscriber.onError(e);
            }
        }

        // One message was already requested by start(), don't pump more messages than requested

        if (!requestedFirst) {
            requestedFirst = true;
            if (n > 1)
                response.request((int) n - 1);
        }
        else
            response.request((int) n);
    }

    private void pipelineCancel() {
        // TODO: Cancel not implemented yet
    }


    // Wrapper classes for the gRPC and data pipeline interfaces

    private class UploadRequestObserver implements StreamObserver<TRequest> {

        @Override
        public void onNext(TRequest value) {
            apiOnNext(value);
        }

        @Override
        public void onError(Throwable t) {
            apiOnError(t);
        }

        @Override
        public void onCompleted() {
            apiOnComplete();
        }
    }

    private class UploadPublisher implements Flow.Processor<TRequest, ByteBuf> {

        private final Function<TRequest, ByteBuf> _unwrapFunc;
        private Flow.Subscriber<? super ByteBuf> _subscriber;

        public UploadPublisher(Function<TRequest, ByteBuf> unwrapFunc) {
            _unwrapFunc = unwrapFunc;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuf> subscriber) {
            _subscriber = subscriber;
            pipelineSubscribe(this);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            _subscriber.onSubscribe(subscription);
        }

        @Override
        public void onNext(TRequest item) {
            // Do not publish empty messages to the data pipeline
            // Particularly if the first message is empty, this can break some parsers
            var buffer = _unwrapFunc.apply(item);
            if (buffer.readableBytes() > 0)
                _subscriber.onNext(buffer);
        }

        @Override
        public void onError(Throwable throwable) {
            _subscriber .onError(throwable);
        }

        @Override
        public void onComplete() {
            _subscriber .onComplete();
        }
    }

    private class UploadSubscription implements Flow.Subscription {

        @Override
        public void request(long n) {
            pipelineRequest(n);
        }

        @Override
        public void cancel() {
            pipelineCancel();
        }
    }
}
