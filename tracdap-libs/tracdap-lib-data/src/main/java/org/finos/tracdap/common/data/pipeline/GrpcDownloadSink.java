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

import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.ArrowBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Supplier;


public class GrpcDownloadSink<TResponse extends MessageLite, TBuilder extends MessageLite.Builder> {

    public static final boolean STREAMING = true;
    public static final boolean AGGREGATED = false;

    private static final long REQUEST_MAX_BUFFER = 128;
    private static final long REQUEST_MIN_BUFFER = 64;

    private final ServerCallStreamObserver<TResponse> responseStream;
    private final boolean streaming;

    private final Supplier<TBuilder> builder;
    private BiFunction<TBuilder, ByteString, TBuilder> dataFunc;

    private final TBuilder aggregateResponse;
    private final List<ArrowBuf> aggregateBuffer;

    private CompletableFuture<TBuilder> firstMessage;
    private Flow.Subscription subscription;
    private Runnable cleanup;

    private long nRequested;
    private long nReceived;


    public GrpcDownloadSink(StreamObserver<TResponse> response, Supplier<TBuilder> builder, boolean streaming) {

        if (!(response instanceof ServerCallStreamObserver))
            throw new EUnexpected();

        responseStream = (ServerCallStreamObserver<TResponse>) response;
        responseStream.setOnCancelHandler(this::apiOnCancel);
        responseStream.setOnReadyHandler(this::apiOnReady);

        this.builder = builder;
        this.streaming = streaming;

        if (streaming) {
            aggregateResponse = null;
            aggregateBuffer = null;
        }
        else {
            aggregateResponse = builder.get();
            aggregateBuffer = new ArrayList<>();
        }
    }

    public void whenComplete(Runnable cleanup) {
        this.cleanup = cleanup;
    }

    public <TRequest extends MessageLite> CompletionStage<TRequest> start(TRequest request) {

        return CompletableFuture.completedFuture(request);
    }

    public Void failed(Throwable error) {

        try {

            if (!streaming)
                releaseAggregate();

            responseStream.onError(error);

            if (subscription != null)
                subscription.cancel();

            return null;
        }
        finally {
            if (cleanup != null) {
                cleanup.run();
                cleanup = null;
            }
        }
    }

    @SuppressWarnings("unused")
    public <TResult> CompletableFuture<TResult>
    firstMessage(BiFunction<TBuilder, TResult, TBuilder> resultFunc, Class<TResult> resultType) {

        // Sometimes TResult cannot be inferred, allow client code to be explicit

        return firstMessage(resultFunc);
    }

    public <TResult> CompletableFuture<TResult>
    firstMessage(BiFunction<TBuilder, TResult, TBuilder> resultFunc) {

        if (firstMessage != null)
            throw new EUnexpected();

        var future = new CompletableFuture<TResult>();
        firstMessage = future.thenApply(s -> resultFunc.apply(builder.get(), s));

        firstMessage
            .thenAccept(this::firstMessageComplete)
            .exceptionally(this::firstMessageFailed);

        return future;
    }

    public Flow.Subscriber<ArrowBuf>
    dataStream(BiFunction<TBuilder, ByteString, TBuilder> wrapFunc) {

        this.dataFunc = wrapFunc;
        return new DownloadSubscriber();
    }

    private void apiOnCancel() {
        // TODO: Cancel not supported yet
    }

    private void apiOnReady() {
        if (nRequested - nReceived < REQUEST_MIN_BUFFER && subscription != null)
            requestMore();
    }

    @SuppressWarnings("unchecked")
    private void firstMessageComplete(TBuilder response) {

        if (streaming)
            responseStream.onNext((TResponse) response.build());
        else
            aggregateResponse.mergeFrom(response.buildPartial());

        if (subscription != null)
            requestMore();
    }

    private Void firstMessageFailed(Throwable error) {

        try {

            if (!streaming)
                releaseAggregate();

            responseStream.onError(error);

            if (subscription != null)
                subscription.cancel();

            return null;
        }
        finally {
            if (cleanup != null) {
                cleanup.run();
                cleanup = null;
            }
        }
    }

    private void pipelineOnSubscribe(Flow.Subscription subscription) {

        if (this.subscription != null)
            throw new ETracInternal("Upload source is already subscribed");

        this.subscription = subscription;

        if (firstMessage.isCompletedExceptionally()) {
            subscription.cancel();
        }
        else if (firstMessage.isDone()) {
            requestMore();
        }
    }

    private void pipelineOnNext(ArrowBuf chunk) {

        nReceived += 1;

        if (streaming)
            Bytes.readFromStream(chunk, this::pipelineSendChunk);
        else
            aggregateBuffer.add(chunk);

        if (nRequested - nReceived < REQUEST_MIN_BUFFER)
            requestMore();
    }

    @SuppressWarnings("unchecked")
    private void pipelineSendChunk(ByteBuffer chunk) {

        var protoBuilder = builder.get();
        var protoBytes = UnsafeByteOperations.unsafeWrap(chunk);

        dataFunc.apply(protoBuilder, protoBytes);

        var protoMsg = (TResponse) protoBuilder.build();

        responseStream.onNext(protoMsg);
    }

    @SuppressWarnings("unchecked")
    private void pipelineOnComplete() {

        try {

            if (!streaming) {

                var bufferBytes = Bytes.readFromBuffer(aggregateBuffer);
                var protoBytes = UnsafeByteOperations.unsafeWrap(bufferBytes);

                dataFunc.apply(aggregateResponse, protoBytes);

                var protoMsg = (TResponse) aggregateResponse.build();

                responseStream.onNext(protoMsg);
            }

            responseStream.onCompleted();
        }
        finally {
            if (cleanup != null) {
                cleanup.run();
                cleanup = null;
            }
        }
    }

    private void pipelineOnError(Throwable error) {

        try {

            if (!streaming)
                releaseAggregate();

            responseStream.onError(error);
        }
        finally {
            if (cleanup != null) {
                cleanup.run();
                cleanup = null;
            }
        }
    }

    private void requestMore() {

        if (!responseStream.isReady())
            return;

        long nPending = nRequested - nReceived;
        long n = REQUEST_MAX_BUFFER - nPending;
        nRequested += n;
        subscription.request(n);
    }

    private void releaseAggregate() {

        aggregateBuffer.forEach(ArrowBuf::close);
        aggregateBuffer.clear();
    }

    private class DownloadSubscriber implements Flow.Subscriber<ArrowBuf> {

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            pipelineOnSubscribe(subscription);
        }

        @Override
        public void onNext(ArrowBuf chunk) {
            pipelineOnNext(chunk);
        }

        @Override
        public void onError(Throwable throwable) {
            pipelineOnError(throwable);
        }

        @Override
        public void onComplete() {
            pipelineOnComplete();
        }
    }
}
