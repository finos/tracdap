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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;

import com.google.protobuf.MessageLite;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

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
    private BiFunction<TBuilder, ByteBuf, TBuilder> dataFunc;

    private final TBuilder aggregateResponse;
    private final CompositeByteBuf aggregateBuffer;

    private CompletableFuture<TBuilder> firstMessage;
    private Flow.Subscription subscription;

    private long nRequested;
    private long nReceived;


    public GrpcDownloadSink(StreamObserver<TResponse> response, Supplier<TBuilder> builder, boolean streaming) {

        if (!(response instanceof ServerCallStreamObserver))
            throw new EUnexpected();

        responseStream = (ServerCallStreamObserver<TResponse>) response;
        responseStream.setCompression("gzip");
        responseStream.setMessageCompression(true);
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
            aggregateBuffer = ByteBufAllocator.DEFAULT.compositeBuffer();
        }
    }

    public <TRequest extends MessageLite> CompletionStage<TRequest> start(TRequest request) {

        return CompletableFuture.completedFuture(request);
    }

    public Void failed(Throwable error) {

        if (!streaming)
            releaseAggregate();

        responseStream.onError(error);

        if (subscription != null)
            subscription.cancel();

        return null;
    }

    public <TResult> CompletableFuture<TResult> firstMessage(BiFunction<TBuilder, TResult, TBuilder> resultFunc) {

        if (firstMessage != null)
            throw new EUnexpected();

        var future = new CompletableFuture<TResult>();
        firstMessage = future.thenApply(s -> resultFunc.apply(builder.get(), s));

        firstMessage
            .thenAccept(this::firstMessageComplete)
            .exceptionally(this::firstMessageFailed);

        return future;
    }

    public Flow.Subscriber<ByteBuf> dataStream(BiFunction<TBuilder, ByteBuf, TBuilder> wrapFunc) {

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

        if (!streaming)
            releaseAggregate();

        responseStream.onError(error);

        if (subscription != null)
            subscription.cancel();

        return null;
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

    @SuppressWarnings("unchecked")
    private void pipelineOnNext(ByteBuf chunk) {

        nReceived += 1;

        if (streaming) {
            var msg = dataFunc.apply(builder.get(), chunk).build();
            responseStream.onNext((TResponse) msg);
        }
        else
            addToAggregate(chunk);

        if (nRequested - nReceived < REQUEST_MIN_BUFFER)
            requestMore();
    }

    @SuppressWarnings("unchecked")
    private void pipelineOnComplete() {

        if (!streaming) {
            var msg = dataFunc.apply(aggregateResponse, aggregateBuffer).build();
            responseStream.onNext((TResponse) msg);
        }

        responseStream.onCompleted();
    }

    private void pipelineOnError(Throwable error) {

        if (!streaming)
            releaseAggregate();

        responseStream.onError(error);
    }

    private void requestMore() {

        if (!responseStream.isReady())
            return;

        long nPending = nRequested - nReceived;
        long n = REQUEST_MAX_BUFFER - nPending;
        nRequested += n;
        subscription.request(n);
    }

    private void addToAggregate(ByteBuf chunk) {

        aggregateBuffer.addComponent(true, chunk);
    }

    private void releaseAggregate() {

        if (aggregateBuffer.refCnt() > 0)
            aggregateBuffer.release();
    }

    private class DownloadSubscriber implements Flow.Subscriber<ByteBuf> {

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            pipelineOnSubscribe(subscription);
        }

        @Override
        public void onNext(ByteBuf chunk) {
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
