/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.plugins.azure.storage;

import org.finos.tracdap.common.data.IDataContext;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.ParallelTransferOptions;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import org.apache.arrow.memory.ArrowBuf;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;


public class AzureBlobWriter implements Flow.Subscriber<ArrowBuf> {

    private static final boolean ALWAYS_OVERWRITE = true;

    private final BlobAsyncClient blobClient;
    private final CompletableFuture<Long> signal;
    private final IDataContext dataContext;

    private FluxTransformer fluxTransformer;
    private long nBytes;

    AzureBlobWriter(BlobAsyncClient blobClient, CompletableFuture<Long> signal, IDataContext dataContext) {

        this.blobClient = blobClient;
        this.signal = signal;
        this.dataContext = dataContext;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        fluxTransformer = new FluxTransformer();

        var fluxSubscription = new FluxSubscription(subscription);
        fluxTransformer.onSubscribe(fluxSubscription);

        // Using default transfer options
        var options = new ParallelTransferOptions();

        var scheduler = AzureScheduling.schedulerFor(dataContext.eventLoopExecutor());

        blobClient.upload(Flux.from(fluxTransformer), options, ALWAYS_OVERWRITE)
                .publishOn(scheduler)
                .subscribe(this::onSuccess, this::onFailure);
    }

    @Override
    public void onNext(ArrowBuf item) {
        nBytes += item.readableBytes();
        fluxTransformer.onNext(item);
    }

    @Override
    public void onError(Throwable error) {

        // onError() can be called before onSubscribe() in some cases
        // In this case, pass the error straight to the callback signal

        if (fluxTransformer != null)
            fluxTransformer.onError(error);
        else
            signal.completeExceptionally(error);
    }

    @Override
    public void onComplete() {
        fluxTransformer.onComplete();
    }

    private void onSuccess(BlockBlobItem blob) {
        signal.complete(nBytes);
    }

    private void onFailure(Throwable error) {
        signal.completeExceptionally(error);
    }

    private static class FluxTransformer implements Processor<ArrowBuf, ByteBuffer> {

        private Subscriber<? super ByteBuffer> subscriber;
        private Subscription subscription;

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> subscriber) {

            this.subscriber = subscriber;

            if (subscription != null)
                subscriber.onSubscribe(subscription);
        }

        @Override
        public void onSubscribe(Subscription subscription) {

            this.subscription = subscription;

            if (subscriber != null)
                subscriber.onSubscribe(subscription);
        }

        @Override
        public void onNext(ArrowBuf arrowBuf) {

            try (arrowBuf) {  // always release arrow buf

                var copyBuf = ByteBuffer.allocateDirect((int) arrowBuf.readableBytes());
                copyBuf.put(arrowBuf.nioBuffer());
                copyBuf.flip();

                subscriber.onNext(copyBuf);
            }
        }

        @Override
        public void onError(Throwable error) {
            subscriber.onError(error);
        }

        @Override
        public void onComplete() {
            subscriber.onComplete();
        }
    }

    private static class FluxSubscription implements Subscription {

        private final Flow.Subscription innerSubscription;

        FluxSubscription(Flow.Subscription innerSubscription) {
            this.innerSubscription = innerSubscription;
        }

        @Override
        public void request(long n) {
            innerSubscription.request(n);
        }

        @Override
        public void cancel() {
            innerSubscription.cancel();
        }
    }
}
