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

package org.finos.tracdap.plugins.azure.storage;

import org.finos.tracdap.common.data.IDataContext;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.DownloadRetryOptions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import org.apache.arrow.memory.ArrowBuf;

import java.nio.ByteBuffer;
import java.util.concurrent.Flow;


public class AzureBlobReader implements Flow.Publisher<ArrowBuf> {

    private static final int DEFAULT_RETRIES = 1;
    private static final boolean NO_MD5_CHECK = false;

    private final BlobAsyncClient blobClient;
    private final IDataContext dataContext;

    private FluxSubscriber fluxSubscriber;

    private boolean cancelled;
    private boolean failed;

    AzureBlobReader(BlobAsyncClient blobClient, IDataContext dataContext) {

        this.blobClient = blobClient;
        this.dataContext = dataContext;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ArrowBuf> subscriber) {

        fluxSubscriber = new FluxSubscriber(subscriber);

        var range = new BlobRange(0);  // or offset, size

        var options = new DownloadRetryOptions()
                .setMaxRetryRequests(DEFAULT_RETRIES);

        var conditions = new BlobRequestConditions();  // no special conditions

        var eventLoop = AzureScheduling.schedulerFor(dataContext.eventLoopExecutor());

        blobClient.downloadStreamWithResponse(range, options, conditions, NO_MD5_CHECK)
                .subscribeOn(eventLoop)
                .subscribe(this::onDownload, fluxSubscriber::onError);
    }

    private void onDownload(BlobDownloadAsyncResponse asyncDownload) {

        var eventLoop = AzureScheduling.schedulerFor(dataContext.eventLoopExecutor());

        asyncDownload.getValue()
                .subscribeOn(eventLoop)
                .subscribe(fluxSubscriber);
    }

    private class FluxSubscriber implements Subscriber<ByteBuffer> {

        private final Flow.Subscriber<? super ArrowBuf> subscriber;

        FluxSubscriber(Flow.Subscriber<? super ArrowBuf> innerSubscriber) {
            this.subscriber = innerSubscriber;
        }

        @Override
        public void onSubscribe(Subscription innerSubscription) {

            var subscription = new FluxSubscription(innerSubscription);
            subscriber.onSubscribe(subscription);
        }

        @Override
        public void onNext(ByteBuffer buffer) {

            if (cancelled || failed)
                return;

            var allocator = dataContext.arrowAllocator();
            var size = buffer.remaining();

            try (var arrowBuf = allocator.buffer(size)) {

                arrowBuf.setBytes(0, buffer);
                arrowBuf.writerIndex(size);

                arrowBuf.getReferenceManager().retain();

                subscriber.onNext(arrowBuf);
            }
        }

        @Override
        public void onError(Throwable t) {

            if (cancelled || failed)
                return;

            failed = true;
            subscriber.onError(t);
        }

        @Override
        public void onComplete() {

            if (cancelled || failed)
                return;

            subscriber.onComplete();
        }
    }

    private class FluxSubscription implements Flow.Subscription {

        private final Subscription innerSubscription;

        FluxSubscription(Subscription innerSubscription) {
            this.innerSubscription = innerSubscription;
        }

        @Override
        public void request(long n) {
            innerSubscription.request(n);
        }

        @Override
        public void cancel() {
            cancelled = true;
            innerSubscription.cancel();
        }
    }
}
