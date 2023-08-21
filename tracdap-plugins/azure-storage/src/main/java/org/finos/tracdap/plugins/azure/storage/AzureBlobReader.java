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
import org.finos.tracdap.common.storage.CommonFileReader;
import org.finos.tracdap.common.storage.StorageErrors;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.DownloadRetryOptions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;


public class AzureBlobReader extends CommonFileReader {

    private static final int DEFAULT_CHUNK_SIZE = 2097152;  // 2 MB
    private static final int DEFAULT_RETRIES = 1;
    private static final boolean NO_MD5_CHECK = false;

    private final BlobAsyncClient blobClient;
    private final IDataContext dataContext;

    private final long offset;
    private final long size;

    private FluxSubscriber fluxSubscriber;
    private Subscription subscription;
    private int requestCache;

    AzureBlobReader(
            BlobAsyncClient blobClient, IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath,
            long offset, long limit, long chunkSize) {

        super(dataContext, errors, storageKey, storagePath, chunkSize, 2, 32);

        this.blobClient = blobClient;
        this.dataContext = dataContext;

        this.offset = offset;
        this.size = limit;
    }

    AzureBlobReader(
            BlobAsyncClient blobClient, IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath) {

        this(blobClient, dataContext, errors, storageKey, storagePath, 0, 0, DEFAULT_CHUNK_SIZE);
    }

    @Override
    protected void clientStart() {

        fluxSubscriber = new FluxSubscriber();

        var range = size > 0
                ? new BlobRange(offset, size)
                : new BlobRange(offset);

        var options = new DownloadRetryOptions()
                .setMaxRetryRequests(DEFAULT_RETRIES);

        var conditions = new BlobRequestConditions();  // no special conditions

        var eventLoop = AzureScheduling.schedulerFor(dataContext.eventLoopExecutor());

        blobClient.downloadStreamWithResponse(range, options, conditions, NO_MD5_CHECK)
                .publishOn(eventLoop)
                .subscribe(this::onDownload, fluxSubscriber::onError);
    }

    @Override
    protected void clientRequest(long n) {

        if (subscription != null)
            subscription.request(n);
        else
            requestCache += n;
    }

    @Override
    protected void clientCancel() {

        if (subscription != null) {
            subscription.cancel();
            subscription = null;
        }
    }

    private void onDownload(BlobDownloadAsyncResponse asyncDownload) {

        var eventLoop = AzureScheduling.schedulerFor(dataContext.eventLoopExecutor());

        asyncDownload.getValue()
                .publishOn(eventLoop)
                .subscribe(fluxSubscriber);
    }

    private class FluxSubscriber implements Subscriber<ByteBuffer> {

        @Override
        public void onSubscribe(Subscription subscription) {

            if (isDone())
                return;

            AzureBlobReader.this.subscription = subscription;

            if (requestCache > 0) {
                subscription.request(requestCache);
                requestCache = 0;
            }
        }

        @Override
        public void onNext(ByteBuffer buffer) {
            AzureBlobReader.this.onChunk(buffer);
        }

        @Override
        public void onError(Throwable error) {
            AzureBlobReader.this.onError(error);
        }

        @Override
        public void onComplete() {
            AzureBlobReader.this.onComplete();
        }
    }
}
