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

package com.accenture.trac.svc.data.service;

import com.accenture.trac.api.MetadataReadRequest;
import com.accenture.trac.api.TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub;
import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.common.util.Concurrent;
import com.accenture.trac.common.util.Futures;
import com.accenture.trac.metadata.FileDefinition;
import com.accenture.trac.metadata.StorageDefinition;
import com.accenture.trac.metadata.Tag;
import com.accenture.trac.metadata.TagSelector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Stream;


public class DataReadService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final StorageManager storageManager;
    private final TrustedMetadataApiFutureStub metaApi;

    public DataReadService(
            StorageManager storageManager,
            TrustedMetadataApiFutureStub metaApi) {

        this.storageManager = storageManager;
        this.metaApi = metaApi;
    }

    public void readFile(
            String tenant, TagSelector selector,
            Flow.Subscriber<ByteBuf> dataStream,
            IExecutionContext execCtx) {

        var allocator = ByteBufAllocator.DEFAULT;
        var state = new RequestState();

        CompletableFuture.completedFuture(null)

                .thenCompose(x -> readMetadata(tenant, selector))
                .thenAccept(obj -> state.file = obj.getDefinition().getFile())

                .thenCompose(x -> readMetadata(tenant, state.file.getStorageId()))
                .thenAccept(obj -> state.storage = obj.getDefinition().getStorage())

                .thenApply(x -> readFile(state.file, state.storage, execCtx))
                .thenAccept(byteStream -> byteStream.subscribe(dataStream))

                .exceptionally(error -> {

                    log.error(error.getMessage(), error);

                    dataStream.onSubscribe(new Flow.Subscription() {
                        @Override public void request(long n) {}

                        @Override public void cancel() {}
                    });

                    dataStream.onError(error);

                    return null;
                });
    }

    private CompletionStage<Tag> readMetadata(String tenant, TagSelector selector) {

        var metaRequest = MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build();

        return Futures.javaFuture(metaApi.readObject(metaRequest));
    }

    private Flow.Publisher<ByteBuf> readFile(
            FileDefinition fileDef, StorageDefinition storageDef,
            IExecutionContext execCtx) {

        var dataItem = fileDef.getDataItem();
        var storageItem = storageDef.getDataItemsOrThrow(dataItem);

        var incarnation = storageItem.getIncarnations(0);
        var copy = incarnation.getCopies(0);

        var storageKey = copy.getStorageKey();
        var storagePath = copy.getStoragePath();

        var storage = storageManager.getFileStorage(storageKey);
        return storage.reader(storagePath, execCtx);
    }

}
