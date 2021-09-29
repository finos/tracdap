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

import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.api.TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub;
import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.common.util.Futures;
import com.accenture.trac.metadata.*;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public class DataWriteService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IStorageManager storageManager;
    private final TrustedMetadataApiFutureStub metaApi;

    public DataWriteService(
            IStorageManager storageManager,
            TrustedMetadataApiFutureStub metaApi) {

        this.storageManager = storageManager;
        this.metaApi = metaApi;
    }

    public CompletionStage<Long> createFile(
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        log.info("In service method...");

        var defs = new DefSet(null, null);

        var preallocateRequest = MetadataWriteRequest.newBuilder()
            .setObjectType(ObjectType.FILE)
            .build();

        CompletableFuture.completedFuture(null)
                .thenApply(x -> metaApi.preallocateId(preallocateRequest))
                .thenCompose(Futures::javaFuture)
                .thenApply(x -> buildDefinitions())
                .thenAccept(defs_ -> {defs.file = defs_.file; defs.storage = defs_.storage;})
                .thenCompose(x -> writeDataItem(defs.storage, defs.file.getDataItem(), contentStream, execContext));

        return CompletableFuture.failedFuture(new Exception("Not implemented yet"));
    }

    private CompletionStage<Long> writeDataItem(
            StorageDefinitionOrBuilder storageDef, String dataItem,
            Flow.Publisher<ByteBuf> contentStream, IExecutionContext execContext) {

        var storageItem = storageDef.getDataItemsOrThrow(dataItem);
        var incarnation = storageItem.getIncarnations(storageItem.getIncarnationsCount() - 1);  // TODO: Right one
        var copy = incarnation.getCopies(incarnation.getCopiesCount() - 1);  // TODO: Right one

        var storage = storageManager.getFileStorage(copy.getStorageKey());
        var storagePath = copy.getStoragePath();

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, execContext);
        contentStream.subscribe(writer);

        return signal;
    }


    private DefSet buildDefinitions() {

        var FILE_DATA_ITEM_TEMPLATE = "file/%s/version-%d";
        var FILE_STORAGE_PATH_TEMPLATE = "file/%s/version-%d/%s";

        var storageId = UUID.randomUUID();
        var fileId = UUID.randomUUID();
        var fileVersion = 1;

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileId, fileVersion);

        var fileName = "TODO_FILE_NAME.ext";
        var extension = fileName.contains(".")
                ? fileName.substring(fileName.lastIndexOf(".") + 1)
                : "";
        var fileType = "";

        var fileDef = FileDefinition.newBuilder()
                .setName(fileName)
                .setExtension(extension)
                .setMimeType(fileType)
                .setSize(0)   // TODO: Size
                .setStorageId(TagSelector.newBuilder()
                    .setObjectType(ObjectType.STORAGE)
                    .setObjectId(storageId.toString())
                    .setLatestObject(true)
                    .setLatestTag(true))
                .setDataItem(dataItem)
                .build();

        var storagePath = String.format(FILE_STORAGE_PATH_TEMPLATE, fileId, fileVersion, fileName);
        var storageKey = "DUMMY_STORAGE";  // TODO: Where to store
        var storageTimestamp = MetadataCodec
                .encodeDatetime(Instant.now()  // TODO: timestamp
                .atOffset(ZoneOffset.UTC));

        var storageCopy = StorageCopy.newBuilder()
                .setStorageKey(storageKey)
                .setStoragePath(storagePath)
                .setStorageFormat(fileType)  // TODO: Always use binary blob type for files?
                .setCopyStatus(CopyStatus.COPY_AVAILABLE)
                .setCopyTimestamp(storageTimestamp);

        var storageIncarnation = StorageIncarnation.newBuilder()
                .addCopies(storageCopy)
                .setIncarnationIndex(0)
                .setIncarnationTimestamp(storageTimestamp)
                .setIncarnationStatus(IncarnationStatus.INCARNATION_AVAILABLE);

        var storageItem = StorageItem.newBuilder()
                .addIncarnations(storageIncarnation)
                .build();

        var storageDef = StorageDefinition.newBuilder()
                .putDataItems(dataItem, storageItem).build();

        return new DefSet(fileDef, storageDef);
    }

    private static class DefSet {

        DefSet(FileDefinition file, StorageDefinition storage) {
            this.file = file;
            this.storage = storage;
        }

        FileDefinition file;
        StorageDefinition storage;
    }


}
