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
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.common.util.Futures;
import com.accenture.trac.metadata.*;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DataWriteService {

    private static final String TRAC_FILE_NAME_ATTR = "trac_file_name";
    private static final String TRAC_FILE_EXTENSION_ATTR = "trac_file_extension";
    private static final String TRAC_FILE_MIME_TYPE_ATTR = "trac_file_mime_type";
    private static final String TRAC_FILE_SIZE_ATTR = "trac_file_size";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IStorageManager storageManager;
    private final TrustedMetadataApiFutureStub metaApi;

    public DataWriteService(
            IStorageManager storageManager,
            TrustedMetadataApiFutureStub metaApi) {

        this.storageManager = storageManager;
        this.metaApi = metaApi;
    }

    public CompletionStage<TagHeader> createFile(
            String tenant, List<TagUpdate> tags,
            String name, String mimeType,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        log.info("In service method...");

        // TODO: Validation

        var defs = new RequestState();
        defs.tags = tags;

        var preallocateRequest = MetadataWriteRequest.newBuilder()
            .setTenant(tenant)
            .setObjectType(ObjectType.FILE)
            .build();

        return CompletableFuture.completedFuture(null)

                // Call meta svc to preallocate file object ID
                .thenApply(x -> metaApi.preallocateId(preallocateRequest))
                .thenCompose(Futures::javaFuture)
                .thenAccept(fileId -> defs.fileId = fileId)

                // Build initial definition objects
                .thenApply(x -> buildDefinitions(defs.fileId, name, mimeType))
                .thenAccept(defs_ -> {
                    defs.file = defs_.file;
                    defs.storage = defs_.storage; })

                // Write file content stream to the storage layer
                .thenCompose(x -> writeDataItem(
                    defs.storage,
                    defs.file.getDataItem(),
                    contentStream, execContext))

                // Record size from storage in file definition
                .thenAccept(size -> defs.file =
                    defs.file.toBuilder()
                    .setSize(size)
                    .build())

                // Save storage metadata
                .thenCompose(x -> createObject(
                        tenant, tags, ObjectType.STORAGE, defs.storage,
                        ObjectDefinition.Builder::setStorage))

                // Record storage ID in file definition
                .thenAccept(storageHeader -> defs.file =
                    defs.file.toBuilder()
                    .setStorageId(MetadataUtil.selectorForLatest(storageHeader))
                    .build())

                // Add file-specific controlled tag attrs
                .thenAccept(x -> defs.tags = addFileAAttrs(defs.tags, defs.file))

                // Save file metadata
                .thenCompose(x -> createPreallocated(
                        tenant, defs.tags, defs.fileId, defs.file,
                        ObjectDefinition.Builder::setFile));
    }

    private List<TagUpdate> addFileAAttrs(List<TagUpdate> tags, FileDefinition fileDef) {

        var nameAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_FILE_NAME_ATTR)
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue(fileDef.getName()))
                .build();

        var extensionAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_FILE_EXTENSION_ATTR)
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue(fileDef.getExtension()))
                .build();

        var mimeTypeAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_FILE_MIME_TYPE_ATTR)
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue(fileDef.getMimeType()))
                .build();

        var sizeAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_FILE_SIZE_ATTR)
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue(fileDef.getSize()))
                .build();

        var fileAttrs = List.of(nameAttr, extensionAttr, mimeTypeAttr, sizeAttr);

        return Stream.concat(tags.stream(), fileAttrs.stream()).collect(Collectors.toList());
    }

    private CompletionStage<Long> writeDataItem(
            StorageDefinitionOrBuilder storageDef, String dataItem,
            Flow.Publisher<ByteBuf> contentStream, IExecutionContext execContext) {

        var storageItem = storageDef.getDataItemsOrThrow(dataItem);
        var incarnation = storageItem.getIncarnations(storageItem.getIncarnationsCount() - 1);  // TODO: Right one
        var copy = incarnation.getCopies(incarnation.getCopiesCount() - 1);  // TODO: Right one

        var storage = storageManager.getFileStorage(copy.getStorageKey());
        var storagePath = copy.getStoragePath();
        var storageDir = storagePath.substring(0, storagePath.lastIndexOf("/"));  // TODO: Can this be cleaner?

        return storage.mkdir(storageDir, true).thenCompose(x -> {

            var signal = new CompletableFuture<Long>();
            var writer = storage.writer(storagePath, signal, execContext);
            contentStream.subscribe(writer);
            return signal;
        });
    }

    private <TDef> CompletionStage<TagHeader> createObject(
            String tenant, List<TagUpdate> tags, ObjectType objectType, TDef def,
            BiFunction<ObjectDefinition.Builder, TDef, ObjectDefinition.Builder> objSetter) {

        var objBuilder = ObjectDefinition.newBuilder().setObjectType(objectType);
        var obj = objSetter.apply(objBuilder, def);

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectType)
                .setDefinition(obj)
                .addAllTagUpdates(tags)
                .build();

        return Futures.javaFuture(metaApi.createObject(request));
    }

    private <TDef> CompletionStage<TagHeader> createPreallocated(
            String tenant, List<TagUpdate> tags, TagHeader objectHeader, TDef def,
            BiFunction<ObjectDefinition.Builder, TDef, ObjectDefinition.Builder> objSetter) {

        var objectType = objectHeader.getObjectType();
        var objBuilder = ObjectDefinition.newBuilder().setObjectType(objectType);
        var obj = objSetter.apply(objBuilder, def);

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectType)
                .setPriorVersion(MetadataUtil.selectorFor(objectHeader))
                .setDefinition(obj)
                .addAllTagUpdates(tags)
                .build();

        return Futures.javaFuture(metaApi.createPreallocatedObject(request));
    }

    private RequestState buildDefinitions(TagHeader fileHeader, String fileName, String mimeType) {

        var FILE_DATA_ITEM_TEMPLATE = "file/%s/version-%d";
        var FILE_STORAGE_PATH_TEMPLATE = "file/%s/version-%d/%s";

        var fileId = UUID.fromString(fileHeader.getObjectId());
        var fileVersion = 1;

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileId, fileVersion);

        var extension = fileName.contains(".")
                ? fileName.substring(fileName.lastIndexOf(".") + 1)
                : "";

        // Size and storage ID omitted, will be set later
        var fileDef = FileDefinition.newBuilder()
                .setName(fileName)
                .setExtension(extension)
                .setMimeType(mimeType)
                .setDataItem(dataItem)
                .build();

        var storagePath = String.format(FILE_STORAGE_PATH_TEMPLATE, fileId, fileVersion, fileName);
        var storageKey = "UNIT_TEST_STORAGE";  // TODO: Where to store
        var storageTimestamp = MetadataCodec
                .encodeDatetime(Instant.now()  // TODO: timestamp
                .atOffset(ZoneOffset.UTC));

        var storageCopy = StorageCopy.newBuilder()
                .setStorageKey(storageKey)
                .setStoragePath(storagePath)
                .setStorageFormat(mimeType)  // TODO: Always use binary blob type for files?
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

        var defSet = new RequestState();
        defSet.file = fileDef;
        defSet.storage = storageDef;

        return defSet;
    }

}
