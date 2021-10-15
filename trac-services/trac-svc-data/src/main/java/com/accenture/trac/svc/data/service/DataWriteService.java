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

    private static final String FILE_DATA_ITEM_TEMPLATE = "file/%s/version-%d";
    private static final String FILE_STORAGE_PATH_TEMPLATE = "file/%s/version-%d/%s";

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

        var defs = new RequestState();
        defs.fileTags = tags;

        return CompletableFuture.completedFuture(null)

                // Call meta svc to preallocate file object ID
                .thenApply(x -> preallocateForType(tenant, ObjectType.FILE))
                .thenApply(metaApi::preallocateId)
                .thenCompose(Futures::javaFuture)
                .thenAccept(fileId -> defs.fileId = fileId)

                // Build definition objects
                .thenAccept(x -> defs.file = createFileDef(defs.fileId, name, mimeType))
                .thenAccept(x -> defs.storage = createStorageDef(defs.fileId, name, mimeType))

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

                // Add controlled tag attrs (must be done after file size is known)
                .thenAccept(x -> defs.fileTags = createFileAttrs(defs.fileTags, defs.file))
                .thenAccept(x -> defs.storageTags = createStorageAttrs(defs.fileTags, defs.file))

                // Save storage metadata
                .thenCompose(x -> createObject(
                        tenant, tags, ObjectType.STORAGE, defs.storage,
                        ObjectDefinition.Builder::setStorage))

                // Record storage ID in file definition
                .thenAccept(storageHeader -> defs.file =
                    defs.file.toBuilder()
                    .setStorageId(MetadataUtil.selectorForLatest(storageHeader))
                    .build())

                // Save file metadata
                .thenCompose(x -> createPreallocated(
                        tenant, defs.fileTags, defs.fileId, defs.file,
                        ObjectDefinition.Builder::setFile));
    }

    public CompletionStage<TagHeader> updateFile(
            String tenant, List<TagUpdate> tags,
            TagSelector priorVersion,
            String name, String mimeType,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        var defs = new RequestState();
        defs.fileTags = tags;

        return CompletableFuture.completedFuture(null)

                // Read prior file metadata
                .thenApply(x -> requestForSelector(tenant, priorVersion))
                .thenApply(metaApi::readObject)
                .thenCompose(Futures::javaFuture)
                .thenAccept(priorFile -> {
                    defs.priorFileId = priorFile.getHeader();
                    defs.priorFile = priorFile.getDefinition().getFile();
                })

                // Read prior storage metadata
                .thenApply(x -> requestForSelector(tenant, defs.priorFile.getStorageId()))
                .thenApply(metaApi::readObject)
                .thenCompose(Futures::javaFuture)
                .thenAccept(priorStorage -> {
                    defs.priorStorageId = priorStorage.getHeader();
                    defs.priorStorage = priorStorage.getDefinition().getStorage();
                })

                // Bump object versions
                .thenAccept(x -> defs.fileId = bumpVersion(defs.priorFileId))
                .thenAccept(x -> defs.storageId = bumpVersion(defs.priorStorageId))

                // Build definition objects
                .thenAccept(x -> defs.file = updateFileDef(defs.priorFile, defs.fileId, name, mimeType))
                .thenAccept(x -> defs.storage = updateStorageDef(defs.priorStorage, defs.fileId, name, mimeType))

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

                // Add controlled tag attrs (must be done after file size is known)
                .thenAccept(x -> defs.fileTags = updateFileAttrs(defs.fileTags, defs.file))
                .thenAccept(x -> defs.storageTags = updateStorageAttrs(defs.fileTags, defs.file))

                // Save storage metadata
                .thenCompose(x -> updateObject(
                        tenant, MetadataUtil.selectorFor(defs.priorStorageId),
                        defs.storage, defs.storageTags, ObjectDefinition.Builder::setStorage))

                // Save file metadata
                .thenCompose(x -> updateObject(
                        tenant, MetadataUtil.selectorFor(defs.priorFileId),
                        defs.file, defs.fileTags, ObjectDefinition.Builder::setFile));

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

    private <TDef> CompletionStage<TagHeader> updateObject(
            String tenant, TagSelector priorVersion, TDef def, List<TagUpdate> tags,
            BiFunction<ObjectDefinition.Builder, TDef, ObjectDefinition.Builder> objSetter) {

        var objBuilder = ObjectDefinition.newBuilder().setObjectType(priorVersion.getObjectType());
        var obj = objSetter.apply(objBuilder, def);

        var request = MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(priorVersion.getObjectType())
                .setPriorVersion(priorVersion)
                .setDefinition(obj)
                .addAllTagUpdates(tags)
                .build();

        return Futures.javaFuture(metaApi.updateObject(request));
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

    private static FileDefinition createFileDef(
            TagHeader fileHeader, String fileName, String mimeType) {

        return buildFileDef(FileDefinition.newBuilder(), fileHeader, fileName, mimeType);
    }

    private static FileDefinition updateFileDef(
            FileDefinition priorFile,
            TagHeader fileHeader, String fileName, String mimeType) {

        return buildFileDef(priorFile.toBuilder(), fileHeader, fileName, mimeType);
    }

    private static FileDefinition buildFileDef(
            FileDefinition.Builder fileDef,
            TagHeader fileHeader, String fileName, String mimeType) {

        var fileId = UUID.fromString(fileHeader.getObjectId());
        var fileVersion = fileHeader.getObjectVersion();

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileId, fileVersion);

        var extension = fileName.contains(".")
                ? fileName.substring(fileName.lastIndexOf(".") + 1)
                : "";

        // Size and storage ID omitted, will be set later
        return fileDef
                .setName(fileName)
                .setExtension(extension)
                .setMimeType(mimeType)
                .setDataItem(dataItem)
                .build();
    }

    private static StorageDefinition createStorageDef(
            TagHeader fileHeader, String fileName, String mimeType) {

        return buildStorageDef(StorageDefinition.newBuilder(), fileHeader, fileName, mimeType);
    }

    private static StorageDefinition updateStorageDef(
            StorageDefinition priorStorage,
            TagHeader fileHeader, String fileName, String mimeType) {

        return buildStorageDef(priorStorage.toBuilder(), fileHeader, fileName, mimeType);
    }

    private static StorageDefinition buildStorageDef(
            StorageDefinition.Builder storageDef,
            TagHeader fileHeader, String fileName, String mimeType) {

        var fileId = UUID.fromString(fileHeader.getObjectId());
        var fileVersion = fileHeader.getObjectVersion();

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileId, fileVersion);
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

        return storageDef
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private static List<TagUpdate> createFileAttrs(List<TagUpdate> tags, FileDefinition fileDef) {

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

    private static List<TagUpdate> updateFileAttrs(List<TagUpdate> tags, FileDefinition fileDef) {

        // Extension and mime type are not allowed to change between file versions

        var nameAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_FILE_NAME_ATTR)
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue(fileDef.getName()))
                .build();

        var sizeAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_FILE_SIZE_ATTR)
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue(fileDef.getSize()))
                .build();

        var fileAttrs = List.of(nameAttr, sizeAttr);

        return Stream.concat(tags.stream(), fileAttrs.stream()).collect(Collectors.toList());
    }

    private static List<TagUpdate> createStorageAttrs(List<TagUpdate> tags, FileDefinition fileDef) {

        return List.of();  // TODO: Storage attrs
    }

    private static List<TagUpdate> updateStorageAttrs(List<TagUpdate> tags, FileDefinition fileDef) {

        return List.of();  // TODO: Storage attrs
    }

    private static MetadataWriteRequest preallocateForType(String tenant, ObjectType objectType) {

        return MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectType)
                .build();
    }

    private static MetadataReadRequest requestForSelector(String tenant, TagSelector selector) {

        return MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build();
    }

    private static TagHeader bumpVersion(TagHeader priorVersion) {

        return priorVersion.toBuilder()
                .setObjectVersion(priorVersion.getObjectVersion() + 1)
                .setTagVersion(1)
                .build();
    }
}
