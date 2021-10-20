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
import com.accenture.trac.api.config.DataServiceConfig;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.exception.EDataSize;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.common.storage.IFileStorage;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.common.concurrent.Futures;
import com.accenture.trac.metadata.*;

import com.accenture.trac.svc.data.validation.Validator;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Random;
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
    private static final String FILE_STORAGE_PATH_TEMPLATE = "file/%s/version-%d%s/%s";
    private static final String FILE_STORAGE_PATH_SUFFIX_TEMPLATE = "-x%06x";

    private static final String BACKSLASH = "/";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataServiceConfig config;
    private final IStorageManager storageManager;
    private final TrustedMetadataApiFutureStub metaApi;
    private final Validator validator = new Validator();
    private final Random random = new Random();

    public DataWriteService(
            DataServiceConfig config,
            IStorageManager storageManager,
            TrustedMetadataApiFutureStub metaApi) {

        this.config = config;
        this.storageManager = storageManager;
        this.metaApi = metaApi;
    }

    public CompletionStage<TagHeader> createFile(
            String tenant, List<TagUpdate> tags,
            String name, String mimeType, Long expectedSize,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        var defs = new RequestState();
        defs.fileTags = tags;

        // This timestamp is set in the storage definition to timestamp storage incarnations/copies
        // It is also used in the physical storage path

        // TODO: Single object timestamp

        // It would be nice to have a single timestamp for each object version,
        // which is used in the object header, inside the definitions where needed and for any physical attributes
        // Currently the metadata service generates its own timestamps, which will always be different
        // One possible solution would be a change in the trusted metadata API,
        // letting other TRAC services specify the object timestamp
        // To avoid polluting the public API, this could go into the gRPC call metadata

        defs.objectTimestamp = Instant.now();

        return CompletableFuture.completedFuture(null)

                // Call meta svc to preallocate file object ID
                .thenApply(x -> preallocateForType(tenant, ObjectType.FILE))
                .thenApply(metaApi::preallocateId)
                .thenCompose(Futures::javaFuture)
                .thenAccept(fileId -> defs.priorFileId = fileId)

                // Preallocate ID comes back with version 0, bump to get ID for first real version
                .thenAccept(x -> defs.fileId = bumpVersion(defs.priorFileId))

                // Build definition objects
                .thenAccept(x -> defs.file = createFileDef(defs.fileId, name, mimeType))
                .thenAccept(x -> defs.storage = createStorageDef(
                        config.getDefaultStorage(),  defs.objectTimestamp,
                        defs.fileId, name, mimeType, random))

                // Write file content stream to the storage layer
                .thenCompose(x -> writeDataItem(
                        defs.storage,
                        defs.file.getDataItem(),
                        contentStream, execContext))

                // Check and record size from storage in file definition
                .thenApply(size -> checkSize(size, expectedSize))
                .thenAccept(size -> defs.file = recordSize(size, defs.file))

                // Add controlled tag attrs (must be done after file size is known)
                .thenAccept(x -> defs.fileTags = createFileAttrs(defs.fileTags, defs.file))
                .thenAccept(x -> defs.storageTags = createStorageAttrs(defs.fileTags, defs.file))

                // Save storage metadata
                .thenCompose(x -> createObject(
                        tenant, tags, ObjectType.STORAGE, defs.storage,
                        ObjectDefinition.Builder::setStorage))

                // Record storage ID in file definition
                .thenAccept(storageId -> defs.file = recordStorageId(storageId, defs.file))

                // Save file metadata
                .thenCompose(x -> createPreallocated(
                        tenant, defs.fileTags, defs.fileId, defs.file,
                        ObjectDefinition.Builder::setFile));
    }

    public CompletionStage<TagHeader> updateFile(
            String tenant, List<TagUpdate> tags,
            TagSelector priorVersion,
            String name, String mimeType, Long expectedSize,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        var defs = new RequestState();
        defs.fileTags = tags;
        defs.objectTimestamp = Instant.now();

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
                .thenAccept(x -> defs.storage = updateStorageDef(
                        defs.priorStorage, config.getDefaultStorage(), defs.objectTimestamp,
                        defs.fileId, name, mimeType, random))

                .thenAccept(x -> validator.validateVersion(defs.file, defs.priorFile))

                // Write file content stream to the storage layer
                .thenCompose(x -> writeDataItem(
                        defs.storage,
                        defs.file.getDataItem(),
                        contentStream, execContext))

                // Check and record size from storage in file definition
                .thenApply(size -> checkSize(size, expectedSize))
                .thenAccept(size -> defs.file = recordSize(size, defs.file))

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

        // Select the incarnation to write data for
        // Currently there is only one incarnation being created per storage item

        var incarnationIndex = 0;
        var incarnation = storageItem.getIncarnations(incarnationIndex);

        // Select the copy to write data for
        // Currently there is only one copy being created per incarnation

        var copyIndex = 0;
        var copy = incarnation.getCopies(copyIndex);

        // Get the storage implementation for the storage key where this copy is being saved

        var storage = storageManager.getFileStorage(copy.getStorageKey());

        // Create the parent directory where the item will be stored

        var storagePath = copy.getStoragePath();
        var storageDir = storagePath.substring(0, storagePath.lastIndexOf(BACKSLASH));

        var mkdir = storage.mkdir(storageDir, true, execContext);

        // Finally, kick off the file write operation

        return mkdir.thenComposeAsync(x ->
                doWriteDataItem(storage, storagePath, contentStream, execContext),
                execContext.eventLoopExecutor());
    }

    private CompletionStage<Long> doWriteDataItem(
            IFileStorage storage, String storagePath,
            Flow.Publisher<ByteBuf> contentStream, IExecutionContext execContext) {

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, execContext);
        contentStream.subscribe(writer);
        return signal;
    }

    private long checkSize(long actualSize, Long expectedSize) {

        // Size cannot be checked if no expected size is provided
        if (expectedSize == null)
            return actualSize;

        if (actualSize != expectedSize) {

            var err = String.format(
                    "File size received does not match the size expected: Received %d B, expected %d B",
                    actualSize, expectedSize);

            log.error(err);
            throw new EDataSize(err);
        }

        return actualSize;
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


    // -----------------------------------------------------------------------------------------------------------------
    // METADATA BUILDERS
    // -----------------------------------------------------------------------------------------------------------------

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
            String storageKey, Instant storageTimestamp,
            TagHeader fileHeader, String fileName, String mimeType, Random random) {

        return buildStorageDef(
                StorageDefinition.newBuilder(), storageKey, storageTimestamp,
                fileHeader, fileName, mimeType, random);
    }

    private static StorageDefinition updateStorageDef(
            StorageDefinition priorStorage, String storageKey, Instant storageTimestamp,
            TagHeader fileHeader, String fileName, String mimeType, Random random) {

        return buildStorageDef(
                priorStorage.toBuilder(), storageKey, storageTimestamp,
                fileHeader, fileName, mimeType, random);
    }

    private static StorageDefinition buildStorageDef(
            StorageDefinition.Builder storageDef, String storageKey, Instant storageTimestamp,
            TagHeader fileHeader, String fileName, String mimeType, Random random) {

        var fileId = UUID.fromString(fileHeader.getObjectId());
        var fileVersion = fileHeader.getObjectVersion();

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileId, fileVersion);

        // It is possible to call updateFile twice on the same prior version at the same time
        // In this case both calls will try to create the same file at the same time
        // It is also possible that a failed call leaves behind a file without creating a valid metadata record
        // In this case, it would not be possible for an update to succeed one the orphans file is there
        // Best efforts checks can be made but will never cover all possible concurrent code paths

        // To get around this problem, we add some random hex digits into the storage path
        // Update collisions are still resolved when the metadata record is created
        // In this case, the request error handler *should* clean up the file
        // For orphaned files, there is still a chance the random bytes collide
        // But this can be resolved by retrying

        var storageSuffixBytes = random.nextInt(1 << 24);
        var storageSuffix = String.format(FILE_STORAGE_PATH_SUFFIX_TEMPLATE, storageSuffixBytes);

        var storagePath = String.format(FILE_STORAGE_PATH_TEMPLATE, fileId, fileVersion, storageSuffix, fileName);

        var storageEncodedTimestamp = MetadataCodec.encodeDatetime(storageTimestamp);

        // For FILE objects, storage format is taken as the supplied mime type of the file

        var storageCopy = StorageCopy.newBuilder()
                .setStorageKey(storageKey)
                .setStoragePath(storagePath)
                .setStorageFormat(mimeType)
                .setCopyStatus(CopyStatus.COPY_AVAILABLE)
                .setCopyTimestamp(storageEncodedTimestamp);

        var storageIncarnation = StorageIncarnation.newBuilder()
                .addCopies(storageCopy)
                .setIncarnationIndex(0)
                .setIncarnationTimestamp(storageEncodedTimestamp)
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

    private FileDefinition recordSize(long actualSize, FileDefinition fileDef) {

        return fileDef.toBuilder()
                .setSize(actualSize)
                .build();
    }

    private static FileDefinition recordStorageId(TagHeader storageId, FileDefinition fileDef) {

        var storageSelector = MetadataUtil.selectorForLatest(storageId);

        return fileDef.toBuilder()
                .setStorageId(storageSelector)
                .build();
    }
}
