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
import com.accenture.trac.common.validation.Validator;
import com.accenture.trac.metadata.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.accenture.trac.svc.data.service.MetadataBuilders.*;


public class FileReadWriteService {

    private static final String TRAC_FILE_NAME_ATTR = "trac_file_name";
    private static final String TRAC_FILE_EXTENSION_ATTR = "trac_file_extension";
    private static final String TRAC_FILE_MIME_TYPE_ATTR = "trac_file_mime_type";
    private static final String TRAC_FILE_SIZE_ATTR = "trac_file_size";

    private static final String TRAC_STORAGE_OBJECT_ATTR = "trac_storage_object";

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

    public FileReadWriteService(
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

        var req = new RequestState();
        req.fileTags = tags;           // File tags requested by the client
        req.storageTags = List.of();   // Storage tags is empty to start with

        // This timestamp is set in the storage definition to timestamp storage incarnations/copies
        // It is also used in the physical storage path

        // TODO: Single object timestamp

        // It would be nice to have a single timestamp for each object version,
        // which is used in the object header, inside the definitions where needed and for any physical attributes
        // Currently the metadata service generates its own timestamps, which will always be different
        // One possible solution would be a change in the trusted metadata API,
        // letting other TRAC services specify the object timestamp
        // To avoid polluting the public API, this could go into the gRPC call metadata

        req.objectTimestamp = Instant.now();

        return CompletableFuture.completedFuture(null)

                // Call meta svc to preallocate file object ID
                .thenApply(x -> preallocateRequest(tenant, ObjectType.FILE))
                .thenApply(metaApi::preallocateId)
                .thenCompose(Futures::javaFuture)
                .thenAccept(fileId -> req.priorFileId = fileId)

                // Preallocate ID comes back with version 0, bump to get ID for first real version
                .thenAccept(x -> req.fileId = bumpVersion(req.priorFileId))

                // Build definition objects
                .thenAccept(x -> req.file = createFileDef(req.fileId, name, mimeType))
                .thenAccept(x -> req.storage = createStorageDef(
                        config.getDefaultStorage(),  req.objectTimestamp,
                        req.fileId, name, mimeType, random))

                // Write file content stream to the storage layer
                .thenCompose(x -> writeDataItem(
                        req.storage,
                        req.file.getDataItem(),
                        contentStream, execContext))

                // Check and record size from storage in file definition
                .thenApply(size -> checkSize(size, expectedSize))
                .thenAccept(size -> req.file = recordSize(size, req.file))

                // Add controlled tag attrs (must be done after file size is known)
                .thenAccept(x -> req.fileTags = createFileAttrs(req.fileTags, req.file))
                .thenAccept(x -> req.storageTags = createStorageAttrs(req.storageTags, req.fileId))

                // Save storage metadata
                .thenCompose(x -> MetadataHelpers.createObject(metaApi,
                        tenant, tags, ObjectType.STORAGE, req.storage,
                        ObjectDefinition.Builder::setStorage))

                // Record storage ID in file definition
                .thenAccept(storageId -> req.file = recordStorageId(storageId, req.file))

                // Save file metadata
                .thenCompose(x -> MetadataHelpers.createPreallocated(metaApi,
                        tenant, req.fileTags, req.fileId, req.file,
                        ObjectDefinition.Builder::setFile));
    }

    public CompletionStage<TagHeader> updateFile(
            String tenant, List<TagUpdate> tags,
            TagSelector priorVersion,
            String name, String mimeType, Long expectedSize,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        var req = new RequestState();
        req.fileTags = tags;           // File tags requested by the client
        req.storageTags = List.of();   // Storage tags is empty to start with
        req.objectTimestamp = Instant.now();

        return CompletableFuture.completedFuture(null)

                // Read prior file metadata
                .thenApply(x -> requestForSelector(tenant, priorVersion))
                .thenApply(metaApi::readObject)
                .thenCompose(Futures::javaFuture)
                .thenAccept(priorFile -> {
                        req.priorFileId = priorFile.getHeader();
                        req.priorFile = priorFile.getDefinition().getFile();
                })

                // Read prior storage metadata
                .thenApply(x -> requestForSelector(tenant, req.priorFile.getStorageId()))
                .thenApply(metaApi::readObject)
                .thenCompose(Futures::javaFuture)
                .thenAccept(priorStorage -> {
                        req.priorStorageId = priorStorage.getHeader();
                        req.priorStorage = priorStorage.getDefinition().getStorage();
                })

                // Bump object versions
                .thenAccept(x -> req.fileId = bumpVersion(req.priorFileId))
                .thenAccept(x -> req.storageId = bumpVersion(req.priorStorageId))

                // Build definition objects
                .thenAccept(x -> req.file = updateFileDef(req.priorFile, req.fileId, name, mimeType))
                .thenAccept(x -> req.storage = updateStorageDef(
                        req.priorStorage, config.getDefaultStorage(), req.objectTimestamp,
                        req.fileId, name, mimeType, random))

                .thenAccept(x -> validator.validateVersion(req.file, req.priorFile))

                // Write file content stream to the storage layer
                .thenCompose(x -> writeDataItem(
                        req.storage,
                        req.file.getDataItem(),
                        contentStream, execContext))

                // Check and record size from storage in file definition
                .thenApply(size -> checkSize(size, expectedSize))
                .thenAccept(size -> req.file = recordSize(size, req.file))

                // Add controlled tag attrs (must be done after file size is known)
                .thenAccept(x -> req.fileTags = updateFileAttrs(req.fileTags, req.file))

                // Storage attrs do not require an explicit update

                // Save storage metadata
                .thenCompose(x -> MetadataHelpers.updateObject(metaApi,
                        tenant, MetadataUtil.selectorFor(req.priorStorageId),
                        req.storage, req.storageTags, ObjectDefinition.Builder::setStorage))

                // Save file metadata
                .thenCompose(x -> MetadataHelpers.updateObject(metaApi,
                        tenant, MetadataUtil.selectorFor(req.priorFileId),
                        req.file, req.fileTags, ObjectDefinition.Builder::setFile));

    }

    public void readFile(
            String tenant, TagSelector selector,
            CompletableFuture<FileDefinition> definition,
            Flow.Subscriber<ByteBuf> content,
            IExecutionContext execCtx) {

        var allocator = ByteBufAllocator.DEFAULT;
        var state = new RequestState();

        CompletableFuture.completedFuture(null)

                .thenCompose(x -> MetadataHelpers.readObject(metaApi, tenant, selector))
                .thenAccept(obj -> state.file = obj.getDefinition().getFile())

                .thenCompose(x -> MetadataHelpers.readObject(metaApi, tenant, state.file.getStorageId()))
                .thenAccept(obj -> state.storage = obj.getDefinition().getStorage())

                .thenAccept(x -> definition.complete(state.file))

                .thenApply(x -> readFile(state.file, state.storage, execCtx))
                .thenAccept(byteStream -> byteStream.subscribe(content))

                .exceptionally(error -> Helpers.reportError(error, definition, content));
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

    private static List<TagUpdate> createStorageAttrs(List<TagUpdate> tags, TagHeader objectId) {

        // TODO: Special metadata Value type for handling tag selectors
        var selector = MetadataUtil.selectorForLatest(objectId);
        var storageObjectAttr = String.format("%s:%s", selector.getObjectType(), selector.getObjectId());

        var storageForAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_STORAGE_OBJECT_ATTR)
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue(storageObjectAttr))
                .build();

        var storageAttrs= List.of(storageForAttr);

        return Stream.concat(tags.stream(), storageAttrs.stream()).collect(Collectors.toList());
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
