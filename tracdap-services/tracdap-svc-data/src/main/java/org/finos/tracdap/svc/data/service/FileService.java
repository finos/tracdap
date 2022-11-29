/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.svc.data.service;

import org.apache.arrow.memory.BufferAllocator;
import org.finos.tracdap.api.*;
import org.finos.tracdap.api.TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub;
import org.finos.tracdap.common.auth.GrpcClientAuth;
import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.config.StorageConfig;
import org.finos.tracdap.metadata.*;

import org.finos.tracdap.common.exception.EDataSize;
import org.finos.tracdap.common.grpc.GrpcClientWrap;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.common.validation.Validator;

import io.grpc.MethodDescriptor;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.finos.tracdap.common.metadata.MetadataConstants.*;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorFor;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorForLatest;
import static org.finos.tracdap.svc.data.service.MetadataBuilders.*;


public class FileService {

    private static final String FILE_DATA_ITEM_TEMPLATE = "file/%s/version-%d";
    private static final String FILE_STORAGE_PATH_TEMPLATE = "file/%s/version-%d%s/%s";
    private static final String FILE_STORAGE_PATH_SUFFIX_TEMPLATE = "-x%06x";

    private static final String BACKSLASH = "/";

    private static final MethodDescriptor<MetadataReadRequest, Tag> READ_OBJECT_METHOD = TrustedMetadataApiGrpc.getReadObjectMethod();
    private static final MethodDescriptor<MetadataBatchRequest, MetadataBatchResponse> READ_BATCH_METHOD = TrustedMetadataApiGrpc.getReadBatchMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> PREALLOCATE_ID_METHOD = TrustedMetadataApiGrpc.getPreallocateIdMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_PREALLOCATED_METHOD = TrustedMetadataApiGrpc.getCreatePreallocatedObjectMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_OBJECT_METHOD = TrustedMetadataApiGrpc.getUpdateObjectMethod();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final StorageConfig storageConfig;
    private final BufferAllocator arrowAllocator;
    private final IStorageManager storageManager;
    private final TrustedMetadataApiFutureStub metaApi;

    private final GrpcClientWrap grpcWrap = new GrpcClientWrap(getClass());
    private final Validator validator = new Validator();
    private final Random random = new Random();

    public FileService(
            StorageConfig storageConfig,
            BufferAllocator arrowAllocator,
            IStorageManager storageManager,
            TrustedMetadataApiFutureStub metaApi) {

        this.storageConfig = storageConfig;
        this.arrowAllocator = arrowAllocator;
        this.storageManager = storageManager;
        this.metaApi = metaApi;
    }

    public CompletionStage<TagHeader> createFile(
            String tenant, List<TagUpdate> tags,
            String name, String mimeType, Long expectedSize,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execCtx,
            String authToken) {

        var state = new RequestState();
        state.fileTags = tags;           // File tags requested by the client
        state.storageTags = List.of();   // Storage tags is empty to start with
        state.authToken = authToken;

        var dataCtx = new DataContext(execCtx.eventLoopExecutor(), arrowAllocator);

        // This timestamp is set in the storage definition to timestamp storage incarnations/copies
        // It is also used in the physical storage path

        // TODO: Single object timestamp

        // It would be nice to have a single timestamp for each object version,
        // which is used in the object header, inside the definitions where needed and for any physical attributes
        // Currently the metadata service generates its own timestamps, which will always be different
        // One possible solution would be a change in the trusted metadata API,
        // letting other TRAC services specify the object timestamp
        // To avoid polluting the public API, this could go into the gRPC call metadata

        state.objectTimestamp = Instant.now();

        var client = metaApi.withCallCredentials(new GrpcClientAuth(state.authToken));

        return CompletableFuture.completedFuture(null)

                // Call meta svc to preallocate file object ID
                .thenApply(x -> preallocateRequest(tenant, ObjectType.FILE))
                .thenCompose(req -> grpcWrap.unaryAsync(PREALLOCATE_ID_METHOD, req, client::preallocateId))
                .thenAccept(fileId -> state.preAllocFileId = fileId)

                // Preallocate ID comes back with version 0, bump to get ID for first real version
                .thenAccept(x -> state.fileId = bumpVersion(state.preAllocFileId))

                // Also pre-allocate for storage
                .thenApply(x -> preallocateRequest(tenant, ObjectType.STORAGE))
                .thenCompose(req -> grpcWrap.unaryAsync(PREALLOCATE_ID_METHOD, req, client::preallocateId))
                .thenAccept(storageId -> state.preAllocStorageId = storageId)
                .thenAccept(x -> state.storageId = bumpVersion(state.preAllocStorageId))

                // Build definition objects
                .thenAccept(x -> state.file = createFileDef(state.fileId, name, mimeType, state.storageId))
                .thenAccept(x -> state.storage = createStorageDef(
                        storageConfig.getDefaultBucket(),  state.objectTimestamp,
                        state.fileId, name, mimeType))

                // Write file content stream to the storage layer
                .thenCompose(x -> writeDataItem(
                        state.storage,
                        state.file.getDataItem(),
                        contentStream, dataCtx))

                // Check and record size from storage in file definition
                .thenApply(size -> checkSize(size, expectedSize))
                .thenAccept(size -> state.file = recordSize(size, state.file))

                // Add controlled tag attrs (must be done after file size is known)
                .thenAccept(x -> state.fileTags = createFileAttrs(state.fileTags, state.file))
                .thenAccept(x -> state.storageTags = createStorageAttrs(state.storageTags, state.fileId))

                // Save all metadata
                .thenCompose(x -> saveMetadata(tenant, state));
    }

    public CompletionStage<TagHeader> updateFile(
            String tenant, List<TagUpdate> tags,
            TagSelector priorVersion,
            String name, String mimeType, Long expectedSize,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execCtx,
            String authToken) {

        var state = new RequestState();
        var prior = new RequestState();
        state.fileTags = tags;           // File tags requested by the client
        state.storageTags = List.of();   // Storage tags is empty to start with
        state.objectTimestamp = Instant.now();
        state.authToken = authToken;
        prior.authToken = authToken;

        var dataCtx = new DataContext(execCtx.eventLoopExecutor(), arrowAllocator);

        return CompletableFuture.completedFuture(null)

                // Load all prior metadata (file and storage)
                .thenCompose(x -> loadMetadata(tenant, priorVersion, prior))

                // Bump object versions
                .thenAccept(x -> state.fileId = bumpVersion(prior.fileId))
                .thenAccept(x -> state.storageId = bumpVersion(prior.storageId))

                // Build definition objects
                .thenAccept(x -> state.file = updateFileDef(prior.file, state.fileId, name, mimeType))
                .thenAccept(x -> state.storage = updateStorageDef(
                        prior.storage, storageConfig.getDefaultBucket(), state.objectTimestamp,
                        state.fileId, name, mimeType))

                .thenAccept(x -> validator.validateVersion(state.file, prior.file))

                // Write file content stream to the storage layer
                .thenCompose(x -> writeDataItem(
                        state.storage,
                        state.file.getDataItem(),
                        contentStream, dataCtx))

                // Check and record size from storage in file definition
                .thenApply(size -> checkSize(size, expectedSize))
                .thenAccept(size -> state.file = recordSize(size, state.file))

                // Add controlled tag attrs (must be done after file size is known)
                .thenAccept(x -> state.fileTags = updateFileAttrs(state.fileTags, state.file))

                // Storage attrs do not require an explicit update

                // Save all metadata
                .thenCompose(x -> saveMetadata(tenant, state, prior));
    }

    public void readFile(
            String tenant, TagSelector selector,
            CompletableFuture<FileDefinition> definition,
            Flow.Subscriber<ByteBuf> content,
            IExecutionContext execCtx,
            String authToken) {

        var state = new RequestState();
        state.authToken = authToken;

        var dataCtx = new DataContext(execCtx.eventLoopExecutor(), arrowAllocator);

        CompletableFuture.completedFuture(null)

                .thenCompose(x -> loadMetadata(tenant, selector, state))

                .thenAccept(x -> definition.complete(state.file))

                .thenApply(x -> readFile(state.file, state.storage, dataCtx))
                .thenAccept(byteStream -> byteStream.subscribe(content))

                .exceptionally(error -> Helpers.reportError(error, definition, content));
    }

    private CompletionStage<Void> loadMetadata(String tenant, TagSelector fileSelector, RequestState state) {

        var client = metaApi.withCallCredentials(new GrpcClientAuth(state.authToken));
        var request = requestForSelector(tenant, fileSelector);

        return grpcWrap
                .unaryAsync(READ_OBJECT_METHOD, request, client::readObject)
                .thenAccept(tag -> {
                    state.fileId = tag.getHeader();
                    state.file = tag.getDefinition().getFile();
                })
                .thenCompose(x ->loadStorageMetadata(tenant, state));
    }

    private CompletionStage<Void> loadStorageMetadata(String tenant, RequestState state) {

        var client = metaApi.withCallCredentials(new GrpcClientAuth(state.authToken));
        var request = requestForSelector(tenant, state.file.getStorageId());

        return grpcWrap
                .unaryAsync(READ_OBJECT_METHOD, request, client::readObject)
                .thenAccept(tag -> {

                    state.storageId = tag.getHeader();
                    state.storage = tag.getDefinition().getStorage();
                });
    }

    private CompletionStage<TagHeader> saveMetadata(String tenant, RequestState state) {

        var client = metaApi.withCallCredentials(new GrpcClientAuth(state.authToken));

        var priorStorageId = selectorFor(state.preAllocStorageId);
        var storageReq = buildCreateObjectReq(tenant, priorStorageId, state.storage, state.storageTags);

        var priorFileId = selectorFor(state.preAllocFileId);
        var fileReq = buildCreateObjectReq(tenant, priorFileId, state.file, state.fileTags);

        return grpcWrap
                .unaryAsync(CREATE_PREALLOCATED_METHOD, storageReq, client::createPreallocatedObject)
                .thenCompose(x -> grpcWrap
                .unaryAsync(CREATE_PREALLOCATED_METHOD, fileReq, client::createPreallocatedObject));
    }

    private CompletionStage<TagHeader> saveMetadata(String tenant, RequestState state, RequestState prior) {

        var client = metaApi.withCallCredentials(new GrpcClientAuth(state.authToken));

        var priorStorageId = selectorFor(prior.storageId);
        var storageReq = buildCreateObjectReq(tenant, priorStorageId, state.storage, state.storageTags);

        var priorFileId = selectorFor(prior.fileId);
        var fileReq = buildCreateObjectReq(tenant, priorFileId, state.file, state.fileTags);

        return grpcWrap
                .unaryAsync(UPDATE_OBJECT_METHOD, storageReq, client::updateObject)
                .thenCompose(x -> grpcWrap
                .unaryAsync(UPDATE_OBJECT_METHOD, fileReq, client::updateObject));
    }

    private CompletionStage<Long> writeDataItem(
            StorageDefinitionOrBuilder storageDef, String dataItem,
            Flow.Publisher<ByteBuf> contentStream, IDataContext dataContext) {

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

        var mkdir = storage.mkdir(storageDir, true, dataContext);

        // Finally, kick off the file write operation

        return mkdir.thenComposeAsync(x ->
                doWriteDataItem(storage, storagePath, contentStream, dataContext),
                dataContext.eventLoopExecutor());
    }

    private CompletionStage<Long> doWriteDataItem(
            IFileStorage storage, String storagePath,
            Flow.Publisher<ByteBuf> contentStream, IDataContext dataContext) {

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, dataContext);
        contentStream.subscribe(writer);
        return signal;
    }

    private Flow.Publisher<ByteBuf> readFile(
            FileDefinition fileDef, StorageDefinition storageDef,
            IDataContext dataContext) {

        var dataItem = fileDef.getDataItem();
        var storageItem = storageDef.getDataItemsOrThrow(dataItem);

        var incarnation = storageItem.getIncarnations(0);
        var copy = incarnation.getCopies(0);

        var storageKey = copy.getStorageKey();
        var storagePath = copy.getStoragePath();

        var storage = storageManager.getFileStorage(storageKey);
        return storage.reader(storagePath, dataContext);
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

    private FileDefinition createFileDef(
            TagHeader fileHeader, String fileName, String mimeType, TagHeader storageId) {

        return buildFileDef(FileDefinition.newBuilder(), fileHeader, fileName, mimeType, selectorForLatest(storageId));
    }

    private FileDefinition updateFileDef(
            FileDefinition priorFile,
            TagHeader fileHeader, String fileName, String mimeType) {

        return buildFileDef(priorFile.toBuilder(), fileHeader, fileName, mimeType, priorFile.getStorageId());
    }

    private FileDefinition buildFileDef(
            FileDefinition.Builder fileDef,
            TagHeader fileHeader, String fileName, String mimeType,
            TagSelector storageId) {

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
                .setStorageId(storageId)
                .setDataItem(dataItem)
                .build();
    }

    private StorageDefinition createStorageDef(
            String storageKey, Instant storageTimestamp,
            TagHeader fileHeader, String fileName, String mimeType) {

        return buildStorageDef(
                StorageDefinition.newBuilder(), storageKey, storageTimestamp,
                fileHeader, fileName, mimeType);
    }

    private StorageDefinition updateStorageDef(
            StorageDefinition priorStorage, String storageKey, Instant storageTimestamp,
            TagHeader fileHeader, String fileName, String mimeType) {

        return buildStorageDef(
                priorStorage.toBuilder(), storageKey, storageTimestamp,
                fileHeader, fileName, mimeType);
    }

    private StorageDefinition buildStorageDef(
            StorageDefinition.Builder storageDef, String storageKey, Instant storageTimestamp,
            TagHeader fileHeader, String fileName, String mimeType) {

        var fileId = UUID.fromString(fileHeader.getObjectId());
        var fileVersion = fileHeader.getObjectVersion();

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileId, fileVersion);

        // We are going to add this data item to the storage definition
        // If the item already exists in storage, then the file object must have been superseded
        // If we can spot the update already, there is no need to continue with the write operation

        if (storageDef.containsDataItems(dataItem)) {

            var err = String.format("File version [%d] has been superseded", fileVersion - 1);

            log.error(err);
            log.error("(updates are present in the storage definition)");

            throw new EMetadataDuplicate(err);
        }

        // It is possible to call updateFile twice on the same prior version at the same time,
        // In this case, the check above passes and both calls try to create the same file at the same time
        // It is also possible that a failed call leaves behind a file without creating a valid metadata record
        // In this case, it would not be possible for an update to succeed as an orphan file is there
        // Best efforts checks can be made but will never cover all possible concurrent code paths

        // To get around this problem, we add some random hex digits into the storage path
        // Update collisions are still resolved when the final metadata record is created
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

    private List<TagUpdate> createFileAttrs(List<TagUpdate> tags, FileDefinition fileDef) {

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

    private List<TagUpdate> updateFileAttrs(List<TagUpdate> tags, FileDefinition fileDef) {

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

    private List<TagUpdate> createStorageAttrs(List<TagUpdate> tags, TagHeader objectId) {

        // TODO: Special metadata Value type for handling tag selectors
        var selector = selectorForLatest(objectId);
        var storageObjectAttr = MetadataUtil.objectKey(selector);

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
}
