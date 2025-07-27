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

package org.finos.tracdap.svc.data.service;


import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc.InternalMetadataApiFutureStub;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.async.Futures;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.middleware.GrpcClientConfig;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.exception.EDataSize;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.common.validation.Validator;

import org.apache.arrow.memory.ArrowBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import static org.finos.tracdap.common.metadata.MetadataConstants.*;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorFor;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorForLatest;
import static org.finos.tracdap.svc.data.service.MetadataBuilders.*;


public class FileService {

    private static final String FILE_DATA_ITEM_TEMPLATE = "file/%s/version-%d";
    private static final String FILE_STORAGE_PATH_TEMPLATE = "file/%s/version-%d%s/%s";
    private static final String FILE_STORAGE_PATH_SUFFIX_TEMPLATE = "-x%06x";

    private static final String BACKSLASH = "/";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TenantStorageManager storageManager;
    private final InternalMetadataApiFutureStub metaApi;

    private final Validator validator = new Validator();
    private final Random random = new Random();

    public FileService(
            TenantStorageManager storageManager,
            InternalMetadataApiFutureStub metaApi) {

        this.storageManager = storageManager;
        this.metaApi = metaApi;
    }

    public CompletionStage<TagHeader> createFile(
            FileWriteRequest request,
            RequestMetadata requestMetadata,
            Flow.Publisher<ArrowBuf> contentStream,
            IDataContext dataCtx,
            GrpcClientConfig clientConfig) {

        var initialState = new RequestState();
        initialState.tenant = request.getTenant();
        initialState.requestMetadata = requestMetadata;
        initialState.clientConfig = clientConfig;

        return CompletableFuture.completedFuture(initialState)

                // Call meta svc to preallocate file and storage IDs
                .thenCompose(state -> preallocateIds(request, state))

                // Build new object definitions (file and storage)
                .thenApply(state -> createMetadata(request, state))

                // Write file content stream to the storage layer
                .thenCompose(state -> writeFileContent(request, state, contentStream, dataCtx))

                // Build new tag attrs (must be done after file size is known)
                .thenApply(state -> finalizeMetadata(request, state))

                // Save all metadata
                .thenCompose(state -> saveMetadata(request, state));
    }

    public CompletionStage<TagHeader> updateFile(
            FileWriteRequest request,
            RequestMetadata requestMetadata,
            Flow.Publisher<ArrowBuf> contentStream,
            IDataContext dataCtx,
            GrpcClientConfig clientConfig) {

        var initialState = new RequestState();
        initialState.tenant = request.getTenant();
        initialState.requestMetadata = requestMetadata;
        initialState.clientConfig = clientConfig;

        var priorState = new RequestState();
        priorState.clientConfig = initialState.clientConfig;

        return CompletableFuture.completedFuture(priorState)

                // Load all metadata (file and storage) into prior state
                .thenCompose(prior -> loadMetadata(request.getTenant(), request.getPriorVersion(), prior))

                // Build updated object definitions (file and storage)
                .thenApply(prior -> updateMetadata(request, initialState, prior))

                // Run version validator
                .thenApply(state ->  { validator.validateVersion(state.file, priorState.file); return state; })

                // Write file content stream to the storage layer
                .thenCompose(state -> writeFileContent(request, state, contentStream, dataCtx))

                // Build updated tag attrs (must be done after file size is known)
                .thenApply(state -> finalizeUpdateMetadata(request, state))

                // Save all metadata
                .thenCompose(state -> saveMetadata(request, state, priorState));
    }

    public void readFile(
            FileReadRequest request,
            RequestMetadata requestMetadata,
            CompletableFuture<FileDefinition> definition,
            Flow.Subscriber<ArrowBuf> content,
            IDataContext dataCtx,
            GrpcClientConfig clientConfig) {

        var initialState = new RequestState();
        initialState.tenant = request.getTenant();
        initialState.requestMetadata = requestMetadata;
        initialState.clientConfig = clientConfig;

        CompletableFuture.completedFuture(initialState)

                .thenCompose(state -> loadMetadata(request.getTenant(), request.getSelector(), state))

                .thenApply(state -> { definition.complete(state.file); return state; })

                .thenApply(state -> readFileContent(state, dataCtx))

                .thenAccept(byteStream -> byteStream.subscribe(content))

                .exceptionally(error -> Helpers.reportError(error, definition, content));
    }

    private CompletionStage<RequestState> preallocateIds(FileWriteRequest request, RequestState state) {

        var client = state.clientConfig.configureClient(metaApi);

        var fileIdReq = MetadataBuilders.preallocateRequest(request.getTenant(), ObjectType.FILE);
        var storageIdReq = MetadataBuilders.preallocateRequest(request.getTenant(), ObjectType.STORAGE);

        var batchReq = MetadataWriteBatchRequest.newBuilder()
                .setTenant(request.getTenant())
                .addPreallocateIds(fileIdReq)
                .addPreallocateIds(storageIdReq)
                .build();

        return Futures
                .javaFuture(client.writeBatch(batchReq))
                .thenApply(batchResp -> recordPreallocateIds(batchResp, state));
    }

    private RequestState recordPreallocateIds(MetadataWriteBatchResponse batchResponse, RequestState state) {

        state.preAllocFileId = batchResponse.getPreallocateIds(0);
        state.preAllocStorageId = batchResponse.getPreallocateIds(1);

        return state;
    }

    private CompletionStage<RequestState> loadMetadata(String tenant, TagSelector fileSelector, RequestState state) {

        var client = state.clientConfig.configureClient(metaApi);
        var request = requestForSelector(tenant, fileSelector);

        return Futures.javaFuture(client.readObject(request))
                .thenAccept(tag -> {
                    state.fileId = tag.getHeader();
                    state.file = tag.getDefinition().getFile();
                })
                .thenCompose(x ->loadStorageMetadata(tenant, state));
    }

    private CompletionStage<RequestState> loadStorageMetadata(String tenant, RequestState state) {

        var client = state.clientConfig.configureClient(metaApi);
        var request = requestForSelector(tenant, state.file.getStorageId());

        return Futures.javaFuture(client.readObject(request))
                .thenApply(tag -> {
                    state.storageId = tag.getHeader();
                    state.storage = tag.getDefinition().getStorage();
                    return state;
                });
    }

    private CompletionStage<TagHeader> saveMetadata(FileWriteRequest request, RequestState state) {

        var client = state.clientConfig.configureClient(metaApi);

        var priorFileId = selectorFor(state.preAllocFileId);
        var priorStorageId = selectorFor(state.preAllocStorageId);

        var tenant = request.getTenant();
        var fileReq = buildCreateObjectReq(tenant, priorFileId, state.file, state.fileTags);
        var storageReq = buildCreateObjectReq(tenant, priorStorageId, state.storage, state.storageTags);

        var batchReq = MetadataWriteBatchRequest.newBuilder()
                .setTenant(tenant)
                .addCreatePreallocatedObjects(fileReq)
                .addCreatePreallocatedObjects(storageReq)
                .build();

        return Futures
                .javaFuture(client.writeBatch(batchReq))
                .thenApply(resp -> resp.getCreatePreallocatedObjects(0));
    }

    private CompletionStage<TagHeader> saveMetadata(FileWriteRequest request, RequestState state, RequestState prior) {

        var client = state.clientConfig.configureClient(metaApi);

        var priorFileId = selectorFor(prior.fileId);
        var priorStorageId = selectorFor(prior.storageId);

        var tenant = request.getTenant();
        var fileReq = buildCreateObjectReq(tenant, priorFileId, state.file, state.fileTags);
        var storageReq = buildCreateObjectReq(tenant, priorStorageId, state.storage, state.storageTags);

        var batchReq = MetadataWriteBatchRequest.newBuilder()
                .setTenant(tenant)
                .addUpdateObjects(fileReq)
                .addUpdateObjects(storageReq)
                .build();

        return Futures
                .javaFuture(client.writeBatch(batchReq))
                .thenApply(resp -> resp.getUpdateObjects(0));
    }

    private CompletionStage<RequestState> writeFileContent(
            FileWriteRequest request, RequestState state,
            Flow.Publisher<ArrowBuf> contentStream, IDataContext dataContext) {

        var dataItem = state.file.getDataItem();
        var storageItem = state.storage.getDataItemsOrThrow(dataItem);

        // Select the incarnation to write data for
        // Currently there is only one incarnation being created per storage item

        var incarnationIndex = 0;
        var incarnation = storageItem.getIncarnations(incarnationIndex);

        // Select the copy to write data for
        // Currently there is only one copy being created per incarnation

        var copyIndex = 0;
        var copy = incarnation.getCopies(copyIndex);

        // Get the storage implementation for the storage key where this copy is being saved

        var storage = storageManager
                .getTenantStorage(state.tenant)
                .getFileStorage(copy.getStorageKey());

        // Create the parent directory where the item will be stored

        var storagePath = copy.getStoragePath();
        var storageDir = storagePath.substring(0, storagePath.lastIndexOf(BACKSLASH));

        var mkdir = storage.mkdir(storageDir, true, dataContext);

        // Kick off the file write operation

        var writeFile =  mkdir.thenComposeAsync(x ->
                doWriteFileContent(storage, storagePath, contentStream, dataContext),
                dataContext.eventLoopExecutor());

        // Once the operation completes, check and record the stored data size

        return writeFile.thenApply(size -> recordFileSize(size, request, state));
    }

    private CompletionStage<Long> doWriteFileContent(
            IFileStorage storage, String storagePath,
            Flow.Publisher<ArrowBuf> contentStream, IDataContext dataContext) {

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, dataContext);
        contentStream.subscribe(writer);
        return signal;
    }

    private RequestState recordFileSize(long actualSize, FileWriteRequest request, RequestState state) {

        // If a file size is provided in the write request, the actual size should match
        if (request.hasSize() && actualSize != request.getSize()) {

            var err = String.format(
                    "File size received does not match the size expected: Received %d B, expected %d B",
                    actualSize, request.getSize());

            log.error(err);
            throw new EDataSize(err);
        }

        state.fileSize = actualSize;

        return state;
    }

    private Flow.Publisher<ArrowBuf> readFileContent(
            RequestState state, IDataContext dataContext) {

        var dataItem = state.file.getDataItem();
        var storageItem = state.storage.getDataItemsOrThrow(dataItem);

        var incarnation = storageItem.getIncarnations(0);
        var copy = incarnation.getCopies(0);

        var storageKey = copy.getStorageKey();
        var storagePath = copy.getStoragePath();

        var storage = storageManager
                .getTenantStorage(state.tenant)
                .getFileStorage(storageKey);

        return storage.reader(storagePath, dataContext);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // METADATA BUILDERS
    // -----------------------------------------------------------------------------------------------------------------

    private RequestState createMetadata(FileWriteRequest request, RequestState state) {

        var tenantStorage = storageManager.getTenantStorage(state.tenant);
        var storageKey = tenantStorage.defaultLocation();
        var timestamp = state.requestMetadata.requestTimestamp().toInstant();

        state.fileId = MetadataUtil.nextObjectVersion(state.preAllocFileId, timestamp);
        state.storageId = MetadataUtil.nextObjectVersion(state.preAllocStorageId, timestamp);

        state.file = buildFileDef(
                FileDefinition.newBuilder(),
                state.fileId, selectorForLatest(state.storageId),
                request.getName(), request.getMimeType());

        state.storage = buildStorageDef(
                StorageDefinition.newBuilder(),
                state.fileId, timestamp, storageKey,
                request.getName(), request.getMimeType());

        return state;
    }

    private RequestState updateMetadata(FileWriteRequest request, RequestState state, RequestState prior) {

        var tenantStorage = storageManager.getTenantStorage(state.tenant);
        var storageKey = tenantStorage.defaultLocation();
        var timestamp = state.requestMetadata.requestTimestamp().toInstant();

        state.fileId = MetadataUtil.nextObjectVersion(prior.fileId, timestamp);
        state.storageId = MetadataUtil.nextObjectVersion(prior.storageId, timestamp);

        state.file = buildFileDef(
                prior.file.toBuilder(),
                state.fileId, prior.file.getStorageId(),
                request.getName(), request.getMimeType());

        state.storage = buildStorageDef(
                prior.storage.toBuilder(),
                state.fileId, timestamp, storageKey,
                request.getName(), request.getMimeType());

        return state;
    }

    private FileDefinition buildFileDef(
            FileDefinition.Builder fileDef,
            TagHeader fileId, TagSelector storageId,
            String fileName, String mimeType) {

        var fileUuid = UUID.fromString(fileId.getObjectId());
        var fileVersion = fileId.getObjectVersion();

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileUuid, fileVersion);

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

    private StorageDefinition buildStorageDef(
            StorageDefinition.Builder storageDef,
            TagHeader fileId, Instant storageTimestamp, String storageKey,
            String fileName, String mimeType) {

        var fileUuid = UUID.fromString(fileId.getObjectId());
        var fileVersion = fileId.getObjectVersion();

        var dataItem = String.format(FILE_DATA_ITEM_TEMPLATE, fileUuid, fileVersion);

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

        var storagePath = String.format(FILE_STORAGE_PATH_TEMPLATE, fileUuid, fileVersion, storageSuffix, fileName);

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

    private RequestState finalizeMetadata(FileWriteRequest request, RequestState state) {

        // Record actual size in the file def
        state.file = state.file.toBuilder()
                .setSize(state.fileSize)
                .build();

        // Client can set uncontrolled tags on the file, but not the storage object
        // Controlled tags are added to the storage object
        state.fileTags = request.getTagUpdatesList();
        state.storageTags = controlledStorageAttrs(state.fileId);

        // TODO: Add controlled tags to DATA objects, e.g. trac_data_rows, trac_data_size

        return state;
    }

    private RequestState finalizeUpdateMetadata(FileWriteRequest request, RequestState state) {

        // Record actual size in the file def
        state.file = state.file.toBuilder()
                .setSize(state.fileSize)
                .build();

        // Client can update uncontrolled tags on the file
        // Controlled tags for storage do not change on update
        state.fileTags = request.getTagUpdatesList();
        state.storageTags = List.of();

        return state;
    }

    private static List<TagUpdate> controlledStorageAttrs(TagHeader fileId) {

        // TODO: Special metadata Value type for handling tag selectors
        var selector = MetadataUtil.selectorForLatest(fileId);
        var storageObjectAttr = MetadataUtil.objectKey(selector);

        var storageForAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_STORAGE_OBJECT_ATTR)
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue(storageObjectAttr))
                .build();

        return List.of(storageForAttr);
    }
}
