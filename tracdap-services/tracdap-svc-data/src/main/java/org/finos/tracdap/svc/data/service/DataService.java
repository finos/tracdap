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
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.common.data.pipeline.CounterStage;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.async.Futures;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.codec.ICodec;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.middleware.GrpcClientConfig;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.PartKeys;
import org.finos.tracdap.common.validation.Validator;

import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import static org.finos.tracdap.common.metadata.MetadataConstants.TRAC_STORAGE_OBJECT_ATTR;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorFor;
import static org.finos.tracdap.common.metadata.MetadataUtil.selectorForLatest;


public class DataService {

    private static final String DATA_ITEM_TEMPLATE = "data/%s/%s/%s/snap-%d/delta-%d";
    private static final String STORAGE_PATH_TEMPLATE = "data/%s/%s/%s/snap-%d/delta-%d-x%06x";

    // TODO: Storage layout plugins to replace specialized logic
    private static final String STRUCT_STORAGE_FORMAT = "application/json";
    private static final String STRUCT_STORAGE_EXTENSION = "json";
    private static final String STRICT_STORAGE_PATH_TEMPLATE = "data/%s/%s/%s/snap-%d/delta-%d-x%06x.%s";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TenantStorageManager storageManager;
    private final ICodecManager codecManager;
    private final InternalMetadataApiGrpc.InternalMetadataApiFutureStub metaClient;

    private final Validator validator = new Validator();
    private final Random random = new Random();

    public DataService(
            TenantStorageManager storageManager,
            ICodecManager codecManager,
            InternalMetadataApiGrpc.InternalMetadataApiFutureStub metaClient) {

        this.storageManager = storageManager;
        this.codecManager = codecManager;
        this.metaClient = metaClient;
    }

    public CompletionStage<TagHeader> createDataset(
            DataWriteRequest request,
            Flow.Publisher<ArrowBuf> contentStream,
            IDataContext dataCtx,
            RequestMetadata requestMetadata,
            GrpcClientConfig clientConfig) {

        var initialState = new RequestState();
        initialState.tenant = request.getTenant();
        initialState.requestMetadata = requestMetadata;
        initialState.clientConfig = clientConfig;

        // Look up the requested data codec
        // If the codec is unknown the request will fail right away
        var codec = codecManager.getCodec(request.getFormat());
        var codecOptions = Map.<String, String>of();

        return CompletableFuture.completedFuture(initialState)

                // Resolve a concrete schema to use for this save operation
                // This may fail if it refers to missing or incompatible external objects
                .thenCompose(state -> resolveSchema(request, state))

                // Preallocate IDs for the new objects
                // (this should always succeed, so long as the metadata service is up)
                .thenCompose(state -> preallocateIds(request, state))

                // Build metadata objects for the dataset that will be saved
                .thenApply(state -> buildMetadata(request, state))

                // Decode the data content stream and write it to the storage layer
                // This is where the main data processing streams are executed
                // When this future completes, the data processing stream has completed (or failed)
                .thenCompose(state -> decodeAndSave(
                        state, contentStream,
                        codec, codecOptions, dataCtx))

                // Update metadata objects with results from data processing
                // (currently just size, but could also include other basic stats)
                // Metadata tags are also built here
                .thenApply(state -> finalizeMetadata(request, state))

                // Save metadata to the metadata store
                // This effectively "commits" the dataset by making it visible
                .thenCompose(state -> saveMetadata(request, state));
    }

    public CompletionStage<TagHeader> updateDataset(
            DataWriteRequest request,
            Flow.Publisher<ArrowBuf> contentStream,
            IDataContext dataCtx,
            RequestMetadata requestMetadata,
            GrpcClientConfig clientConfig) {

        var initialState = new RequestState();
        initialState.tenant = request.getTenant();
        initialState.requestMetadata = requestMetadata;
        initialState.clientConfig = clientConfig;

        var priorState = new RequestState();
        priorState.clientConfig = clientConfig;

        // Look up the requested data codec
        // If the codec is unknown the request will fail right away
        var codec = codecManager.getCodec(request.getFormat());
        var codecOptions = Map.<String, String>of();

        return CompletableFuture.completedFuture(priorState)

                // Load metadata for the prior version (DATA, STORAGE, SCHEMA if external)
                .thenCompose(prior -> loadMetadata(request.getTenant(), request.getPriorVersion(), prior))

                .thenCompose(prior -> resolveSchema(request, initialState, prior))

                // Build metadata objects for the dataset that will be saved
                .thenApply(state -> buildUpdateMetadata(request, state, priorState))

                // Validate schema version update
                // No need to validate data version update here, since data RW service is creating it!
                // Metadata service will validate the update though
                .thenApply(state -> { validator.validateVersion(state.data, priorState.data); return state; })

                // Decode the data content stream and write it to the storage layer
                // This is where the main data processing streams are executed
                // When this future completes, the data processing stream has completed (or failed)
                .thenCompose(state -> decodeAndSave(
                        state, contentStream,
                        codec, codecOptions,
                        dataCtx))

                // Update metadata objects with results from data processing
                // (currently just size, but could also include other basic stats)
                // Metadata tags are also built here
                .thenApply(state -> finalizeMetadata(request, state))

                // Save metadata to the metadata store
                // This effectively "commits" the dataset by making it visible
                .thenCompose(state -> saveMetadata(request, state, priorState));

    }

    public void readDataset(
            DataReadRequest request,
            CompletableFuture<SchemaDefinition> schema,
            Flow.Subscriber<ArrowBuf> contentStream,
            IDataContext dataCtx,
            RequestMetadata requestMetadata,
            GrpcClientConfig clientConfig) {

        var state = new RequestState();
        state.tenant = request.getTenant();
        state.requestMetadata = requestMetadata;
        state.clientConfig = clientConfig;

        var codec = codecManager.getCodec(request.getFormat());
        var codecOptions = Map.<String, String>of();

        state.offset = request.getOffset();
        state.limit = request.getLimit();

        CompletableFuture.completedFuture(null)

                // Load metadata for the dataset (DATA, STORAGE, SCHEMA if external)
                .thenCompose(x -> loadMetadata(request.getTenant(), request.getSelector(), state))

                // Select which copy of the data will be read
                .thenAccept(x -> selectCopy(state))

                // Report the resolved schema back to the caller
                // This will be used to construct the first message in the response stream
                .thenAccept(x -> schema.complete(state.schema))

                // Load data from storage and encode it for transmission
                // This is where the main data processing streams are executed
                // When this future completes, the data processing stream has completed (or failed)
                .thenAccept(x -> loadAndEncode(
                        state, contentStream,
                        codec, codecOptions, dataCtx))

                .exceptionally(error -> Helpers.reportError(error, schema, contentStream));
    }

    private CompletionStage<RequestState> preallocateIds(DataWriteRequest request, RequestState state) {

        var client = state.clientConfig.configureClient(metaClient);

        var dataIdReq = MetadataBuilders.preallocateRequest(request.getTenant(), ObjectType.DATA);
        var storageIdReq = MetadataBuilders.preallocateRequest(request.getTenant(), ObjectType.STORAGE);

        var batchReq = MetadataWriteBatchRequest.newBuilder()
                .setTenant(request.getTenant())
                .addPreallocateIds(dataIdReq)
                .addPreallocateIds(storageIdReq)
                .build();

        return Futures
                .javaFuture(client.writeBatch(batchReq))
                .thenApply(batchResp -> recordPreallocateIds(batchResp, state));
    }

    private RequestState recordPreallocateIds(MetadataWriteBatchResponse batchResponse, RequestState state) {

        state.preAllocDataId = batchResponse.getPreallocateIds(0);
        state.preAllocStorageId = batchResponse.getPreallocateIds(1);

        return state;
    }

    private CompletionStage<RequestState> loadMetadata(String tenant, TagSelector dataSelector, RequestState state) {

        var client = state.clientConfig.configureClient(metaClient);
        var request = MetadataBuilders.requestForSelector(tenant, dataSelector);

        return Futures.javaFuture(client.readObject(request))
                .thenAccept(tag -> {
                    state.dataId = tag.getHeader();
                    state.data = tag.getDefinition().getData();
                })
                .thenCompose(x -> state.data.hasSchemaId()
                    ? loadStorageAndExternalSchema(tenant, state)
                    : loadStorageAndEmbeddedSchema(tenant, state));
    }

    private CompletionStage<RequestState> loadStorageAndExternalSchema(String tenant, RequestState state) {

        var client = state.clientConfig.configureClient(metaClient);
        var request = MetadataBuilders.requestForBatch(tenant, state.data.getStorageId(), state.data.getSchemaId());

        return Futures.javaFuture(client.readBatch(request))
                .thenApply(response -> {

                    var storageTag = response.getTag(0);
                    var schemaTag = response.getTag(1);

                    state.storageId = storageTag.getHeader();
                    state.storage = storageTag.getDefinition().getStorage();

                    // Schema comes from the external object
                    state.schema = schemaTag.getDefinition().getSchema();

                    return state;
                });
    }

    private CompletionStage<RequestState> loadStorageAndEmbeddedSchema(String tenant, RequestState state) {

        var client = state.clientConfig.configureClient(metaClient);
        var request = MetadataBuilders.requestForSelector(tenant, state.data.getStorageId());

        return Futures.javaFuture(client.readObject(request))
                .thenApply(tag -> {

                    state.storageId = tag.getHeader();
                    state.storage = tag.getDefinition().getStorage();

                    // A schema is still needed! Take a reference to the embedded schema object
                    state.schema = state.data.getSchema();

                    return state;
                });
    }

    private CompletionStage<RequestState> resolveSchema(DataWriteRequest request, RequestState state) {

        return resolveSchema(request, state, null);
    }

    private CompletionStage<RequestState> resolveSchema(DataWriteRequest request, RequestState state, RequestState prior) {

        var client = state.clientConfig.configureClient(metaClient);

        if (request.hasSchema()) {
            state.schema = request.getSchema();
            return CompletableFuture.completedFuture(state);
        }

        if (request.hasSchemaId()) {

            // If prior version used the same external schema, no need to reload
            if (prior != null && request.getSchemaId().equals(prior.data.getSchemaId())) {
                state.schema = prior.schema;
                return CompletableFuture.completedFuture(state);
            }

            var schemaReq = MetadataBuilders.requestForSelector(request.getTenant(), request.getSchemaId());

            return Futures.javaFuture(client.readObject(schemaReq))
                    .thenApply(tag -> { state.schema = tag.getDefinition().getSchema(); return state; });
        }

        throw new EUnexpected();
    }

    private void selectCopy(RequestState state) {

        var opaqueKey = PartKeys.opaqueKey(PartKeys.ROOT);

        // Snap index is not needed, since DataPartition only holds the current snap

        // Current implementation only supports snap updates, so delta is always zero
        var deltaIndex = 0;

        // Current implementation only creates the first incarnation and copy of each delta
        var incarnationIndex = 0;
        var copyIndex = 0;

        var delta = state.data
                .getPartsOrThrow(opaqueKey)
                .getSnap()
                .getDeltas(deltaIndex);

        var dataItem = delta.getDataItem();

        state.copy = state.storage
                .getDataItemsOrThrow(dataItem)
                .getIncarnations(incarnationIndex)
                .getCopies(copyIndex);
    }

    private CompletionStage<TagHeader> saveMetadata(DataWriteRequest request, RequestState state) {

        var client = state.clientConfig.configureClient(metaClient);

        var priorDataId = selectorFor(state.preAllocDataId);
        var priorStorageId = selectorFor(state.preAllocStorageId);

        var dataReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorDataId, state.data, state.dataTags);
        var storageReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorStorageId, state.storage, state.storageTags);

        var batchReq = MetadataWriteBatchRequest.newBuilder()
                .setTenant(request.getTenant())
                .addCreatePreallocatedObjects(dataReq)
                .addCreatePreallocatedObjects(storageReq)
                .build();

        return Futures
                .javaFuture(client.writeBatch(batchReq))
                .thenApply(batchResp -> batchResp.getCreatePreallocatedObjects(0));
    }

    private CompletionStage<TagHeader> saveMetadata(DataWriteRequest request, RequestState state, RequestState prior) {

        var client = state.clientConfig.configureClient(metaClient);

        var priorDataId = selectorFor(prior.dataId);
        var priorStorageId = selectorFor(prior.storageId);

        var dataReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorDataId, state.data, state.dataTags);
        var storageReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorStorageId, state.storage, state.storageTags);

        var batchReq = MetadataWriteBatchRequest.newBuilder()
                .setTenant(request.getTenant())
                .addUpdateObjects(dataReq)
                .addUpdateObjects(storageReq)
                .build();

        return Futures
                .javaFuture(client.writeBatch(batchReq))
                .thenApply(batchResp -> batchResp.getUpdateObjects(0));
    }

    private RequestState buildMetadata(DataWriteRequest request, RequestState state) {

        state.dataId = MetadataBuilders.bumpVersion(state.preAllocDataId);
        state.storageId = MetadataBuilders.bumpVersion(state.preAllocStorageId);

        state.part = PartKeys.ROOT;
        state.snap = 0;
        state.delta = 0;

        var dataItem = buildDataItem(state);
        var storagePath = buildStoragePath(state);
        var timestamp = state.requestMetadata.requestTimestamp();

        var dataDef = createDataDef(request, state, dataItem);
        var storageDef = createStorageDef(state, dataItem, storagePath, timestamp);

        state.data = dataDef;
        state.storage = storageDef;

        state.copy = storageDef
                .getDataItemsOrThrow(dataItem)
                .getIncarnations(0)
                .getCopies(0);

        return state;
    }

    private RequestState buildUpdateMetadata(DataWriteRequest request, RequestState state, RequestState prior) {

        state.dataId = MetadataBuilders.bumpVersion(prior.dataId);
        state.storageId = MetadataBuilders.bumpVersion(prior.storageId);

        state.part = PartKeys.ROOT;
        state.delta = 0;

        if (prior.data.containsParts(state.part.getOpaqueKey())) {

            var existingPart = prior.data.getPartsOrThrow(state.part.getOpaqueKey());
            var existingSnap = existingPart.getSnap();

            // Only "snap" updates supported atm
            state.snap = existingSnap.getSnapIndex() + 1;
        }
        else {
            state.snap = 0;
        }

        var dataItem = buildDataItem(state);
        var storagePath = buildStoragePath(state);
        var timestamp = state.requestMetadata.requestTimestamp();

        // We are going to add this data item to the storage definition
        // If the item already exists in storage, then the file object must have been superseded
        // If we can spot the update already, there is no need to continue with the write operation

        if (prior.storage.containsDataItems(dataItem)) {

            var err = String.format("File version [%d] has been superseded", prior.dataId.getObjectVersion());

            log.error(err);
            log.error("(updates are present in the storage definition)");

            throw new EMetadataDuplicate(err);
        }

        state.data = updateDataDef(request, state, dataItem, prior.data);
        state.storage = updateStorageDef(state, dataItem, storagePath, timestamp, prior.storage);

        state.copy = state.storage
                .getDataItemsOrThrow(dataItem)
                .getIncarnations(0)
                .getCopies(0);

        return state;
    }

    private String buildDataItem(RequestState state) {

        var dataType = state.schema.getSchemaType().name().toLowerCase();
        var objectId = state.dataId.getObjectId();
        var partKey = state.part.getOpaqueKey();

        return String.format(DATA_ITEM_TEMPLATE,
                dataType, objectId,
                partKey, state.snap, state.delta);
    }

    private String buildStoragePath(RequestState state) {

        var dataType = state.schema.getSchemaType().name().toLowerCase();
        var objectId = state.dataId.getObjectId();
        var partKey = state.part.getOpaqueKey();
        var suffixBytes = random.nextInt(1 << 24);

        if (state.schema.getSchemaType() == SchemaType.STRUCT_SCHEMA) {

            return String.format(STRICT_STORAGE_PATH_TEMPLATE,
                    dataType, objectId,
                    partKey, state.snap, state.delta,
                    suffixBytes, STRUCT_STORAGE_EXTENSION);
        }
        else {

            return String.format(STORAGE_PATH_TEMPLATE,
                    dataType, objectId,
                    partKey, state.snap, state.delta,
                    suffixBytes);
        }
    }

    private DataDefinition createDataDef(DataWriteRequest request, RequestState state, String dataItem) {

        var dataDef = DataDefinition.newBuilder();

        // Schema must be supplied as either ID or full schema (confirmed by validation)
        if (request.hasSchemaId())
            dataDef.setSchemaId(request.getSchemaId());
        else if (request.hasSchema())
            dataDef.setSchema(request.getSchema());
        else
            throw new EUnexpected();

        // A new data def always means one new part/snap/delta

        var delta = DataDelta.newBuilder()
                .setDeltaIndex(state.delta)
                .setDataItem(dataItem);

        var snap = DataSnapshot.newBuilder()
                .setSnapIndex(state.snap)
                .addDeltas(state.delta, delta);

        var part = DataPartition.newBuilder()
                .setPartKey(state.part)
                .setSnap(snap);

        dataDef.putParts(state.part.getOpaqueKey(), part.build());

        // Set the storage ID, always points to the latest object/tag of the storage object

        var storageId = selectorForLatest(state.storageId);
        dataDef.setStorageId(storageId);

        return dataDef.build();
    }

    private DataDefinition updateDataDef(DataWriteRequest request, RequestState state, String dataItem, DataDefinition priorDef) {

        var dataDef = priorDef.toBuilder();

        // Update schema (or schema ID)

        if (request.hasSchemaId())
            dataDef.setSchemaId(request.getSchemaId());
        else if (request.hasSchema())
            dataDef.setSchema(request.getSchema());
        else
            throw new EUnexpected();

        // Add the new part / snap / delta as required

        var opaqueKey = state.part.getOpaqueKey();

        var part = priorDef.containsParts(opaqueKey)
                ? priorDef.getPartsOrThrow(opaqueKey).toBuilder()
                : DataPartition.newBuilder().setPartKey(state.part);

        var snap = part.hasSnap() && part.getSnap().getSnapIndex() == state.snap
                ? part.getSnap().toBuilder()
                : DataSnapshot.newBuilder().setSnapIndex(state.snap);

        var delta = DataDelta.newBuilder()
                .setDeltaIndex(state.delta)
                .setDataItem(dataItem);

        snap.addDeltas(state.delta, delta);
        part.setSnap(snap);
        dataDef.putParts(opaqueKey, part.build());

        // Do not update storage ID, as this selector always refers to the latest object / tag

        return dataDef.build();
    }

    private StorageDefinition createStorageDef(
            RequestState state, String dataItem,
            String storagePath, OffsetDateTime objectTimestamp) {

        var storageItem = buildStorageItem(state, storagePath, objectTimestamp);

        return StorageDefinition.newBuilder()
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private StorageDefinition updateStorageDef(
            RequestState state, String dataItem,
            String storagePath, OffsetDateTime objectTimestamp,
            StorageDefinition priorDef) {

        var storageItem = buildStorageItem(state, storagePath, objectTimestamp);

        return priorDef.toBuilder()
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private StorageItem buildStorageItem(RequestState state, String storagePath, OffsetDateTime objectTimestamp) {

        var tenantStorage = storageManager.getTenantStorage(state.tenant);
        var location = tenantStorage.defaultLocation();

        var format = state.schema.getSchemaType() == SchemaType.STRUCT_SCHEMA
                ? STRUCT_STORAGE_FORMAT
                : tenantStorage.defaultFormat();

        // For the time being, data has one incarnation and a single storage copy

        var incarnationIndex = 0;

        var copy = StorageCopy.newBuilder()
                .setCopyStatus(CopyStatus.COPY_AVAILABLE)
                .setCopyTimestamp(MetadataCodec.encodeDatetime(objectTimestamp))
                .setStorageKey(location)
                .setStoragePath(storagePath)
                .setStorageFormat(format);

        var incarnation = StorageIncarnation.newBuilder()
                .setIncarnationStatus(IncarnationStatus.INCARNATION_AVAILABLE)
                .setIncarnationIndex(incarnationIndex)
                .setIncarnationTimestamp(MetadataCodec.encodeDatetime(objectTimestamp))
                .addCopies(copy);

        return StorageItem.newBuilder()
                .addIncarnations(incarnationIndex, incarnation)
                .build();
    }


    private RequestState finalizeMetadata(DataWriteRequest request, RequestState state) {

        // Update row count for the delta being processed

        var part = state.data.getPartsOrThrow(state.part.getOpaqueKey());
        var delta = part.getSnap().getDeltas(part.getSnap().getDeltasCount() - 1);

        // Delta row count is to allow for updates in deltas which update rather than append a dataset
        // Currently all rows are physical so the two values are the same
        var augmentedDelta = delta.toBuilder()
                .setPhysicalRowCount(state.dataRowCount)
                .setDeltaRowCount(state.dataRowCount)
                .build();

        var augmentedPart = part.toBuilder()
                .setSnap(part.getSnap().toBuilder()
                .setDeltas(part.getSnap().getDeltasCount() - 1, augmentedDelta))
                .build();

        var augmentedData = state.data.toBuilder()
                .putParts(state.part.getOpaqueKey(), augmentedPart);

        // Update top level row count by aggregating across live deltas
        // Superseded snapshots are not included

        long totalRowCount = augmentedData.getPartsMap().values().stream()
                .map(DataPartition::getSnap)
                .map(DataSnapshot::getDeltasList)
                .flatMap(List::stream)
                .mapToLong(DataDelta::getDeltaRowCount)
                .sum();

        state.data = augmentedData
                .setRowCount(totalRowCount)
                .build();

        // Client can set uncontrolled tags on the dataset, but not the storage object
        // Controlled tags are added to the storage object
        state.dataTags = request.getTagUpdatesList();
        state.storageTags = controlledStorageAttrs(state.dataId);

        return state;
    }

    private static List<TagUpdate> controlledStorageAttrs(TagHeader dataId) {

        // TODO: Metadata svc should have a common way to record object references
        var selector = MetadataUtil.selectorForLatest(dataId);
        var storageObjectAttr = MetadataUtil.objectKey(selector);

        var storageForAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_STORAGE_OBJECT_ATTR)
                .setValue(MetadataCodec.encodeValue(storageObjectAttr))
                .build();

        return List.of(storageForAttr);
    }

    private void loadAndEncode(
            RequestState state, Flow.Subscriber<ArrowBuf> contentStream,
            ICodec codec, Map<String, String> codecOptions,
            IDataContext dataCtx) {

        var storage = storageManager
                .getTenantStorage(state.tenant)
                .getDataStorage(state.copy.getStorageKey());

        var pipeline = storage.pipelineReader(state.copy, state.schema, dataCtx, state.offset, state.limit);
        var encoder = codec.getEncoder(dataCtx.arrowAllocator(), codecOptions);

        pipeline.addStage(encoder);
        pipeline.addSink(contentStream);

        pipeline.execute();
    }

    private CompletionStage<RequestState> decodeAndSave(
            RequestState state, Flow.Publisher<ArrowBuf> contentStream,
            ICodec codec, Map<String, String> codecOptions,
            IDataContext dataCtx) {

        var storage = storageManager
                .getTenantStorage(state.tenant)
                .getDataStorage(state.copy.getStorageKey());

        var pipeline = DataPipeline.forSource(contentStream, dataCtx);
        var decoder = codec.getDecoder(state.schema, dataCtx.arrowAllocator(), codecOptions);
        var counter = new CounterStage();
        var signal = new CompletableFuture<Long>();

        pipeline.addStage(decoder);
        pipeline.addStage(counter);
        pipeline = storage.pipelineWriter(state.copy, dataCtx, pipeline, signal);

        pipeline.execute();

        return signal.thenApply(fileSize -> recordSaveResult(fileSize, counter, state));
    }

    private RequestState recordSaveResult(long fileSize, CounterStage counter, RequestState state) {

        state.fileSize = fileSize;
        state.dataRowCount = counter.getRowCount();
        state.dataBatchCount = counter.getBatchCount();

        return state;
    }
}
