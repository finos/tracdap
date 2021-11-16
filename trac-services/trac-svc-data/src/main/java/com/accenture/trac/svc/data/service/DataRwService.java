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

import com.accenture.trac.api.*;
import com.accenture.trac.api.config.DataServiceConfig;
import com.accenture.trac.common.codec.ICodec;
import com.accenture.trac.common.codec.ICodecManager;
import com.accenture.trac.common.concurrent.Futures;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.data.DataContext;
import com.accenture.trac.common.data.IDataContext;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.PartKeys;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.common.validation.Validator;
import com.accenture.trac.metadata.*;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.NettyAllocationManager;
import org.apache.arrow.memory.RootAllocator;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import static com.accenture.trac.common.metadata.MetadataUtil.selectorFor;
import static com.accenture.trac.common.metadata.MetadataUtil.selectorForLatest;
import static com.accenture.trac.svc.data.service.MetadataBuilders.*;

public class DataRwService {

    private static final String DATA_ITEM_TEMPLATE = "data/%s/%s/part-%s/snap-%d/delta-%d-%s";
    private static final String DATA_ITEM_SUFFIX_TEMPLATE = "x%06x";

    private final DataServiceConfig config;
    private final BufferAllocator arrowAllocator;
    private final IStorageManager storageManager;
    private final ICodecManager codecManager;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient;

    private final Validator validator = new Validator();
    private final Random random = new Random();

    public DataRwService(
            DataServiceConfig config,
            BufferAllocator arrowAllocator,
            IStorageManager storageManager,
            ICodecManager codecManager,
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient) {

        this.config = config;
        this.arrowAllocator = arrowAllocator;
        this.storageManager = storageManager;
        this.codecManager = codecManager;
        this.metaClient = metaClient;
    }

    public CompletionStage<TagHeader> createDataset(
            DataWriteRequest request,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execCtx) {

        var dataCtx = new DataContext(execCtx.eventLoopExecutor(), arrowAllocator);
        var state = new RequestState();
        var objectTimestamp = Instant.now().atOffset(ZoneOffset.UTC);

        // Look up the requested data codec
        // If the codec is unknown the request will fail right away
        var codec = codecManager.getCodec(request.getFormat());
        var codecOptions = Map.<String, String>of();

        return CompletableFuture.completedFuture(null)

                // Resolve a concrete schema to use for this save operation
                // This may fail if it refers to missing or incompatible external objects
                .thenCompose(x -> resolveSchema(request, state))

                // Preallocate IDs for the new objects
                // (this should always succeed, so long as the metadata service is up)
                .thenCompose(x -> preallocateIds(request, state))

                // Build metadata objects for the dataset that will be saved
                .thenApply(x -> buildMetadata(request, state, objectTimestamp))

                // Decode the data content stream and write it to the storage layer
                // This is where the main data processing streams are executed
                // When this future completes, the data processing stream has completed (or failed)
                .thenCompose(x -> decodeAndSave(
                        state.schema, contentStream,
                        codec, codecOptions,
                        state.copy, dataCtx))

                // A quick sanity check that the data was written successfully
                .thenApply(rowsSaved -> checkRows(request, rowsSaved))

                // Update metadata objects with results from data processing
                // (currently just size, but could also include other basic stats)
                // Metadata tags are also built here
                .thenAccept(rowsSaved -> finalizeMetadata(state, rowsSaved))

                // Save metadata to the metadata store
                // This effectively "commits" the dataset by making it visible
                .thenCompose(x -> saveMetadata(request, state));
    }

    public CompletionStage<TagHeader> updateDataset(
            DataWriteRequest request,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execCtx) {

        var dataCtx = new DataContext(execCtx.eventLoopExecutor(), arrowAllocator);
        var state = new RequestState();
        var prior = new RequestState();
        var objectTimestamp = Instant.now().atOffset(ZoneOffset.UTC);

        // Look up the requested data codec
        // If the codec is unknown the request will fail right away
        var codec = codecManager.getCodec(request.getFormat());
        var codecOptions = Map.<String, String>of();

        return CompletableFuture.completedFuture(null)

                // Load metadata for the prior version (DATA, STORAGE, SCHEMA if external)
                .thenCompose(x -> loadMetadata(request.getTenant(), request.getPriorVersion(), prior))

                // Resolve a concrete schema to use for this save operation
                // This may fail if it refers to missing or incompatible external objects
                .thenCompose(x -> resolveSchema(request, state))

                // Build metadata objects for the dataset that will be saved
                .thenApply(x -> buildUpdateMetadata(request, state, prior, objectTimestamp))

                // Validate schema version update
                // No need to validate data version update here, since data RW service is creating it!
                // Metadata service will validate the update though
                .thenAccept(x -> validator.validateVersion(state.data, prior.data))

                // Decode the data content stream and write it to the storage layer
                // This is where the main data processing streams are executed
                // When this future completes, the data processing stream has completed (or failed)
                .thenCompose(x -> decodeAndSave(
                        state.schema, contentStream,
                        codec, codecOptions,
                        state.copy, dataCtx))

                // A quick sanity check that the data was written successfully
                .thenApply(rowsSaved -> checkRows(request, rowsSaved))

                // Update metadata objects with results from data processing
                // (currently just size, but could also include other basic stats)
                // Metadata tags are also built here
                .thenAccept(rowsSaved -> finalizeUpdateMetadata(state, rowsSaved))

                // Save metadata to the metadata store
                // This effectively "commits" the dataset by making it visible
                .thenCompose(x -> saveMetadata(request, state, prior));

    }

    public void readDataset(
            DataReadRequest request,
            CompletableFuture<SchemaDefinition> schema,
            Flow.Subscriber<ByteBuf> contentStream,
            IExecutionContext execCtx) {

        var dataCtx = new DataContext(execCtx.eventLoopExecutor(), arrowAllocator);
        var state = new RequestState();

        var codec = codecManager.getCodec(request.getFormat());
        var codecOptions = Map.<String, String>of();

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
                        state.schema, contentStream,
                        codec, codecOptions,
                        state.copy, dataCtx))

                .exceptionally(error -> Helpers.reportError(error, schema, contentStream));
    }

    private CompletionStage<Void> loadMetadata(String tenant, TagSelector dataSelector, RequestState state) {

        var request = requestForSelector(tenant, dataSelector);

        return Futures
                .javaFuture(metaClient.readObject(request))
                .thenAccept(tag -> {
                    state.dataId = tag.getHeader();
                    state.data = tag.getDefinition().getData();
                })
                .thenCompose(x -> state.data.hasSchemaId()
                    ? loadStorageAndExternalSchema(tenant, state)
                    : loadStorageAndEmbeddedSchema(tenant, state));
    }

    private CompletionStage<Void> loadStorageAndExternalSchema(String tenant, RequestState state) {

        var request = requestForBatch(tenant, state.data.getStorageId(), state.data.getSchemaId());

        return Futures
                .javaFuture(metaClient.readBatch(request))
                .thenAccept(response -> {

                    var storageTag = response.getTag(0);
                    var schemaTag = response.getTag(1);

                    state.storageId = storageTag.getHeader();
                    state.storage = storageTag.getDefinition().getStorage();

                    // Schema comes from the external object
                    state.schema = schemaTag.getDefinition().getSchema();
                });
    }

    private CompletionStage<Void> loadStorageAndEmbeddedSchema(String tenant, RequestState state) {

        var request = requestForSelector(tenant, state.data.getStorageId());

        return Futures
                .javaFuture(metaClient.readObject(request))
                .thenAccept(tag -> {

                    state.storageId = tag.getHeader();
                    state.storage = tag.getDefinition().getStorage();

                    // A schema is still needed! Take a reference to the embedded schema object
                    state.schema = state.data.getSchema();
                });
    }

    private void selectCopy(RequestState state) {

        var opaqueKey = PartKeys.opaqueKey(PartKeys.ROOT);
        var snapIndex = 0;
        var deltaIndex = 0;

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

    private CompletionStage<SchemaDefinition> resolveSchema(String tenant, DataDefinition dataDef, RequestState state) {

        if (dataDef.hasSchema()) {
            state.schema = dataDef.getSchema();
            return CompletableFuture.completedFuture(state.schema);
        }

        if (dataDef.hasSchemaId()) {

            var schemaReq = MetadataBuilders.requestForSelector(tenant, dataDef.getSchemaId());
            return Futures
                    .javaFuture(metaClient.readObject(schemaReq))
                    .thenApply(tag -> state.schema = tag.getDefinition().getSchema());
        }

        throw new EUnexpected();
    }

    private CompletionStage<SchemaDefinition> resolveSchema(DataWriteRequest request, RequestState state) {

        if (request.hasSchema()) {
            state.schema = request.getSchema();
            return CompletableFuture.completedFuture(state.schema);
        }

        if (request.hasSchemaId()) {

            var schemaReq = MetadataBuilders.requestForSelector(request.getTenant(), request.getSchemaId());
            return Futures
                    .javaFuture(metaClient.readObject(schemaReq))
                    .thenApply(tag -> state.schema = tag.getDefinition().getSchema());
        }

        throw new EUnexpected();
    }

    private CompletionStage<Void> preallocateIds(DataWriteRequest request, RequestState state) {

        var preAllocDataReq = preallocateRequest(request.getTenant(), ObjectType.DATA);
        var preAllocStorageReq = preallocateRequest(request.getTenant(), ObjectType.STORAGE);

        return CompletableFuture.completedFuture(0)

                .thenCompose(x -> Futures.javaFuture(metaClient.preallocateId(preAllocDataReq)))
                .thenAccept(dataId -> state.preAllocDataId = dataId)

                .thenCompose(x -> Futures.javaFuture(metaClient.preallocateId(preAllocStorageReq)))
                .thenAccept(storageId -> state.preAllocStorageId = storageId);
    }

    private RequestState buildMetadata(DataWriteRequest request, RequestState state, OffsetDateTime objectTimestamp) {

        state.dataTags = request.getTagUpdatesList();  // File tags requested by the client
        state.storageTags = List.of();                 // Storage tags is empty to start with

        state.dataId = bumpVersion(state.preAllocDataId);
        state.storageId = bumpVersion(state.preAllocStorageId);

        state.part = PartKeys.ROOT;
        state.snap = 0;
        state.delta = 0;

        var dataItem = buildDataItem(state);
        var dataDef = createDataDef(request, state, dataItem);
        var storageDef = createStorageDef(request, dataItem, objectTimestamp);

        state.data = dataDef;
        state.storage = storageDef;

        state.copy = storageDef
                .getDataItemsOrThrow(dataItem)
                .getIncarnations(0)
                .getCopies(0);

        return state;
    }

    private RequestState buildUpdateMetadata(
            DataWriteRequest request, RequestState state, RequestState prior,
            OffsetDateTime objectTimestamp) {

        state.dataTags = request.getTagUpdatesList();  // File tags requested by the client
        state.storageTags = List.of();                 // Storage tags is empty to start with

        state.dataId = bumpVersion(prior.dataId);
        state.storageId = bumpVersion(prior.storageId);

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
        state.data = updateDataDef(request, state, dataItem, prior.data);
        state.storage = updateStorageDef(prior.storage, dataItem, objectTimestamp);

        state.copy = state.storage
                .getDataItemsOrThrow(dataItem)
                .getIncarnations(0)
                .getCopies(0);

        return state;
    }

    private String buildDataItem(RequestState state) {

        var suffixBytes = random.nextInt(1 << 24);
        var suffix = String.format(DATA_ITEM_SUFFIX_TEMPLATE, suffixBytes);

        var dataType = state.schema.getSchemaType().name().toLowerCase();
        var objectId = state.dataId.getObjectId();
        var partKey = state.part.getOpaqueKey();

        return String.format(DATA_ITEM_TEMPLATE,
                dataType, objectId,
                partKey, state.snap, state.delta,
                suffix);
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

        var delta = DataDefinition.Delta.newBuilder()
                .setDeltaIndex(state.delta)
                .setDataItem(dataItem);

        var snap = DataDefinition.Snap.newBuilder()
                .setSnapIndex(state.snap)
                .addDeltas(state.delta, delta);

        var part = DataDefinition.Part.newBuilder()
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
                : DataDefinition.Part.newBuilder().setPartKey(state.part);

        var snap = part.hasSnap() && part.getSnap().getSnapIndex() == state.snap
                ? part.getSnap().toBuilder()
                : DataDefinition.Snap.newBuilder().setSnapIndex(state.snap);

        var delta = DataDefinition.Delta.newBuilder()
                .setDeltaIndex(state.delta)
                .setDataItem(dataItem);

        snap.addDeltas(state.delta, delta);
        part.setSnap(snap);
        dataDef.putParts(opaqueKey, part.build());

        // Do not update storage ID, as this selector always refers to the latest object / tag

        return dataDef.build();
    }

    private StorageDefinition createStorageDef(DataWriteRequest request, String dataItem, OffsetDateTime objectTimestamp) {

        var storageItem = buildStorageItem(dataItem, objectTimestamp);

        return StorageDefinition.newBuilder()
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private StorageDefinition updateStorageDef(StorageDefinition priorDef, String dataItem, OffsetDateTime objectTimestamp) {

        var storageItem = buildStorageItem(dataItem, objectTimestamp);

        return priorDef.toBuilder()
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private StorageItem buildStorageItem(String dataItem, OffsetDateTime objectTimestamp) {

        var storageKey = config.getDefaultStorage();
        var storageFormat = "application/vnd.apache.arrow.file";  // TODO: Constants and config

        // For the time being, data has one incarnation and a single storage copy

        var incarnationIndex = 0;

        var copy = StorageCopy.newBuilder()
                .setCopyStatus(CopyStatus.COPY_AVAILABLE)
                .setCopyTimestamp(MetadataCodec.encodeDatetime(objectTimestamp))
                .setStorageKey(storageKey)
                .setStoragePath(dataItem)
                .setStorageFormat(storageFormat);

        var incarnation = StorageIncarnation.newBuilder()
                .setIncarnationStatus(IncarnationStatus.INCARNATION_AVAILABLE)
                .setIncarnationIndex(incarnationIndex)
                .setIncarnationTimestamp(MetadataCodec.encodeDatetime(objectTimestamp))
                .addCopies(copy);

        return StorageItem.newBuilder()
                .addIncarnations(incarnationIndex, incarnation)
                .build();
    }


    private void finalizeMetadata(RequestState state, long rowsSaved) {

    }

    private void finalizeUpdateMetadata(RequestState state, long rowsSaved) {

    }

    private CompletionStage<TagHeader> saveMetadata(DataWriteRequest request, RequestState state) {

        var priorStorageId = selectorFor(state.preAllocStorageId);
        var storageReq = buildCreateObjectReq(request.getTenant(), priorStorageId, state.storage, state.storageTags);

        var priorDataId = selectorFor(state.preAllocDataId);
        var dataReq = buildCreateObjectReq(request.getTenant(), priorDataId, state.data, state.dataTags);

        return CompletableFuture.completedFuture(0)

                .thenApply(x -> storageReq)
                .thenApply(metaClient::createPreallocatedObject)
                .thenCompose(Futures::javaFuture)

                .thenApply(x -> dataReq)
                .thenApply(metaClient::createPreallocatedObject)
                .thenCompose(Futures::javaFuture);
    }

    private CompletionStage<TagHeader> saveMetadata(DataWriteRequest request, RequestState state, RequestState prior) {

        var priorStorageId = selectorFor(prior.storageId);
        var storageReq = buildCreateObjectReq(request.getTenant(), priorStorageId, state.storage, state.storageTags);

        var priorDataId = selectorFor(prior.dataId);
        var dataReq = buildCreateObjectReq(request.getTenant(), priorDataId, state.data, state.dataTags);

        return CompletableFuture.completedFuture(0)

                .thenApply(x -> storageReq)
                .thenApply(metaClient::updateObject)
                .thenCompose(Futures::javaFuture)

                .thenApply(x -> dataReq)
                .thenApply(metaClient::updateObject)
                .thenCompose(Futures::javaFuture);
    }

    private <TDef extends Message> MetadataWriteRequest buildCreateObjectReq(
            String tenant, TagSelector priorVersion,
            TDef definition, List<TagUpdate> tagUpdates) {

        var objectDef = objectOf(definition);

        return MetadataWriteRequest.newBuilder()
                .setTenant(tenant)
                .setObjectType(objectDef.getObjectType())
                .setPriorVersion(priorVersion)
                .setDefinition(objectDef)
                .addAllTagUpdates(tagUpdates)
                .build();
    }

    private ObjectDefinition objectOf(Message def) {

        if (def instanceof DataDefinition)
            return objectOf((DataDefinition) def);

        if (def instanceof FileDefinition)
            return objectOf((FileDefinition) def);

        if (def instanceof SchemaDefinition)
            return objectOf((SchemaDefinition) def);

        if (def instanceof StorageDefinition)
            return objectOf((StorageDefinition) def);

        throw new EUnexpected();
    }

    private ObjectDefinition objectOf(DataDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setData(def)
                .build();
    }

    private ObjectDefinition objectOf(FileDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.FILE)
                .setFile(def)
                .build();
    }

    private ObjectDefinition objectOf(SchemaDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setSchema(def)
                .build();
    }

    private ObjectDefinition objectOf(StorageDefinition def) {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.STORAGE)
                .setStorage(def)
                .build();
    }

    private void loadAndEncode(
            SchemaDefinition schema, Flow.Subscriber<ByteBuf> contentStream,
            ICodec codec, Map<String, String> codecOptions,
            StorageCopy copy,
            IDataContext dataCtx) {

        var encoder = codec.getEncoder(arrowAllocator, schema, codecOptions);
        encoder.subscribe(contentStream);

        var storageKey = copy.getStorageKey();
        var storage = storageManager.getDataStorage(storageKey);
        var reader = storage.reader(schema, copy, dataCtx);
        reader.subscribe(encoder);
    }

    private CompletionStage<Long> decodeAndSave(
            SchemaDefinition schema, Flow.Publisher<ByteBuf> contentStream,
            ICodec codec, Map<String, String> codecOptions,
            StorageCopy copy,
            IDataContext dataCtx) {

        var signal = new CompletableFuture<Long>();

        var decoder = codec.getDecoder(arrowAllocator, schema, codecOptions);
        contentStream.subscribe(decoder);

        var storageKey = copy.getStorageKey();
        var storage = storageManager.getDataStorage(storageKey);
        var writer = storage.writer(schema, copy, signal, dataCtx);
        decoder.subscribe(writer);

        return signal;
    }

    private long checkRows(DataWriteRequest request, long rowsSaved) {

        // TODO: if (request.hasExpectedRows()) { ... }

        return rowsSaved;
    }
}
