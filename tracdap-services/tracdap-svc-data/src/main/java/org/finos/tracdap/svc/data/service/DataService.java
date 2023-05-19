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

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.auth.internal.InternalAuthProvider;
import org.finos.tracdap.common.async.Futures;
import org.finos.tracdap.common.data.ArrowSchema;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.codec.ICodec;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.PartKeys;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.config.StorageConfig;
import org.finos.tracdap.config.TenantConfig;
import org.finos.tracdap.metadata.*;

import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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

    private static final Duration DATA_OPERATION_TIMEOUT = Duration.of(1, ChronoUnit.HOURS);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final StorageConfig storageConfig;
    private final Map<String, TenantConfig> tenantConfig;
    private final IStorageManager storageManager;
    private final ICodecManager codecManager;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient;
    private final InternalAuthProvider internalAuth;

    private final Validator validator = new Validator();
    private final Random random = new Random();

    public DataService(
            StorageConfig storageConfig,
            Map<String, TenantConfig> tenantConfig,
            IStorageManager storageManager,
            ICodecManager codecManager,
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaClient,
            InternalAuthProvider internalAuth) {

        this.storageConfig = storageConfig;
        this.tenantConfig = tenantConfig;
        this.storageManager = storageManager;
        this.codecManager = codecManager;
        this.metaClient = metaClient;
        this.internalAuth = internalAuth;
    }

    public CompletionStage<TagHeader> createDataset(
            DataWriteRequest request,
            Flow.Publisher<ArrowBuf> contentStream,
            IDataContext dataCtx,
            UserInfo userInfo) {

        var state = new RequestState();
        state.requestOwner = userInfo;
        state.requestTimestamp = Instant.now();
        state.credentials = internalAuth.createDelegateSession(state.requestOwner, DATA_OPERATION_TIMEOUT);

        var objectTimestamp = state.requestTimestamp.atOffset(ZoneOffset.UTC);

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
                // .thenApply(rowsSaved -> checkRows(request, rowsSaved))

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
            Flow.Publisher<ArrowBuf> contentStream,
            IDataContext dataCtx,
            UserInfo userInfo) {

        var state = new RequestState();
        state.requestOwner = userInfo;
        state.requestTimestamp = Instant.now();
        state.credentials = internalAuth.createDelegateSession(state.requestOwner, DATA_OPERATION_TIMEOUT);

        var prior = new RequestState();
        prior.credentials = state.credentials;

        var objectTimestamp = state.requestTimestamp.atOffset(ZoneOffset.UTC);

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
                // .thenApply(rowsSaved -> checkRows(request, rowsSaved))

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
            Flow.Subscriber<ArrowBuf> contentStream,
            IDataContext dataCtx,
            UserInfo userInfo) {

        var state = new RequestState();
        state.requestOwner = userInfo;
        state.requestTimestamp = Instant.now();
        state.credentials = internalAuth.createDelegateSession(state.requestOwner, DATA_OPERATION_TIMEOUT);

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

    private CompletionStage<Void> loadMetadata(String tenant, TagSelector dataSelector, RequestState state) {

        var client = metaClient.withCallCredentials(state.credentials);
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

    private CompletionStage<Void> loadStorageAndExternalSchema(String tenant, RequestState state) {

        var client = metaClient.withCallCredentials(state.credentials);
        var request = MetadataBuilders.requestForBatch(tenant, state.data.getStorageId(), state.data.getSchemaId());

        return Futures.javaFuture(client.readBatch(request))
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

        var client = metaClient.withCallCredentials(state.credentials);
        var request = MetadataBuilders.requestForSelector(tenant, state.data.getStorageId());

        return Futures.javaFuture(client.readObject(request))
                .thenAccept(tag -> {

                    state.storageId = tag.getHeader();
                    state.storage = tag.getDefinition().getStorage();

                    // A schema is still needed! Take a reference to the embedded schema object
                    state.schema = state.data.getSchema();
                });
    }

    private CompletionStage<SchemaDefinition> resolveSchema(DataWriteRequest request, RequestState state) {

        var client = metaClient.withCallCredentials(state.credentials);

        if (request.hasSchema()) {
            state.schema = request.getSchema();
            return CompletableFuture.completedFuture(state.schema);
        }

        if (request.hasSchemaId()) {

            var schemaReq = MetadataBuilders.requestForSelector(request.getTenant(), request.getSchemaId());

            return Futures.javaFuture(client.readObject(schemaReq))
                    .thenApply(tag -> state.schema = tag.getDefinition().getSchema());
        }

        throw new EUnexpected();
    }

    private void selectCopy(RequestState state) {

        var opaqueKey = PartKeys.opaqueKey(PartKeys.ROOT);

        // Snap index is not needed, since DataDefinition.Part only holds the current snap

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

    private CompletionStage<Void> preallocateIds(DataWriteRequest request, RequestState state) {

        var client = metaClient.withCallCredentials(state.credentials);

        var preAllocDataReq = MetadataBuilders.preallocateRequest(request.getTenant(), ObjectType.DATA);
        var preAllocStorageReq = MetadataBuilders.preallocateRequest(request.getTenant(), ObjectType.STORAGE);

        return CompletableFuture.completedFuture(0)

                .thenCompose(x -> Futures.javaFuture(client.preallocateId(preAllocDataReq)))
                .thenAccept(dataId -> state.preAllocDataId = dataId)

                .thenCompose(x -> Futures.javaFuture(client.preallocateId(preAllocStorageReq)))
                .thenAccept(storageId -> state.preAllocStorageId = storageId);
    }

    private CompletionStage<TagHeader> saveMetadata(DataWriteRequest request, RequestState state) {

        var client = metaClient.withCallCredentials(state.credentials);

        var priorStorageId = selectorFor(state.preAllocStorageId);
        var storageReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorStorageId, state.storage, state.storageTags);

        var priorDataId = selectorFor(state.preAllocDataId);
        var dataReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorDataId, state.data, state.dataTags);

        return Futures.javaFuture(client.createPreallocatedObject(storageReq))
                .thenCompose(x -> Futures.javaFuture(client.createPreallocatedObject(dataReq)));
    }

    private CompletionStage<TagHeader> saveMetadata(DataWriteRequest request, RequestState state, RequestState prior) {

        var client = metaClient.withCallCredentials(state.credentials);

        var priorStorageId = selectorFor(prior.storageId);
        var storageReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorStorageId, state.storage, state.storageTags);

        var priorDataId = selectorFor(prior.dataId);
        var dataReq = MetadataBuilders.buildCreateObjectReq(request.getTenant(), priorDataId, state.data, state.dataTags);

        return Futures.javaFuture(client.updateObject(storageReq))
                .thenCompose(x -> Futures.javaFuture(client.updateObject(dataReq)));
    }

    private RequestState buildMetadata(DataWriteRequest request, RequestState state, OffsetDateTime objectTimestamp) {

        state.dataTags = request.getTagUpdatesList();  // File tags requested by the client
        state.storageTags = List.of();                 // Storage tags is empty to start with

        state.dataId = MetadataBuilders.bumpVersion(state.preAllocDataId);
        state.storageId = MetadataBuilders.bumpVersion(state.preAllocStorageId);

        state.part = PartKeys.ROOT;
        state.snap = 0;
        state.delta = 0;

        var dataItem = buildDataItem(state);
        var storagePath = buildStoragePath(state);

        var dataDef = createDataDef(request, state, dataItem);
        var storageDef = createStorageDef(request.getTenant(), dataItem, storagePath, objectTimestamp);

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
        state.storage = updateStorageDef(request.getTenant(), prior.storage, dataItem, storagePath, objectTimestamp);

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

        return String.format(STORAGE_PATH_TEMPLATE,
                dataType, objectId,
                partKey, state.snap, state.delta,
                suffixBytes);
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

    private StorageDefinition createStorageDef(
            String tenant, String dataItem, String storagePath,
            OffsetDateTime objectTimestamp) {

        var storageItem = buildStorageItem(tenant, storagePath, objectTimestamp);

        return StorageDefinition.newBuilder()
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private StorageDefinition updateStorageDef(
            String tenant, StorageDefinition priorDef,
            String dataItem, String storagePath,
            OffsetDateTime objectTimestamp) {

        var storageItem = buildStorageItem(tenant, storagePath, objectTimestamp);

        return priorDef.toBuilder()
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private StorageItem buildStorageItem(String tenant, String storagePath, OffsetDateTime objectTimestamp) {

        // Currently tenant config is optional for single-tenant deployments, fall back to global defaults

        var storageBucket = storageConfig.getDefaultBucket();
        var storageFormat = storageConfig.getDefaultFormat();

        if (tenantConfig.containsKey(tenant)) {

            var tenantDefaults = tenantConfig.get(tenant);

            if (tenantDefaults.hasDefaultBucket())
                storageBucket = tenantDefaults.getDefaultBucket();

            if (tenantDefaults.hasDefaultFormat())
                storageFormat = tenantDefaults.getDefaultFormat();
        }

        // For the time being, data has one incarnation and a single storage copy

        var incarnationIndex = 0;

        var copy = StorageCopy.newBuilder()
                .setCopyStatus(CopyStatus.COPY_AVAILABLE)
                .setCopyTimestamp(MetadataCodec.encodeDatetime(objectTimestamp))
                .setStorageKey(storageBucket)
                .setStoragePath(storagePath)
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

        state.storageTags = controlledStorageAttrs(state.dataId);
    }

    private void finalizeUpdateMetadata(RequestState state, long rowsSaved) {

        // Currently, a no-op
    }

    private static List<TagUpdate> controlledStorageAttrs(TagHeader dataId) {

        // TODO: Special metadata Value type for handling tag selectors
        var selector = MetadataUtil.selectorForLatest(dataId);
        var storageObjectAttr = MetadataUtil.objectKey(selector);

        var storageForAttr = TagUpdate.newBuilder()
                .setAttrName(TRAC_STORAGE_OBJECT_ATTR)
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue(storageObjectAttr))
                .build();

        return List.of(storageForAttr);
    }

    private void loadAndEncode(
            RequestState request, Flow.Subscriber<ArrowBuf> contentStream,
            ICodec codec, Map<String, String> codecOptions,
            IDataContext dataCtx) {

        var requiredSchema = ArrowSchema.tracToArrow(request.schema);

        var storageKey = request.copy.getStorageKey();
        var storage = storageManager.getDataStorage(storageKey);
        var encoder = codec.getEncoder(dataCtx.arrowAllocator(), requiredSchema, codecOptions);

        var pipeline = storage.pipelineReader(request.copy, requiredSchema, dataCtx, request.offset, request.limit);
        pipeline.addStage(encoder);
        pipeline.addSink(contentStream);

        pipeline.execute();
    }

    private CompletionStage<Long> decodeAndSave(
            SchemaDefinition schema, Flow.Publisher<ArrowBuf> contentStream,
            ICodec codec, Map<String, String> codecOptions,
            StorageCopy copy,
            IDataContext dataCtx) {

        var requiredSchema = ArrowSchema.tracToArrow(schema);

        var signal = new CompletableFuture<Long>();

        var storageKey = copy.getStorageKey();
        var storage = storageManager.getDataStorage(storageKey);
        var decoder = codec.getDecoder(dataCtx.arrowAllocator(), requiredSchema, codecOptions);

        var pipeline = DataPipeline.forSource(contentStream, dataCtx);
        pipeline.addStage(decoder);
        pipeline = storage.pipelineWriter(copy, requiredSchema, dataCtx, pipeline, signal);

        pipeline.execute();

        return signal;
    }
}
