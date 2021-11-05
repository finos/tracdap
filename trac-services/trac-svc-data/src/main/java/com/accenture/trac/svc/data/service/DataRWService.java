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

import com.accenture.trac.api.DataReadRequest;
import com.accenture.trac.api.DataWriteRequest;
import com.accenture.trac.api.MetadataWriteRequest;
import com.accenture.trac.api.TrustedMetadataApiGrpc;
import com.accenture.trac.api.config.DataServiceConfig;
import com.accenture.trac.common.codec.ICodec;
import com.accenture.trac.common.codec.ICodecManager;
import com.accenture.trac.common.concurrent.Futures;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.data.DataContext;
import com.accenture.trac.common.data.IDataContext;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.metadata.*;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.NettyAllocationManager;
import org.apache.arrow.memory.RootAllocator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import static com.accenture.trac.common.metadata.MetadataUtil.selectorFor;
import static com.accenture.trac.common.metadata.MetadataUtil.selectorForLatest;
import static com.accenture.trac.svc.data.service.MetadataBuilders.*;

public class DataRWService {

    private final DataServiceConfig config;
    private final IStorageManager storageManager;
    private final ICodecManager codecManager;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi;

    private final BufferAllocator arrowAllocator;

    public DataRWService(
            DataServiceConfig config,
            IStorageManager storageManager,
            ICodecManager codecManager,
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi) {

        this.config = config;
        this.storageManager = storageManager;
        this.codecManager = codecManager;
        this.metaApi = metaApi;

        // TODO: Arrow allocator should probably be owned by the main service class and passed in

        var arrowAllocatorConfig = RootAllocator
                .configBuilder()
                .allocationManagerFactory(NettyAllocationManager.FACTORY)
                .build();

        this.arrowAllocator = new RootAllocator(arrowAllocatorConfig);
    }

    public CompletionStage<TagHeader> createDataset(
            DataWriteRequest request,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execCtx) {

        var dataCtx = new DataContext(execCtx.eventLoopExecutor(), arrowAllocator);
        var state = new RequestState();

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
                .thenApply(x -> buildMetadata(request, state))

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
            IExecutionContext execContext) {

        return CompletableFuture.failedFuture(new ETracInternal("Not implemented yet"));
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

                .thenCompose(x -> loadMetadata(request, state))

                // Resolve a concrete schema to use for this load operation
                // This should succeed so long as the metadata service is up,
                // because the schema metadata was validated when the dataset was saved
                .thenCompose(x -> resolveSchema(request, state))

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

    private CompletionStage<Void> loadMetadata(DataReadRequest request, RequestState state) {

        var dataReq = MetadataBuilders.requestForSelector(request.getTenant(), request.getSelector());

        return Futures

                .javaFuture(metaApi.readObject(dataReq))
                .thenAccept(tag -> {
                    state.dataId = tag.getHeader();
                    state.data = tag.getDefinition().getData();
                })

                .thenCompose(x -> MetadataHelpers.readObject(metaApi, request.getTenant(), state.data.getStorageId()))
                .thenAccept(tag -> {
                    state.storageId = tag.getHeader();
                    state.storage = tag.getDefinition().getStorage();
                });
    }

    private CompletionStage<SchemaDefinition> resolveSchema(DataReadRequest request, RequestState state) {

        var dataDef = state.data;

        if (dataDef.hasSchema()) {
            state.schema = dataDef.getSchema();
            return CompletableFuture.completedFuture(state.schema);
        }

        if (dataDef.hasSchemaId()) {

            var schemaReq = MetadataBuilders.requestForSelector(request.getTenant(), dataDef.getSchemaId());
            return Futures
                    .javaFuture(metaApi.readObject(schemaReq))
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
                    .javaFuture(metaApi.readObject(schemaReq))
                    .thenApply(tag -> state.schema = tag.getDefinition().getSchema());
        }

        throw new EUnexpected();
    }

    private CompletionStage<Void> preallocateIds(DataWriteRequest request, RequestState state) {

        var preAllocDataReq = preallocateRequest(request.getTenant(), ObjectType.DATA);
        var preAllocStorageReq = preallocateRequest(request.getTenant(), ObjectType.STORAGE);

        return CompletableFuture.completedFuture(0)

                .thenCompose(x -> Futures.javaFuture(metaApi.preallocateId(preAllocDataReq)))
                .thenAccept(dataId -> state.priorDataId = dataId)

                .thenCompose(x -> Futures.javaFuture(metaApi.preallocateId(preAllocStorageReq)))
                .thenAccept(storageId -> state.priorStorageId = storageId);
    }

    private RequestState buildMetadata(DataWriteRequest request, RequestState state) {

        state.dataTags = request.getTagUpdatesList();  // File tags requested by the client
        state.storageTags = List.of();                 // Storage tags is empty to start with

        state.dataId = bumpVersion(state.priorDataId);
        state.storageId = bumpVersion(state.priorStorageId);

        var dataItem = buildDataItem(request, state);
        var dataDef = buildDataDef(request, state, dataItem);
        var storageDef = buildStorageDef(request, dataItem);

        state.data = dataDef;
        state.storage = storageDef;

        state.copy = storageDef
                .getDataItemsOrThrow(dataItem)
                .getIncarnations(0)
                .getCopies(0);

        return state;
    }

    private String buildDataItem(DataWriteRequest request, RequestState state) {

        var snapIndex = 0;
        var deltaIndex = 0;

        var dataItemTemplate = "data/table/%s/part-%s/snap-%d/delta-%d";

        return String.format(dataItemTemplate,
                state.dataId.getObjectId(), PartType.PART_ROOT,  // TODO
                snapIndex, deltaIndex);
    }

    private DataDefinition buildDataDef(DataWriteRequest request, RequestState state, String dataItem) {

        var partKey = PartKey.newBuilder()
                .setPartType(PartType.PART_ROOT)
                .setOpaqueKey(PartType.PART_ROOT.name());  // TODO: opaque key

        var snapIndex = 0;
        var deltaIndex = 0;

        var storageId = selectorForLatest(state.storageId);

        var delta = DataDefinition.Delta.newBuilder()
                .setDeltaIndex(deltaIndex)
                .setDataItem(dataItem);

        var snap = DataDefinition.Snap.newBuilder()
                .setSnapIndex(snapIndex)
                .addDeltas(deltaIndex, delta);

        var part = DataDefinition.Part.newBuilder()
                .setPartKey(partKey)
                .setSnap(snap)
                .build();

        var dataDef = DataDefinition.newBuilder()
                .setStorageId(storageId);

        if (request.hasSchema())
            dataDef.setSchema(request.getSchema());
        else if (request.hasSchemaId())
            dataDef.setSchemaId(request.getSchemaId());
        else
            throw new EUnexpected();  // TODO

        return dataDef
                .putParts(partKey.getOpaqueKey(), part)
                .build();
    }

    private StorageDefinition buildStorageDef(DataWriteRequest request, String dataItem) {

        var storageKey = config.getDefaultStorage();
        var storageFormat = "text/csv";  // TODO  application/vnd.apache.arrow.file
        var incarnationIndex = 0;

        var copy = StorageCopy.newBuilder()
                .setCopyStatus(CopyStatus.COPY_AVAILABLE)
                //.setCopyTimestamp(null)  // TODO
                .setStorageKey(storageKey)
                .setStoragePath(dataItem)
                .setStorageFormat(storageFormat);

        var incarnation = StorageIncarnation.newBuilder()
                .setIncarnationStatus(IncarnationStatus.INCARNATION_AVAILABLE)
                .setIncarnationIndex(incarnationIndex)
                //.setIncarnationTimestamp(null)  // todo
                .addCopies(copy);

        var storageItem = StorageItem.newBuilder()
                .addIncarnations(incarnationIndex, incarnation)
                .build();

        return StorageDefinition.newBuilder()
                .putDataItems(dataItem, storageItem)
                .build();
    }

    private void finalizeMetadata(RequestState state, long rowsSaved) {

    }

    private CompletionStage<TagHeader> saveMetadata(DataWriteRequest request, RequestState state) {

       //CompletionStage<TagHeader> saveSchema;
        CompletionStage<TagHeader> saveStorage;
        CompletionStage<TagHeader> saveData;

        //saveSchema = CompletableFuture.completedFuture(null);

        var priorStorageId = selectorFor(state.priorStorageId);
        var storageReq = buildCreateObjectReq(request.getTenant(), priorStorageId, state.storage, state.storageTags);

        var priorDataId = selectorFor(state.priorDataId);
        var dataReq = buildCreateObjectReq(request.getTenant(), priorDataId, state.data, state.dataTags);

        return CompletableFuture.completedFuture(0)

                .thenApply(x -> storageReq)
                .thenApply(state.priorStorage == null ? metaApi::createPreallocatedObject : metaApi::updateObject)
                .thenCompose(Futures::javaFuture)

                .thenApply(x -> dataReq)
                .thenApply(state.priorData == null ? metaApi::createPreallocatedObject : metaApi::updateObject)
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


    private Flow.Publisher<DataBlock> readDataset(
            DataDefinition dataDef,
            SchemaDefinition schemaDef,
            StorageDefinition storageDef,
            IDataContext execCtx) {

        var partKey = dataDef.getPartsMap().keySet().stream().findFirst().get();  // TODO: Root part
        var part = dataDef.getPartsOrThrow(partKey);
        var snap = part.getSnap();
        var delta = snap.getDeltas(0);

        var dataItem = delta.getDataItem();
        var storageItem = storageDef.getDataItemsOrThrow(dataItem);
        var incarnation = storageItem.getIncarnations(storageItem.getIncarnationsCount() - 1);
        var copy = incarnation.getCopies(0);

        var storageKey = copy.getStorageKey();
        var storage = storageManager.getDataStorage(storageKey);

        return storage.reader(schemaDef, copy, execCtx);
    }
}
