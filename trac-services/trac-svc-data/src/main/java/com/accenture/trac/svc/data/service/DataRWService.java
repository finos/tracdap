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

import com.accenture.trac.api.TrustedMetadataApiGrpc;
import com.accenture.trac.api.config.DataServiceConfig;
import com.accenture.trac.common.codec.ICodecManager;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.metadata.*;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public class DataRWService {

    private final DataServiceConfig config;
    private final IStorageManager storageManager;
    private final ICodecManager codecManager;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi;

    public DataRWService(
            DataServiceConfig config,
            IStorageManager storageManager,
            ICodecManager codecManager,
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi) {

        this.config = config;
        this.storageManager = storageManager;
        this.codecManager = codecManager;
        this.metaApi = metaApi;
    }

    public CompletionStage<TagHeader> createDataset(
            String tenant, List<TagUpdate> tags,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        return CompletableFuture.failedFuture(new ETracInternal("Not implemented yet"));
    }

    public CompletionStage<TagHeader> updateDataset(
            String tenant, List<TagUpdate> tags,
            TagSelector priorVersion,
            Flow.Publisher<ByteBuf> contentStream,
            IExecutionContext execContext) {

        return CompletableFuture.failedFuture(new ETracInternal("Not implemented yet"));
    }

    public void readDataset(
            String tenant, TagSelector selector, String format,
            CompletableFuture<SchemaDefinition> schema,
            Flow.Subscriber<ByteBuf> content,
            IExecutionContext execCtx) {

        var state = new RequestState();

        var codec = codecManager.getEncoder(format);
        codec.subscribe(content);

        CompletableFuture.completedFuture(null)

                .thenCompose(x -> MetadataHelpers.readObject(metaApi, tenant, selector))
                .thenAccept(obj -> state.data = obj.getDefinition().getData())

                .thenCompose(x -> MetadataHelpers.readObject(metaApi, tenant, state.file.getStorageId()))
                .thenAccept(obj -> state.storage = obj.getDefinition().getStorage())

                .thenCompose(x -> readDatasetSchema(tenant, state.data))
                .thenApply(s -> state.schema = s)

                .thenAccept(x -> schema.complete(state.schema))

                .thenApply(x -> readDataset(state.data, state.schema, state.storage, execCtx))
                .thenAccept(batches -> batches.subscribe(codec))

                .exceptionally(error -> Helpers.reportError(error, schema, content));

        schema.completeExceptionally(new ETracInternal("Not implemented yet"));
    }

    private CompletableFuture<SchemaDefinition> readDatasetSchema(
            String tenant,
            DataDefinition dataDef) {

        if (dataDef.hasSchema())
            return CompletableFuture.completedFuture(dataDef.getSchema());

        if (dataDef.hasSchemaId())
            return CompletableFuture.completedFuture(null)
                    .thenCompose(x -> MetadataHelpers.readObject(metaApi, tenant, dataDef.getSchemaId()))
                    .thenApply(tag -> tag.getDefinition().getSchema());

        // TODO: Error, invalid data def
        throw new EUnexpected();
    }

    private Flow.Publisher<ArrowRecordBatch> readDataset(
            DataDefinition dataDef,
            SchemaDefinition schemaDef,
            StorageDefinition storageDef,
            IExecutionContext execCtx) {

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
