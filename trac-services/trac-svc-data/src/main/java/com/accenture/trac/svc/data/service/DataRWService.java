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
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.storage.IStorageManager;
import com.accenture.trac.metadata.*;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public class DataRWService {

    private final DataServiceConfig config;
    private final IStorageManager storageManager;
    private final TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi;

    public DataRWService(
            DataServiceConfig config,
            IStorageManager storageManager,
            TrustedMetadataApiGrpc.TrustedMetadataApiFutureStub metaApi) {

        this.config = config;
        this.storageManager = storageManager;
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

        schema.completeExceptionally(new ETracInternal("Not implemented yet"));
    }
}
