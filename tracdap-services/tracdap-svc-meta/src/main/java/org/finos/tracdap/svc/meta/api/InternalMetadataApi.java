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

package org.finos.tracdap.svc.meta.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.api.internal.InternalMetadataApiGrpc;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.svc.meta.services.*;

import io.grpc.stub.StreamObserver;


public class InternalMetadataApi extends InternalMetadataApiGrpc.InternalMetadataApiImplBase {

    private final MetadataApiImpl apiImpl;

    public InternalMetadataApi(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService,
            ConfigService configService) {

        apiImpl = new MetadataApiImpl(readService, writeService, searchService, configService, MetadataApiImpl.INTERNAL_API);
    }

    @Override
    public void platformInfo(PlatformInfoRequest request, StreamObserver<PlatformInfoResponse> response) {

        apiImpl.platformInfo(request, response);
    }

    @Override
    public void listTenants(ListTenantsRequest request, StreamObserver<ListTenantsResponse> response) {

        apiImpl.listTenants(request, response);
    }

    @Override
    public void writeBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        apiImpl.writeBatch(request, response);
    }

    @Override
    public void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        apiImpl.createObject(request, response);
    }

    @Override
    public void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        apiImpl.updateObject(request, response);
    }

    @Override
    public void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        apiImpl.updateTag(request, response);
    }

    @Override
    public void preallocateId(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        apiImpl.preallocateId(request, response);
    }

    @Override
    public void createPreallocatedObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        apiImpl.createPreallocatedObject(request, response);
    }

    @Override
    public void readObject(MetadataReadRequest request, StreamObserver<Tag> response) {

        apiImpl.readObject(request, response);
    }

    @Override
    public void readBatch(MetadataBatchRequest request, StreamObserver<MetadataBatchResponse> response) {

        apiImpl.readBatch(request, response);
    }

    @Override
    public void search(MetadataSearchRequest request, StreamObserver<MetadataSearchResponse> response) {

        apiImpl.search(request, response);
    }

    @Override
    public void createConfigObject(ConfigWriteRequest request, StreamObserver<ConfigWriteResponse> response) {

        apiImpl.createConfigObject(request, response);
    }

    @Override
    public void updateConfigObject(ConfigWriteRequest request, StreamObserver<ConfigWriteResponse> response) {

        apiImpl.updateConfigObject(request, response);
    }

    @Override
    public void deleteConfigObject(ConfigWriteRequest request, StreamObserver<ConfigWriteResponse> response) {

        apiImpl.deleteConfigObject(request, response);
    }

    @Override
    public void readConfigEntry(ConfigReadRequest request, StreamObserver<ConfigReadResponse> response) {

        apiImpl.readConfigEntry(request, response);
    }

    @Override
    public void readConfigBatch(ConfigReadBatchRequest request, StreamObserver<ConfigReadBatchResponse> response) {

        apiImpl.readConfigBatch(request, response);
    }

    @Override
    public void listConfigEntries(ConfigListRequest request, StreamObserver<ConfigListResponse> response) {

        apiImpl.listConfigEntries(request, response);
    }
}
