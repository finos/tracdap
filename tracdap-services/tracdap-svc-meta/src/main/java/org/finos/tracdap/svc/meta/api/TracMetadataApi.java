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
import org.finos.tracdap.common.grpc.GrpcServerWrap;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.svc.meta.services.MetadataConstants;
import org.finos.tracdap.svc.meta.services.MetadataReadService;
import org.finos.tracdap.svc.meta.services.MetadataSearchService;
import org.finos.tracdap.svc.meta.services.MetadataWriteService;

import io.grpc.stub.StreamObserver;


public class TracMetadataApi extends TracMetadataApiGrpc.TracMetadataApiImplBase {

    private final MetadataApiImpl apiImpl;
    private final GrpcServerWrap grpcWrap;

    public TracMetadataApi(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService) {

        apiImpl = new MetadataApiImpl(readService, writeService, searchService, MetadataConstants.PUBLIC_API);
        grpcWrap = new GrpcServerWrap();
    }

    @Override
    public void platformInfo(PlatformInfoRequest request, StreamObserver<PlatformInfoResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::platformInfo);
    }

    @Override
    public void listTenants(ListTenantsRequest request, StreamObserver<ListTenantsResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::listTenants);
    }

    @Override
    public void listResources(ListResourcesRequest request, StreamObserver<ListResourcesResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::listResources);
    }

    @Override
    public void resourceInfo(ResourceInfoRequest request, StreamObserver<ResourceInfoResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::resourceInfo);
    }

    @Override
    public void writeBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::writeBatch);
    }

    @Override
    public void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::createObject);
    }

    @Override
    public void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::updateObject);
    }

    @Override
    public void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::updateTag);
    }

    @Override
    public void readObject(MetadataReadRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(request, response, apiImpl::readObject);
    }

    @Override
    public void readBatch(MetadataBatchRequest request, StreamObserver<MetadataBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::readBatch);
    }

    @Override
    public void search(MetadataSearchRequest request, StreamObserver<MetadataSearchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::search);
    }

    @Override
    public void getObject(MetadataGetRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(request, response, apiImpl::getObject);
    }

    @Override
    public void getLatestObject(MetadataGetRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(request, response, apiImpl::getLatestObject);
    }

    @Override
    public void getLatestTag(MetadataGetRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(request, response, apiImpl::getLatestTag);
    }
}
