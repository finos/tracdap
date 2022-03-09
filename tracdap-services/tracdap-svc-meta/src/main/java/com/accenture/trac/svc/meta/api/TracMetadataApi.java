/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.grpc.GrpcServerWrap;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;
import com.accenture.trac.svc.meta.services.MetadataReadService;
import com.accenture.trac.svc.meta.services.MetadataSearchService;
import com.accenture.trac.svc.meta.services.MetadataWriteService;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

import static com.accenture.trac.svc.meta.services.MetadataConstants.PUBLIC_API;


public class TracMetadataApi extends TracMetadataApiGrpc.TracMetadataApiImplBase {

    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_OBJECT_METHOD = TracMetadataApiGrpc.getCreateObjectMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_OBJECT_METHOD = TracMetadataApiGrpc.getUpdateObjectMethod();
    private static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_TAG_METHOD = TracMetadataApiGrpc.getUpdateTagMethod();

    private static final MethodDescriptor<MetadataReadRequest, Tag> READ_OBJECT_METHOD = TracMetadataApiGrpc.getReadObjectMethod();
    private static final MethodDescriptor<MetadataBatchRequest, MetadataBatchResponse> READ_BATCH_METHOD = TracMetadataApiGrpc.getReadBatchMethod();
    private static final MethodDescriptor<MetadataSearchRequest, MetadataSearchResponse> SEARCH_METHOD = TracMetadataApiGrpc.getSearchMethod();

    private static final MethodDescriptor<MetadataGetRequest, Tag> GET_OBJECT_METHOD = TracMetadataApiGrpc.getGetObjectMethod();
    private static final MethodDescriptor<MetadataGetRequest, Tag> GET_LATEST_OBJECT_METHOD = TracMetadataApiGrpc.getGetObjectMethod();
    private static final MethodDescriptor<MetadataGetRequest, Tag> GET_LATEST_TAG_METHOD = TracMetadataApiGrpc.getGetObjectMethod();

    private final MetadataApiImpl apiImpl;
    private final GrpcServerWrap grpcWrap;

    public TracMetadataApi(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService) {

        apiImpl = new MetadataApiImpl(readService, writeService, searchService, PUBLIC_API);
        grpcWrap = new GrpcServerWrap(getClass());
    }

    @Override
    public void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(CREATE_OBJECT_METHOD, request, response, apiImpl::createObject);
    }

    @Override
    public void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(UPDATE_OBJECT_METHOD, request, response, apiImpl::updateObject);
    }

    @Override
    public void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(UPDATE_TAG_METHOD, request, response, apiImpl::updateTag);
    }

    @Override
    public void readObject(MetadataReadRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(READ_OBJECT_METHOD, request, response, apiImpl::readObject);
    }

    @Override
    public void readBatch(MetadataBatchRequest request, StreamObserver<MetadataBatchResponse> response) {

        grpcWrap.unaryCall(READ_BATCH_METHOD, request, response, apiImpl::readBatch);
    }

    @Override
    public void search(MetadataSearchRequest request, StreamObserver<MetadataSearchResponse> response) {

        grpcWrap.unaryCall(SEARCH_METHOD, request, response, apiImpl::search);
    }

    @Override
    public void getObject(MetadataGetRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(GET_OBJECT_METHOD, request, response, apiImpl::getObject);
    }

    @Override
    public void getLatestObject(MetadataGetRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(GET_LATEST_OBJECT_METHOD, request, response, apiImpl::getLatestObject);
    }

    @Override
    public void getLatestTag(MetadataGetRequest request, StreamObserver<Tag> response) {

        grpcWrap.unaryCall(GET_LATEST_TAG_METHOD, request, response, apiImpl::getLatestTag);
    }
}
