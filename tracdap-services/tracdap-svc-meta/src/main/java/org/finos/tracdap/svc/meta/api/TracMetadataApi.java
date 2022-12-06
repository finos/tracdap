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

package org.finos.tracdap.svc.meta.api;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.grpc.GrpcServerWrap;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.svc.meta.services.MetadataReadService;
import org.finos.tracdap.svc.meta.services.MetadataSearchService;
import org.finos.tracdap.svc.meta.services.MetadataWriteService;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.finos.tracdap.svc.meta.services.MetadataConstants;


public class TracMetadataApi extends TracMetadataApiGrpc.TracMetadataApiImplBase {

    private static final String SERVICE_NAME = TracMetadataApiGrpc.SERVICE_NAME.substring(TracMetadataApiGrpc.SERVICE_NAME.lastIndexOf(".") + 1);
    private static final Descriptors.ServiceDescriptor TRAC_METADATA_SERVICE = Metadata.getDescriptor().findServiceByName(SERVICE_NAME);

    static final MethodDescriptor<PlatformInfoRequest, PlatformInfoResponse> PLATFORM_INFO_METHOD = TracMetadataApiGrpc.getPlatformInfoMethod();
    static final MethodDescriptor<ListTenantsRequest, ListTenantsResponse> LIST_TENANTS_METHOD = TracMetadataApiGrpc.getListTenantsMethod();

    static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_OBJECT_METHOD = TracMetadataApiGrpc.getCreateObjectMethod();
    static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> CREATE_OBJECT_BATCH_METHOD = TracMetadataApiGrpc.getCreateObjectBatchMethod();
    static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_OBJECT_METHOD = TracMetadataApiGrpc.getUpdateObjectMethod();
    static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> UPDATE_OBJECT_BATCH_METHOD = TracMetadataApiGrpc.getUpdateObjectBatchMethod();
    static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_TAG_METHOD = TracMetadataApiGrpc.getUpdateTagMethod();
    static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> UPDATE_TAG_BATCH_METHOD = TracMetadataApiGrpc.getUpdateTagBatchMethod();

    static final MethodDescriptor<MetadataReadRequest, Tag> READ_OBJECT_METHOD = TracMetadataApiGrpc.getReadObjectMethod();
    static final MethodDescriptor<MetadataBatchRequest, MetadataBatchResponse> READ_BATCH_METHOD = TracMetadataApiGrpc.getReadBatchMethod();
    static final MethodDescriptor<MetadataSearchRequest, MetadataSearchResponse> SEARCH_METHOD = TracMetadataApiGrpc.getSearchMethod();

    static final MethodDescriptor<MetadataGetRequest, Tag> GET_OBJECT_METHOD = TracMetadataApiGrpc.getGetObjectMethod();
    static final MethodDescriptor<MetadataGetRequest, Tag> GET_LATEST_OBJECT_METHOD = TracMetadataApiGrpc.getGetObjectMethod();
    static final MethodDescriptor<MetadataGetRequest, Tag> GET_LATEST_TAG_METHOD = TracMetadataApiGrpc.getGetObjectMethod();

    private final MetadataApiImpl apiImpl;
    private final GrpcServerWrap grpcWrap;

    public TracMetadataApi(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService) {

        if (TRAC_METADATA_SERVICE == null)
            throw new EUnexpected();

        apiImpl = new MetadataApiImpl(TRAC_METADATA_SERVICE, readService, writeService, searchService, MetadataConstants.PUBLIC_API);
        grpcWrap = new GrpcServerWrap(getClass());
    }

    @Override
    public void platformInfo(PlatformInfoRequest request, StreamObserver<PlatformInfoResponse> response) {

        grpcWrap.unaryCall(PLATFORM_INFO_METHOD, request, response, apiImpl::platformInfo);
    }

    @Override
    public void listTenants(ListTenantsRequest request, StreamObserver<ListTenantsResponse> response) {

        grpcWrap.unaryCall(LIST_TENANTS_METHOD, request, response, apiImpl::listTenants);
    }

    @Override
    public void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(CREATE_OBJECT_METHOD, request, response, apiImpl::createObject);
    }

    @Override
    public void createObjectBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(CREATE_OBJECT_BATCH_METHOD, request, response, apiImpl::createObjectBatch);
    }

    @Override
    public void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(UPDATE_OBJECT_METHOD, request, response, apiImpl::updateObject);
    }

    @Override
    public void updateObjectBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(UPDATE_OBJECT_BATCH_METHOD, request, response, apiImpl::updateObjectBatch);
    }

    @Override
    public void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(UPDATE_TAG_METHOD, request, response, apiImpl::updateTag);
    }

    @Override
    public void updateTagBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(UPDATE_TAG_BATCH_METHOD, request, response, apiImpl::updateTagBatch);
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
