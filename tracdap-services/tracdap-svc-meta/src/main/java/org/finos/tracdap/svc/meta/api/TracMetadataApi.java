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
    static final MethodDescriptor<ListResourcesRequest, ListResourcesResponse> LIST_RESOURCES_METHOD = TracMetadataApiGrpc.getListResourcesMethod();
    static final MethodDescriptor<ResourceInfoRequest, ResourceInfoResponse> RESOURCE_INFO_METHOD = TracMetadataApiGrpc.getResourceInfoMethod();

    static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_OBJECT_METHOD = TracMetadataApiGrpc.getCreateObjectMethod();
    static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_OBJECT_METHOD = TracMetadataApiGrpc.getUpdateObjectMethod();
    static final MethodDescriptor<MetadataWriteRequest, TagHeader> UPDATE_TAG_METHOD = TracMetadataApiGrpc.getUpdateTagMethod();

    static final MethodDescriptor<MetadataReadRequest, Tag> READ_OBJECT_METHOD = TracMetadataApiGrpc.getReadObjectMethod();
    static final MethodDescriptor<MetadataBatchRequest, MetadataBatchResponse> READ_BATCH_METHOD = TracMetadataApiGrpc.getReadBatchMethod();
    static final MethodDescriptor<MetadataSearchRequest, MetadataSearchResponse> SEARCH_METHOD = TracMetadataApiGrpc.getSearchMethod();

    static final MethodDescriptor<MetadataGetRequest, Tag> GET_OBJECT_METHOD = TracMetadataApiGrpc.getGetObjectMethod();
    static final MethodDescriptor<MetadataGetRequest, Tag> GET_LATEST_OBJECT_METHOD = TracMetadataApiGrpc.getGetLatestObjectMethod();
    static final MethodDescriptor<MetadataGetRequest, Tag> GET_LATEST_TAG_METHOD = TracMetadataApiGrpc.getGetLatestTagMethod();

    private final MetadataApiImpl apiImpl;
    private final GrpcServerWrap grpcWrap;

    public TracMetadataApi(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService) {

        if (TRAC_METADATA_SERVICE == null)
            throw new EUnexpected();

        apiImpl = new MetadataApiImpl(TRAC_METADATA_SERVICE, readService, writeService, searchService, MetadataConstants.PUBLIC_API);
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
