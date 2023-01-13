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


public class TrustedMetadataApi extends TrustedMetadataApiGrpc.TrustedMetadataApiImplBase {

    private static final String SERVICE_NAME = TrustedMetadataApiGrpc.SERVICE_NAME.substring(TrustedMetadataApiGrpc.SERVICE_NAME.lastIndexOf(".") + 1);
    private static final Descriptors.ServiceDescriptor TRUSTED_METADATA_SERVICE = MetadataTrusted.getDescriptor().findServiceByName(SERVICE_NAME);

    static final MethodDescriptor<MetadataWriteRequest, TagHeader> PREALLOCATE_ID_METHOD = TrustedMetadataApiGrpc.getPreallocateIdMethod();
    static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> PREALLOCATE_ID_BATCH_METHOD = TrustedMetadataApiGrpc.getPreallocateIdBatchMethod();
    static final MethodDescriptor<MetadataWriteRequest, TagHeader> CREATE_PREALLOCATED_OBJECT_METHOD = TrustedMetadataApiGrpc.getCreatePreallocatedObjectMethod();
    static final MethodDescriptor<MetadataWriteBatchRequest, MetadataWriteBatchResponse> CREATE_PREALLOCATED_OBJECT_BATCH_METHOD = TrustedMetadataApiGrpc.getCreatePreallocatedObjectBatchMethod();



    private final MetadataApiImpl apiImpl;
    private final GrpcServerWrap grpcWrap;

    public TrustedMetadataApi(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService) {

        if (TRUSTED_METADATA_SERVICE == null)
            throw new EUnexpected();

        apiImpl = new MetadataApiImpl(TRUSTED_METADATA_SERVICE, readService, writeService, searchService, MetadataConstants.TRUSTED_API);
        grpcWrap = new GrpcServerWrap();
    }

    @Override
    public void universalWrite(UniversalMetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::universalWrite);
    }

    @Override
    public void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::createObject);
    }

    @Override
    public void createObjectBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::createObjectBatch);
    }

    @Override
    public void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::updateObject);
    }

    @Override
    public void updateObjectBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::updateObjectBatch);
    }

    @Override
    public void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::updateTag);
    }

    @Override
    public void updateTagBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::updateTagBatch);
    }

    @Override
    public void preallocateId(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::preallocateId);
    }

    @Override
    public void preallocateIdBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::preallocateIdBatch);
    }

    @Override
    public void createPreallocatedObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        grpcWrap.unaryCall(request, response, apiImpl::createPreallocatedObject);
    }

    @Override
    public void createPreallocatedObjectBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        grpcWrap.unaryCall(request, response, apiImpl::createPreallocatedObjectBatch);
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
}
