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

import com.accenture.trac.common.util.ApiWrapper;
import com.accenture.trac.svc.meta.services.MetadataReadService;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.api.meta.*;

import io.grpc.stub.StreamObserver;

import java.util.UUID;


public class MetadataReadApi extends MetadataReadApiGrpc.MetadataReadApiImplBase {

    private final ApiWrapper apiWrapper;
    private final MetadataReadService readService;

    public MetadataReadApi(MetadataReadService readService) {
        this.apiWrapper = new ApiWrapper(getClass(), ApiErrorMapping.ERROR_MAPPING);
        this.readService = readService;
    }

    @Override
    public void readObject(ReadRequest request, StreamObserver<Tag> response) {

        apiWrapper.unaryCall(response, () -> readService.readObject(request.getTenant(), request.getSelector()));
    }

    @Override
    public void readBatch(ReadBatchRequest request, StreamObserver<ReadBatchResponse> response) {

        apiWrapper.unaryCall(response, () -> readService.readObjects(request.getTenant(), request.getSelectorList())
            .thenApply(tags -> ReadBatchResponse.newBuilder().addAllTag(tags).build()));
    }

    @Override
    public void loadTag(MetadataReadRequest request, StreamObserver<Tag> response) {

        apiWrapper.unaryCall(response, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());
            var objectVersion = request.getObjectVersion();
            var tagVersion = request.getTagVersion();

            return readService.loadTag(tenant, objectType, objectId, objectVersion, tagVersion);
        });
    }

    @Override
    public void loadLatestTag(MetadataReadRequest request, StreamObserver<Tag> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());
            var objectVersion = request.getObjectVersion();

            return readService.loadLatestTag(tenant, objectType, objectId, objectVersion);
        });
    }

    @Override
    public void loadLatestObject(MetadataReadRequest request, StreamObserver<Tag> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());

            return readService.loadLatestObject(tenant, objectType, objectId);
        });
    }

}
