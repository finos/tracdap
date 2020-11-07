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

import com.accenture.trac.common.api.*;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.common.util.ApiWrapper;

import com.accenture.trac.svc.meta.services.MetadataReadService;
import com.accenture.trac.svc.meta.services.MetadataSearchService;
import com.accenture.trac.svc.meta.services.MetadataWriteService;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.accenture.trac.svc.meta.services.MetadataConstants.PUBLIC_API;


public class BaseMetadataApi {

    // Only a limited set of object types can be created directly by clients
    // Everything else can only be created by the trusted API, i.e. by other TRAC platform components
    public static final List<ObjectType> PUBLIC_TYPES = Arrays.asList(
            ObjectType.FLOW,
            ObjectType.CUSTOM);

    private final ApiWrapper apiWrapper;

    private final MetadataReadService readService;
    private final MetadataWriteService writeService;
    private final MetadataSearchService searchService;

    private final boolean apiTrustLevel;

    public BaseMetadataApi(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService,
            boolean apiTrustLevel) {

        this.apiWrapper = new ApiWrapper(getClass(), ApiErrorMapping.ERROR_MAPPING);

        this.readService = readService;
        this.writeService = writeService;
        this.searchService = searchService;

        this.apiTrustLevel = apiTrustLevel;
    }

    void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();

            if (!PUBLIC_TYPES.contains(objectType)) {
                var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
                var status = Status.PERMISSION_DENIED.withDescription(message);
                return CompletableFuture.failedFuture(status.asRuntimeException());
            }

            return writeService.createObject(tenant, objectType,
                    request.getDefinition(),
                    request.getTagUpdateList(),
                    PUBLIC_API);
        });
    }

    void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();

            if (!PUBLIC_TYPES.contains(objectType)) {
                var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
                var status = Status.PERMISSION_DENIED.withDescription(message);
                return CompletableFuture.failedFuture(status.asRuntimeException());
            }

            return writeService.updateObject(tenant, objectType,
                    request.getPriorVersion(),
                    request.getDefinition(),
                    request.getTagUpdateList(),
                    PUBLIC_API);
        });
    }

    void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            return writeService.updateTag(
                    request.getTenant(),
                    request.getObjectType(),
                    request.getPriorVersion(),
                    request.getTagUpdateList(),
                    PUBLIC_API);
        });
    }

    void preallocateId(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();

            return writeService.preallocateId(tenant, objectType);
        });
    }

    void createPreallocatedObject(MetadataWriteRequest request, StreamObserver<TagHeader> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            return writeService.createPreallocatedObject(
                    request.getTenant(),
                    request.getObjectType(),
                    request.getPriorVersion(),
                    request.getDefinition(),
                    request.getTagUpdateList());
        });
    }

    void readObject(MetadataReadRequest request, StreamObserver<Tag> response) {

        apiWrapper.unaryCall(response, () -> readService.readObject(request.getTenant(), request.getSelector()));
    }

    void readBatch(MetadataBatchRequest request, StreamObserver<MetadataBatchResponse> response) {

        apiWrapper.unaryCall(response, () -> readService.readObjects(request.getTenant(), request.getSelectorList())
                .thenApply(tags -> MetadataBatchResponse.newBuilder().addAllTag(tags).build()));
    }

    void search(MetadataSearchRequest request, StreamObserver<MetadataSearchResponse> response) {

        apiWrapper.unaryCall(response, () -> {

            var tenant = request.getTenant();
            var searchParams = request.getSearchParams();

            var searchResult = searchService.search(tenant, searchParams);

            return searchResult.thenApply(resultList -> MetadataSearchResponse.newBuilder()
                    .addAllSearchResult(resultList)
                    .build());
        });
    }

    public void getObject(MetadataGetRequest request, StreamObserver<Tag> response) {

        apiWrapper.unaryCall(response, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());
            var objectVersion = request.getObjectVersion();
            var tagVersion = request.getTagVersion();

            return readService.loadTag(tenant, objectType, objectId, objectVersion, tagVersion);
        });
    }

    public void getLatestObject(MetadataGetRequest request, StreamObserver<Tag> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());

            return readService.loadLatestObject(tenant, objectType, objectId);
        });
    }

    public void getLatestTag(MetadataGetRequest request, StreamObserver<Tag> responseObserver) {

        apiWrapper.unaryCall(responseObserver, () -> {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());
            var objectVersion = request.getObjectVersion();

            return readService.loadLatestTag(tenant, objectType, objectId, objectVersion);
        });
    }
}
