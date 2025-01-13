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

import io.grpc.stub.StreamObserver;
import org.finos.tracdap.api.*;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.exception.EAuthorization;
import org.finos.tracdap.svc.meta.services.MetadataReadService;
import org.finos.tracdap.svc.meta.services.MetadataSearchService;
import org.finos.tracdap.svc.meta.services.MetadataWriteService;

import java.util.List;
import java.util.UUID;

import static org.finos.tracdap.common.metadata.MetadataConstants.PUBLIC_WRITABLE_OBJECT_TYPES;
import static org.finos.tracdap.svc.meta.services.MetadataConstants.PUBLIC_API;


public class MetadataApiImpl {

    private final MetadataReadService readService;
    private final MetadataWriteService writeService;
    private final MetadataSearchService searchService;

    private final boolean apiTrustLevel;

    MetadataApiImpl(
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService,
            boolean apiTrustLevel) {

        this.readService = readService;
        this.writeService = writeService;
        this.searchService = searchService;

        this.apiTrustLevel = apiTrustLevel;
    }

    @SuppressWarnings("unused")
    void platformInfo(PlatformInfoRequest request, StreamObserver<PlatformInfoResponse> response) {

        try {
            var result = readService.platformInfo();
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    @SuppressWarnings("unused")
    void listTenants(ListTenantsRequest request, StreamObserver<ListTenantsResponse> response) {

        try {
            var result = readService.listTenants();
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void clientConfig(ClientConfigRequest request, StreamObserver<ClientConfigResponse> response) {

        try {
            var result = readService.clientConfig(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void listResources(ListResourcesRequest request, StreamObserver<ListResourcesResponse> response) {

        try {
            var result = readService.listResources(request.getTenant(), request.getResourceType());
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void resourceInfo(ResourceInfoRequest request, StreamObserver<ResourceInfoResponse> response) {

        try {
            var result = readService.resourceInfo(request.getTenant(), request.getResourceType(), request.getResourceKey());
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void createObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        try {

            validateObjectType(request.getObjectType());

            var result = writeService.createObject(request.getTenant(), request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void updateObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        try {

            validateObjectType(request.getObjectType());

            var result = writeService.updateObject(request.getTenant(), request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void updateTag(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        try {

            // Do not check object type for update tags, this is allowed for all types in the public API

            var result = writeService.updateTag(request.getTenant(), request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void preallocateId(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        try {

            validateObjectType(request.getObjectType());

            var result = writeService.preallocateId(request.getTenant(), request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void createPreallocatedObject(MetadataWriteRequest request, StreamObserver<TagHeader> response) {

        try {

            validateObjectType(request.getObjectType());

            var result = writeService.createPreallocatedObject(request.getTenant(), request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void writeBatch(MetadataWriteBatchRequest request, StreamObserver<MetadataWriteBatchResponse> response) {

        try {

            validateListForObjectType(request.getPreallocateIdsList());
            validateListForObjectType(request.getCreatePreallocatedObjectsList());
            validateListForObjectType(request.getCreateObjectsList());
            validateListForObjectType(request.getUpdateObjectsList());

            // Do not check object type for update tags, this is allowed for all types in the public API

            var result = writeService.writeBatch(request);
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void readObject(MetadataReadRequest request, StreamObserver<Tag> response) {

        try {
            var result = readService.readObject(request.getTenant(), request.getSelector());
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void readBatch(MetadataBatchRequest request, StreamObserver<MetadataBatchResponse> response) {

        try {
            var tags = readService.readObjects(request.getTenant(), request.getSelectorList());
            var result = MetadataBatchResponse.newBuilder().addAllTag(tags).build();
            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void search(MetadataSearchRequest request, StreamObserver<MetadataSearchResponse> response) {

        try {

            var tenant = request.getTenant();
            var searchParams = request.getSearchParams();

            var searchResult = searchService.search(tenant, searchParams);
            var result = MetadataSearchResponse.newBuilder()
                    .addAllSearchResult(searchResult)
                    .build();

            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void getObject(MetadataGetRequest request, StreamObserver<Tag> response) {

        try {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());
            var objectVersion = request.getObjectVersion();
            var tagVersion = request.getTagVersion();

            var result = readService.loadTag(tenant, objectType, objectId, objectVersion, tagVersion);

            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void getLatestObject(MetadataGetRequest request, StreamObserver<Tag> response) {

        try {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());

            var result = readService.loadLatestObject(tenant, objectType, objectId);

            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    void getLatestTag(MetadataGetRequest request, StreamObserver<Tag> response) {

        try {

            var tenant = request.getTenant();
            var objectType = request.getObjectType();
            var objectId = UUID.fromString(request.getObjectId());
            var objectVersion = request.getObjectVersion();

            var result = readService.loadLatestTag(tenant, objectType, objectId, objectVersion);

            response.onNext(result);
            response.onCompleted();
        }
        catch (Exception error) {
            response.onError(error);
        }
    }

    private void validateObjectType(ObjectType objectType) {

        if (apiTrustLevel == PUBLIC_API && !PUBLIC_WRITABLE_OBJECT_TYPES.contains(objectType)) {
            var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
            throw new EAuthorization(message);
        }
    }

    private void validateListForObjectType(List<MetadataWriteRequest> requestsList) {

        for (var rq : requestsList) {
            validateObjectType(rq.getObjectType());
        }
    }
}
