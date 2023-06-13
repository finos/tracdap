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

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.svc.meta.services.MetadataReadService;
import org.finos.tracdap.svc.meta.services.MetadataSearchService;
import org.finos.tracdap.svc.meta.services.MetadataWriteService;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.metadata.MetadataConstants.PUBLIC_WRITABLE_OBJECT_TYPES;
import static org.finos.tracdap.svc.meta.api.TracMetadataApi.*;
import static org.finos.tracdap.svc.meta.api.TrustedMetadataApi.CREATE_PREALLOCATED_OBJECT_METHOD;
import static org.finos.tracdap.svc.meta.api.TrustedMetadataApi.PREALLOCATE_ID_METHOD;
import static org.finos.tracdap.svc.meta.api.TrustedMetadataApi.PREALLOCATE_ID_BATCH_METHOD;
import static org.finos.tracdap.svc.meta.services.MetadataConstants.PUBLIC_API;


public class MetadataApiImpl {

    private final Descriptors.ServiceDescriptor serviceDescriptor;
    private final Validator validator;

    private final MetadataReadService readService;
    private final MetadataWriteService writeService;
    private final MetadataSearchService searchService;

    private final boolean apiTrustLevel;

    public MetadataApiImpl(
            Descriptors.ServiceDescriptor serviceDescriptor,
            MetadataReadService readService,
            MetadataWriteService writeService,
            MetadataSearchService searchService,
            boolean apiTrustLevel) {

        this.serviceDescriptor = serviceDescriptor;
        this.validator = new Validator();

        this.readService = readService;
        this.writeService = writeService;
        this.searchService = searchService;

        this.apiTrustLevel = apiTrustLevel;
    }

    PlatformInfoResponse platformInfo(PlatformInfoRequest request) {

        return readService.platformInfo();
    }

    ListTenantsResponse listTenants(ListTenantsRequest request) {

        return readService.listTenants();
    }

    TagHeader createObject(MetadataWriteRequest request) {

        validateRequest(CREATE_OBJECT_METHOD, request);
        validateObjectType(request.getObjectType());

        return writeService.createObjects(
                request.getTenant(),
                Collections.singletonList(request),
                Collections.emptyList()).get(0);
    }

    TagHeader updateObject(MetadataWriteRequest request) {

        validateRequest(UPDATE_OBJECT_METHOD, request);
        validateObjectType(request.getObjectType());

        return writeService.updateObjects(
                request.getTenant(),
                Collections.singletonList(request),
                Collections.emptyList()).get(0);
    }

    private void validateObjectType(ObjectType objectType) {
        if (apiTrustLevel == PUBLIC_API && !PUBLIC_WRITABLE_OBJECT_TYPES.contains(objectType)) {
            var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
            var status = Status.PERMISSION_DENIED.withDescription(message);
            throw status.asRuntimeException();
        }
    }

    TagHeader updateTag(MetadataWriteRequest request) {

        validateRequest(UPDATE_TAG_METHOD, request);

        return writeService.updateTagBatch(
                request.getTenant(),
                Collections.singletonList(request),
                Collections.emptyList()).get(0);
    }

    TagHeader preallocateId(MetadataWriteRequest request) {

        validateRequest(PREALLOCATE_ID_METHOD, request);

        var tenant = request.getTenant();
        var objectType = request.getObjectType();

        return writeService.preallocateIdBatch(tenant, Collections.singletonList(objectType)).get(0);
    }

    MetadataWriteBatchResponse preallocateIdBatch(MetadataWriteBatchRequest request) {

        validateRequest(PREALLOCATE_ID_BATCH_METHOD, request);

        var tenant = request.getTenant();
        var objectTypes = request.getRequestsList().stream()
                .map(MetadataWriteRequest::getObjectType)
                .collect(Collectors.toList());

        var tagHeaders = writeService.preallocateIdBatch(tenant, objectTypes);
        return MetadataWriteBatchResponse.newBuilder()
                .addAllHeaders(tagHeaders)
                .build();
    }

    TagHeader createPreallocatedObject(MetadataWriteRequest request) {

        validateRequest(CREATE_PREALLOCATED_OBJECT_METHOD, request);

        return writeService.createPreallocatedObjectBatch(
                request.getTenant(),
                Collections.singletonList(request),
                Collections.emptyList()).get(0);
    }

    private void validateListForObjectType(List<MetadataWriteRequest> requestsList) {
        for (var rq : requestsList) {
            validateObjectType(rq.getObjectType());
        }

    }

    public UniversalMetadataWriteBatchResponse writeBatch(UniversalMetadataWriteBatchRequest request) {

        validateRequest(TrustedMetadataApiGrpc.getWriteBatchMethod(), request);

        validateListForObjectType(request.getPreallocateObjectsList());
        validateListForObjectType(request.getCreateObjectsList());
        validateListForObjectType(request.getUpdateObjectsList());
        validateListForObjectType(request.getPreallocateObjectsList());
        validateListForObjectType(request.getPreallocateIdsList());

        return writeService.writeBatch(request);
    }

    Tag readObject(MetadataReadRequest request) {

        validateRequest(READ_OBJECT_METHOD, request);

        return readService.readObject(request.getTenant(), request.getSelector());
    }

    MetadataBatchResponse readBatch(MetadataBatchRequest request) {

        validateRequest(READ_BATCH_METHOD, request);

        var tags = readService.readObjects(request.getTenant(), request.getSelectorList());
        return MetadataBatchResponse.newBuilder().addAllTag(tags).build();
    }

    MetadataSearchResponse search(MetadataSearchRequest request) {

        validateRequest(SEARCH_METHOD, request);

        var tenant = request.getTenant();
        var searchParams = request.getSearchParams();

        var searchResult = searchService.search(tenant, searchParams);

        return MetadataSearchResponse.newBuilder()
                .addAllSearchResult(searchResult)
                .build();
    }

    Tag getObject(MetadataGetRequest request) {

        validateRequest(GET_OBJECT_METHOD, request);

        var tenant = request.getTenant();
        var objectType = request.getObjectType();
        var objectId = UUID.fromString(request.getObjectId());
        var objectVersion = request.getObjectVersion();
        var tagVersion = request.getTagVersion();

        return readService.loadTag(tenant, objectType, objectId, objectVersion, tagVersion);
    }

    Tag getLatestObject(MetadataGetRequest request) {

        validateRequest(GET_LATEST_OBJECT_METHOD, request);

        var tenant = request.getTenant();
        var objectType = request.getObjectType();
        var objectId = UUID.fromString(request.getObjectId());

        return readService.loadLatestObject(tenant, objectType, objectId);
    }

    Tag getLatestTag(MetadataGetRequest request) {

        validateRequest(GET_LATEST_TAG_METHOD, request);

        var tenant = request.getTenant();
        var objectType = request.getObjectType();
        var objectId = UUID.fromString(request.getObjectId());
        var objectVersion = request.getObjectVersion();

        return readService.loadLatestTag(tenant, objectType, objectId, objectVersion);
    }

    private <TReq extends Message>
    void validateRequest(MethodDescriptor<TReq, ?> method, TReq request) {

        var protoMethod = serviceDescriptor.findMethodByName(method.getBareMethodName());

        validator.validateFixedMethod(request, protoMethod);
    }

}
