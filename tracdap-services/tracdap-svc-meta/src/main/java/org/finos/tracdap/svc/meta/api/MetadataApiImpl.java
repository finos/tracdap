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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.finos.tracdap.common.metadata.MetadataConstants.PUBLIC_WRITABLE_OBJECT_TYPES;
import static org.finos.tracdap.svc.meta.api.TracMetadataApi.*;
import static org.finos.tracdap.svc.meta.api.TrustedMetadataApi.CREATE_PREALLOCATED_OBJECT_METHOD;
import static org.finos.tracdap.svc.meta.api.TrustedMetadataApi.PREALLOCATE_ID_METHOD;
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

        var objectType = request.getObjectType();

        if (apiTrustLevel == PUBLIC_API && !PUBLIC_WRITABLE_OBJECT_TYPES.contains(objectType)) {
            var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
            var status = Status.PERMISSION_DENIED.withDescription(message);
            throw status.asRuntimeException();
        }

        return writeService.createObject(
                request.getTenant(),
                request.getDefinition(),
                request.getTagUpdatesList());
    }

    MetadataWriteBatchResponse createBatch(MetadataWriteBatchRequest request) {

        validateRequest(CREATE_BATCH_METHOD, request);

        return runWriteBatch(request, MetadataApiImpl::createObject);
    }

    TagHeader updateObject(MetadataWriteRequest request) {

        validateRequest(UPDATE_OBJECT_METHOD, request);

        var objectType = request.getObjectType();

        if (apiTrustLevel == PUBLIC_API && !PUBLIC_WRITABLE_OBJECT_TYPES.contains(objectType)) {
            var message = String.format("Object type %s cannot be created via the TRAC public API", objectType);
            var status = Status.PERMISSION_DENIED.withDescription(message);
            throw status.asRuntimeException();
        }

        return writeService.updateObject(
                request.getTenant(),
                request.getPriorVersion(),
                request.getDefinition(),
                request.getTagUpdatesList());
    }

    MetadataWriteBatchResponse updateBatch(MetadataWriteBatchRequest request) {

        validateRequest(UPDATE_BATCH_METHOD, request);

        return runWriteBatch(request, MetadataApiImpl::updateObject);
    }

    TagHeader updateTag(MetadataWriteRequest request) {

        validateRequest(UPDATE_TAG_METHOD, request);

        return writeService.updateTag(
                request.getTenant(),
                request.getPriorVersion(),
                request.getTagUpdatesList());
    }

    MetadataWriteBatchResponse updateBatchTag(MetadataWriteBatchRequest request) {

        validateRequest(UPDATE_BATCH_TAG_METHOD, request);

        return runWriteBatch(request, MetadataApiImpl::updateTag);
    }

    TagHeader preallocateId(MetadataWriteRequest request) {

        validateRequest(PREALLOCATE_ID_METHOD, request);

        var tenant = request.getTenant();
        var objectType = request.getObjectType();

        return writeService.preallocateId(tenant, objectType);
    }

    TagHeader createPreallocatedObject(MetadataWriteRequest request) {

        validateRequest(CREATE_PREALLOCATED_OBJECT_METHOD, request);

        return writeService.createPreallocatedObject(
                request.getTenant(),
                request.getPriorVersion(),
                request.getDefinition(),
                request.getTagUpdatesList());
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

    private MetadataWriteBatchResponse runWriteBatch(
            MetadataWriteBatchRequest request,
            BiFunction<MetadataApiImpl, MetadataWriteRequest, TagHeader> operation
    ) {
        List<TagUpdate> tags = request.getBatchTagUpdatesList();
        List<TagHeader> tagHeaders = new ArrayList<>();

        for (MetadataWriteRequest rawRequest : request.getRequestsList()) {
            MetadataWriteRequest r = addTagsToRequest(rawRequest, tags);

            TagHeader tagHeader = operation.apply(this, r);

            tagHeaders.add(tagHeader);
        }

        MetadataWriteBatchResponse.Builder builder = MetadataWriteBatchResponse.newBuilder();
        builder.addAllIds(tagHeaders);
        return builder.build();
    }

    private MetadataWriteRequest addTagsToRequest(MetadataWriteRequest rawRequest, List<TagUpdate> tags) {
        MetadataWriteRequest.Builder builder = MetadataWriteRequest.newBuilder(rawRequest);
        builder.addAllTagUpdates(tags);
        return builder.build();
    }

    private <TReq extends Message>
    void validateRequest(MethodDescriptor<TReq, ?> method, TReq request) {

        var protoMethod = serviceDescriptor.findMethodByName(method.getBareMethodName());

        validator.validateFixedMethod(request, protoMethod);
    }
}
