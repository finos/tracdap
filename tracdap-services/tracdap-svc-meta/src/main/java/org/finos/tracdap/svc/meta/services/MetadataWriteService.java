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

package org.finos.tracdap.svc.meta.services;

import io.grpc.Context;
import org.finos.tracdap.api.MetadataWriteBatchRequest;
import org.finos.tracdap.api.MetadataWriteBatchResponse;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.dal.IMetadataDal;
import org.finos.tracdap.common.metadata.dal.MetadataBatchUpdate;
import org.finos.tracdap.common.metadata.tag.ObjectUpdateLogic;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


public class MetadataWriteService {

    private final Validator validator = new Validator();
    private final IMetadataDal metadataStore;

    public MetadataWriteService(IMetadataDal metadataStore) {
        this.metadataStore = metadataStore;
    }

    public TagHeader preallocateId(String tenant, MetadataWriteRequest request) {

        var newIds = processPreallocatedIds(List.of(request));

        metadataStore.savePreallocatedIds(tenant, newIds);

        return newIds.get(0);
    }

    public TagHeader createPreallocatedObject(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        var preallocatedObjects = processPreallocatedObjects(List.of(request), requestMetadata, userMetadata);

        metadataStore.savePreallocatedObjects(tenant, preallocatedObjects);

        return preallocatedObjects.get(0).getHeader();
    }

    public TagHeader createObject(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        var newObjects = processNewObjects(List.of(request), requestMetadata, userMetadata);

        metadataStore.saveNewObjects(tenant, newObjects);

        return newObjects.get(0).getHeader();
    }

    public TagHeader updateObject(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        var newVersions = processNewVersions(tenant, List.of(request), requestMetadata, userMetadata);

        metadataStore.saveNewVersions(tenant, newVersions);

        return newVersions.get(0).getHeader();
    }

    public TagHeader updateTag(String tenant, MetadataWriteRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        var newTags = processNewTags(tenant, List.of(request), requestMetadata, userMetadata);

        metadataStore.saveNewTags(tenant, newTags);

        return newTags.get(0).getHeader();
    }

    public MetadataWriteBatchResponse writeBatch(MetadataWriteBatchRequest request) {

        // Get request and user metadata from the current gRPC context
        var requestMetadata = RequestMetadata.get(Context.current());
        var userMetadata = UserMetadata.get(Context.current());

        var tenant = request.getTenant();

        var preallocatedIds = processPreallocatedIds(request.getPreallocateIdsList());
        var preallocatedObjects = processPreallocatedObjects(request.getCreatePreallocatedObjectsList(), requestMetadata, userMetadata);
        var newObjects = processNewObjects(request.getCreateObjectsList(), requestMetadata, userMetadata);
        var newVersions = processNewVersions(tenant, request.getUpdateObjectsList(), requestMetadata, userMetadata);
        var newTags = processNewTags(tenant, request.getUpdateTagsList(), requestMetadata, userMetadata);

        var batchUpdate = new MetadataBatchUpdate(
                preallocatedIds, preallocatedObjects,
                newObjects, newVersions, newTags);

        metadataStore.saveBatchUpdate(request.getTenant(), batchUpdate);

        var preallocatedObjectIds = preallocatedObjects.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newObjectIds = newObjects.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newVersionIds = newVersions.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newTagIds = newTags.stream().map(Tag::getHeader).collect(Collectors.toList());

        return MetadataWriteBatchResponse.newBuilder()
                .addAllPreallocateIds(preallocatedIds)
                .addAllCreatePreallocatedObjects(preallocatedObjectIds)
                .addAllCreateObjects(newObjectIds)
                .addAllUpdateObjects(newVersionIds)
                .addAllUpdateTags(newTagIds)
                .build();
    }

    private List<TagHeader> processPreallocatedIds(List<MetadataWriteRequest> requests) {

        var preallocatedIds = new ArrayList<TagHeader>(requests.size());

        for (var request : requests) {

            // Assigning object IDs could be moved to a central function and logged
            // There's nothing special about them though, so this is fine for now
            var objectId = UUID.randomUUID();

            var preallocatedId = TagHeader.newBuilder()
                    .setObjectType(request.getObjectType())
                    .setObjectId(objectId.toString())
                    .build();

            preallocatedIds.add(preallocatedId);
        }

        return preallocatedIds;
    }

    private List<Tag> processPreallocatedObjects(
            List<MetadataWriteRequest> requests,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var preallocatedObjects = new ArrayList<Tag>(requests.size());

        for (var request : requests) {

            var objectId = UUID.fromString(request.getPriorVersion().getObjectId());

            var preallocatedObject = ObjectUpdateLogic.buildNewObject(
                    objectId, request.getDefinition(), request.getTagUpdatesList(),
                    requestMetadata, userMetadata);

            preallocatedObjects.add(preallocatedObject);
        }

        return preallocatedObjects;
    }

    private List<Tag> processNewObjects(
            List<MetadataWriteRequest> requests,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var newObjects = new ArrayList<Tag>(requests.size());

        for (var request : requests) {

            // Assigning object IDs could be moved to a central function and logged
            // There's nothing special about them though, so this is fine for now
            var objectId = UUID.randomUUID();

            var newObject = ObjectUpdateLogic.buildNewObject(
                    objectId, request.getDefinition(), request.getTagUpdatesList(),
                    requestMetadata, userMetadata);

            newObjects.add(newObject);
        }

        return newObjects;
    }

    private List<Tag> processNewVersions(
            String tenant, List<MetadataWriteRequest> requests,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        // Do not query the DAL if there are no requests
        if (requests.isEmpty())
            return List.of();

        var priorIds = requests.stream().map(MetadataWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorVersions = metadataStore.loadPriorObjects(tenant, priorIds);

        var newVersions = new ArrayList<Tag>(requests.size());

        for (var i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var priorVersion = priorVersions.get(i);

            // TODO: Apply the version validator in bulk across a batch of updates
            // Will need an update in the validator, currently 50 object updates -> 50 separate validation passes

            validator.validateVersion(
                    request.getDefinition(),
                    priorVersion.getDefinition());

            var newObject = ObjectUpdateLogic.buildNewVersion(
                    priorVersion, request.getDefinition(), request.getTagUpdatesList(),
                    requestMetadata, userMetadata);

            newVersions.add(newObject);
        }

        return newVersions;
    }

    private List<Tag> processNewTags(
            String tenant, List<MetadataWriteRequest> requests,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        // Do not query the DAL if there are no requests
        if (requests.isEmpty())
            return List.of();

        var priorIds = requests.stream().map(MetadataWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorTags = metadataStore.loadPriorTags(tenant, priorIds);

        var newTags = new ArrayList<Tag>(requests.size());

        for (var i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var priorTag = priorTags.get(i);

            var newTag = ObjectUpdateLogic.buildNewTag(
                    priorTag, request.getTagUpdatesList(),
                    requestMetadata, userMetadata);

            newTags.add(newTag);
        }

        return newTags;
    }
}
