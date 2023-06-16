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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.api.MetadataWriteBatchRequest;
import org.finos.tracdap.api.MetadataWriteBatchResponse;
import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.auth.internal.AuthHelpers;
import org.finos.tracdap.common.auth.internal.UserInfo;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;
import org.finos.tracdap.svc.meta.dal.MetadataBatchUpdate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.metadata.MetadataConstants.OBJECT_FIRST_VERSION;
import static org.finos.tracdap.common.metadata.MetadataConstants.TAG_FIRST_VERSION;


public class MetadataWriteService {

    private final Validator validator = new Validator();
    private final IMetadataDal dal;

    public MetadataWriteService(IMetadataDal dal) {
        this.dal = dal;
    }

    public TagHeader preallocateId(String tenant, MetadataWriteRequest request) {

        var newIds = processPreallocatedIds(List.of(request));

        dal.savePreallocatedIds(tenant, newIds);

        return newIds.get(0);
    }

    public TagHeader createPreallocatedObject(String tenant, MetadataWriteRequest request) {

        var userInfo = AuthHelpers.currentUser();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var preallocatedObjects = processPreallocatedObjects(List.of(request), userInfo, timestamp);

        dal.savePreallocatedObjects(tenant, preallocatedObjects);

        return preallocatedObjects.get(0).getHeader();
    }

    public TagHeader createObject(String tenant, MetadataWriteRequest request) {

        var userInfo = AuthHelpers.currentUser();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var newObjects = processNewObjects(List.of(request), userInfo, timestamp);

        dal.saveNewObjects(tenant, newObjects);

        return newObjects.get(0).getHeader();
    }

    public TagHeader updateObject(String tenant, MetadataWriteRequest request) {

        var userInfo = AuthHelpers.currentUser();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var newVersions = processNewVersions(tenant, List.of(request), userInfo, timestamp);

        dal.saveNewVersions(tenant, newVersions);

        return newVersions.get(0).getHeader();
    }

    public TagHeader updateTag(String tenant, MetadataWriteRequest request) {

        var userInfo = AuthHelpers.currentUser();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var newTags = processNewTags(tenant, List.of(request), userInfo, timestamp);

        dal.saveNewTags(tenant, newTags);

        return newTags.get(0).getHeader();
    }

    public MetadataWriteBatchResponse writeBatch(MetadataWriteBatchRequest request) {

        var tenant = request.getTenant();

        var userInfo = AuthHelpers.currentUser();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var preallocatedIds = processPreallocatedIds(request.getPreallocateIdsList());
        var preallocatedObjects = processPreallocatedObjects(request.getCreatePreallocatedList(), userInfo, timestamp);
        var newObjects = processNewObjects(request.getCreateObjectsList(), userInfo, timestamp);
        var newVersions = processNewVersions(tenant, request.getUpdateObjectsList(), userInfo, timestamp);
        var newTags = processNewTags(tenant, request.getUpdateTagsList(), userInfo, timestamp);

        var batchUpdate = new MetadataBatchUpdate(
                preallocatedIds, preallocatedObjects,
                newObjects, newVersions, newTags);

        dal.saveBatchUpdate(request.getTenant(), batchUpdate);

        var preallocatedObjectIds = preallocatedObjects.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newObjectIds = newObjects.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newVersionIds = newVersions.stream().map(Tag::getHeader).collect(Collectors.toList());
        var newTagIds = newTags.stream().map(Tag::getHeader).collect(Collectors.toList());

        return MetadataWriteBatchResponse.newBuilder()
                .addAllPreallocateIds(preallocatedIds)
                .addAllCreatePreallocated(preallocatedObjectIds)
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
            UserInfo userInfo, OffsetDateTime timestamp) {

        var preallocatedObjects = new ArrayList<Tag>(requests.size());

        for (var request : requests) {

            var objectId = UUID.fromString(request.getPriorVersion().getObjectId());

            var preallocatedObject = buildNewObject(
                    objectId,
                    request.getDefinition(),
                    request.getTagUpdatesList(),
                    userInfo,
                    timestamp);

            preallocatedObjects.add(preallocatedObject);
        }

        return preallocatedObjects;
    }

    private List<Tag> processNewObjects(
            List<MetadataWriteRequest> requests,
            UserInfo userInfo, OffsetDateTime timestamp) {

        var newObjects = new ArrayList<Tag>(requests.size());

        for (var request : requests) {

            // Assigning object IDs could be moved to a central function and logged
            // There's nothing special about them though, so this is fine for now
            var objectId = UUID.randomUUID();

            var newObject = buildNewObject(
                    objectId,
                    request.getDefinition(),
                    request.getTagUpdatesList(),
                    userInfo,
                    timestamp);

            newObjects.add(newObject);
        }

        return newObjects;
    }

    private List<Tag> processNewVersions(
            String tenant, List<MetadataWriteRequest> requests,
            UserInfo userInfo, OffsetDateTime timestamp) {

        // Do not query the DAL if there are no requests
        if (requests.isEmpty())
            return List.of();

        var priorIds = requests.stream().map(MetadataWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorVersions = dal.loadPriorObjects(tenant, priorIds);

        var newVersions = new ArrayList<Tag>(requests.size());

        for (var i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var priorVersion = priorVersions.get(i);

            // TODO: Apply the version validator in bulk across a batch of updates
            // Will need an update in the validator, currently 50 object updates -> 50 separate validation passes

            validator.validateVersion(
                    request.getDefinition(),
                    priorVersion.getDefinition());

            var newObject = buildNewVersion(
                    priorVersion,
                    request.getDefinition(),
                    request.getTagUpdatesList(),
                    userInfo,
                    timestamp);

            newVersions.add(newObject);
        }

        return newVersions;
    }

    private List<Tag> processNewTags(
            String tenant, List<MetadataWriteRequest> requests,
            UserInfo userInfo, OffsetDateTime timestamp) {

        // Do not query the DAL if there are no requests
        if (requests.isEmpty())
            return List.of();

        var priorIds = requests.stream().map(MetadataWriteRequest::getPriorVersion).collect(Collectors.toList());
        var priorTags = dal.loadPriorTags(tenant, priorIds);

        var newTags = new ArrayList<Tag>(requests.size());

        for (var i = 0; i < requests.size(); i++) {

            var request = requests.get(i);
            var priorTag = priorTags.get(i);

            var newTag = buildNewTag(
                    priorTag,
                    request.getTagUpdatesList(),
                    userInfo,
                    timestamp);

            newTags.add(newTag);
        }

        return newTags;
    }

    private Tag buildNewObject(
            UUID objectId, ObjectDefinition definition, List<TagUpdate> tagUpdates,
            UserInfo userInfo, OffsetDateTime timestamp) {

        var newHeader = TagHeader.newBuilder()
                .setObjectType(definition.getObjectType())
                .setObjectId(objectId.toString())
                .setObjectVersion(OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setIsLatestTag(true)
                .setIsLatestObject(true)
                .build();

        var newTag = Tag.newBuilder()
                .setHeader(newHeader)
                .setDefinition(definition)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);

        // Apply the common controlled trac_ tags for newly created objects

        var userId = userInfo.getUserId();
        var userName = userInfo.getDisplayName();
        var createAttrs = commonCreateAttrs(timestamp, userId, userName);
        var updateAttrs = commonUpdateAttrs(timestamp, userId, userName);

        newTag = TagUpdateService.applyTagUpdates(newTag, createAttrs);
        newTag = TagUpdateService.applyTagUpdates(newTag, updateAttrs);

        return newTag;
    }

    private Tag buildNewVersion(
            Tag priorTag, ObjectDefinition definition, List<TagUpdate> tagUpdates,
            UserInfo userInfo, OffsetDateTime timestamp) {

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setObjectVersion(oldHeader.getObjectVersion() + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setIsLatestTag(true)
                .setIsLatestObject(true)
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .setDefinition(definition)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);

        // Apply the common controlled trac_ tags for updated objects

        var userId = userInfo.getUserId();
        var userName = userInfo.getDisplayName();
        var commonAttrs = commonUpdateAttrs(timestamp, userId, userName);

        newTag = TagUpdateService.applyTagUpdates(newTag, commonAttrs);

        return newTag;
    }

    private static Tag buildNewTag(
            Tag priorTag, List<TagUpdate> tagUpdates,
            UserInfo userInfo, OffsetDateTime timestamp) {

        // TODO: Record user info for tag-only updates
        // Audit history for object revisions is most important
        // Audit for tag updates will be needed too at some point

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setTagVersion(oldHeader.getTagVersion() + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setIsLatestTag(true)
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);

        return newTag;
    }

    private static List<TagUpdate> commonCreateAttrs(
            OffsetDateTime createTime,
            String createUserId,
            String createUserName) {

        var createTimeAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_CREATE_TIME)
                .setValue(MetadataCodec.encodeValue(createTime))
                .build();

        var createUserIdAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_CREATE_USER_ID)
                .setValue(MetadataCodec.encodeValue(createUserId))
                .build();

        var createUserNameAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_CREATE_USER_NAME)
                .setValue(MetadataCodec.encodeValue(createUserName))
                .build();

        return List.of(createTimeAttr, createUserIdAttr, createUserNameAttr);
    }

    private static List<TagUpdate> commonUpdateAttrs(
            OffsetDateTime createTime,
            String createUserId,
            String createUserName) {

        var updateTimeAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_UPDATE_TIME)
                .setValue(MetadataCodec.encodeValue(createTime))
                .build();

        var updateUserIdAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_UPDATE_USER_ID)
                .setValue(MetadataCodec.encodeValue(createUserId))
                .build();

        var updateUserNameAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_UPDATE_USER_NAME)
                .setValue(MetadataCodec.encodeValue(createUserName))
                .build();

        return List.of(updateTimeAttr, updateUserIdAttr, updateUserNameAttr);
    }
}
