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

import org.finos.tracdap.api.MetadataWriteRequest;
import org.finos.tracdap.common.auth.AuthConstants;
import org.finos.tracdap.common.auth.UserInfo;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
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

    public TagHeader createObject(
            String tenant,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        var newTag = prepareCreateObject(UUID.randomUUID(), definition, tagUpdates);

        dal.saveNewObjects(tenant, Collections.singletonList(newTag));

        return newTag.getHeader();
    }

    public List<TagHeader> createObjects(
            String tenant,
            List<MetadataWriteRequest> requests,
            List<TagUpdate> batchTagUpdates) {
        var newTags = new ArrayList<Tag>();
        for (var request : requests) {

            var tag = prepareCreateObject(
                    UUID.randomUUID(),
                    request.getDefinition(),
                    getTagUpdatesInsideBatch(request, batchTagUpdates)
            );
            newTags.add(tag);
        }

        dal.saveNewObjects(tenant, newTags);

        return newTags.stream().map(Tag::getHeader).collect(Collectors.toList());
    }

    private Tag prepareCreateObject(UUID objectId, ObjectDefinition definition, List<TagUpdate> tagUpdates) {

        var userInfo = AuthConstants.USER_INFO_KEY.get();
        var userId = userInfo.getUserId();
        var userName = userInfo.getDisplayName();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var newHeader = TagHeader.newBuilder()
                .setObjectType(definition.getObjectType())
                .setObjectId(objectId.toString())
                .setObjectVersion(OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        var newTag = Tag.newBuilder()
                .setHeader(newHeader)
                .setDefinition(definition)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);

        // Apply the common controlled trac_ tags for newly created objects
        var createAttrs = commonCreateAttrs(timestamp, userId, userName);
        var updateAttrs = commonUpdateAttrs(timestamp, userId, userName);
        newTag = TagUpdateService.applyTagUpdates(newTag, createAttrs);
        newTag = TagUpdateService.applyTagUpdates(newTag, updateAttrs);

        return newTag;
    }

    public TagHeader updateObject(
            String tenant, TagSelector priorVersion,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        var userInfo = AuthConstants.USER_INFO_KEY.get();

        var priorTag = dal.loadObject(tenant, priorVersion);

        var newTag = prepareUpdateObject(userInfo, priorTag, definition, tagUpdates);

        dal.saveNewVersions(tenant, Collections.singletonList(newTag));

        return newTag.getHeader();
    }

    public List<TagHeader> updateObjects(
            String tenant,
            List<MetadataWriteRequest> requests,
            List<TagUpdate> batchTagUpdates) {

        var userInfo = AuthConstants.USER_INFO_KEY.get();

        var priorVersions = requests.stream()
                .map(MetadataWriteRequest::getPriorVersion)
                .collect(Collectors.toList());
        var priorTags = dal.loadObjects(tenant, priorVersions);

        var newTags = new ArrayList<Tag>();
        for (int i = 0; i < requests.size(); i++) {
            var request = requests.get(i);

            var newTag = prepareUpdateObject(
                    userInfo,
                    priorTags.get(i),
                    request.getDefinition(),
                    getTagUpdatesInsideBatch(request, batchTagUpdates)
            );
            newTags.add(newTag);
        }

        dal.saveNewVersions(tenant, newTags);

        return newTags.stream().map(Tag::getHeader).collect(Collectors.toList());
    }

    private Tag prepareUpdateObject(UserInfo userInfo, Tag priorTag, ObjectDefinition definition, List<TagUpdate> tagUpdates) {
        // Validate version increment on the object
        validator.validateVersion(definition, priorTag.getDefinition());

        var userId = userInfo.getUserId();
        var userName = userInfo.getDisplayName();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setObjectVersion(oldHeader.getObjectVersion() + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .setDefinition(definition)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);

        // Apply the common controlled trac_ tags for updated objects
        var commonAttrs = commonUpdateAttrs(timestamp, userId, userName);
        newTag = TagUpdateService.applyTagUpdates(newTag, commonAttrs);

        return newTag;
    }

    public TagHeader updateTag(
            String tenant, TagSelector priorVersion,
            List<TagUpdate> tagUpdates) {

        var priorTag = dal.loadObject(tenant, priorVersion);

        var newTag = prepareUpdateTag(priorTag, tagUpdates);

        dal.saveNewTags(tenant, Collections.singletonList(newTag));

        return newTag.getHeader();
    }

    public List<TagHeader> updateTagBatch(
            String tenant,
            List<MetadataWriteRequest> requests,
            List<TagUpdate> batchTagUpdates) {

        var priorVersions = requests.stream()
                .map(MetadataWriteRequest::getPriorVersion)
                .collect(Collectors.toList());
        var priorTags = dal.loadObjects(tenant, priorVersions);

        var newTags = new ArrayList<Tag>();
        for (int i = 0; i < requests.size(); i++) {
            var request = requests.get(i);

            var tag = prepareUpdateTag(
                    priorTags.get(i),
                    getTagUpdatesInsideBatch(request, batchTagUpdates)
            );
            newTags.add(tag);
        }

        dal.saveNewTags(tenant, newTags);

        return newTags.stream().map(Tag::getHeader).collect(Collectors.toList());
    }

    private static Tag prepareUpdateTag(Tag priorTag, List<TagUpdate> tagUpdates) {
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setTagVersion(oldHeader.getTagVersion() + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);
        return newTag;
    }

    public TagHeader preallocateId(String tenant, ObjectType objectType) {

        // New random ID
        var objectId = UUID.randomUUID();

        // Header for preallocated IDs does not include an object or tag version
        var preallocatedHeader = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .build();

        // Save as a preallocated ID in the DAL
        dal.preallocateObjectId(tenant, objectType, objectId);

        return preallocatedHeader;
    }

    public List<TagHeader> preallocateIdBatch(String tenant, List<ObjectType> objectTypes) {
        var objectIds = objectTypes.stream().map(objectType -> UUID.randomUUID()).collect(Collectors.toList());

        dal.preallocateObjectIds(tenant, objectTypes, objectIds);

        var tagHeaders = new ArrayList<TagHeader>();
        for (int i = 0; i < objectTypes.size(); i++) {
            var tagHeader = TagHeader.newBuilder()
                    .setObjectType(objectTypes.get(i))
                    .setObjectId(objectIds.get(i).toString())
                    .build();
            tagHeaders.add(tagHeader);
        }
        return tagHeaders;
    }

    public TagHeader createPreallocatedObject(
            String tenant, TagSelector priorVersion,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        // In this case priorVersion refers to the preallocated ID
        var objectId = UUID.fromString(priorVersion.getObjectId());

        var newTag = prepareCreateObject(
                objectId,
                definition,
                tagUpdates
        );

        dal.savePreallocatedObject(tenant, newTag);

        return newTag.getHeader();
    }

    public List<TagHeader> createPreallocatedObjectBatch(
            String tenant,
            List<MetadataWriteRequest> requests,
            List<TagUpdate> batchTagUpdates) {

        var tags = new ArrayList<Tag>();
        for (var request : requests) {
            var tag = prepareCreateObject(
                    UUID.fromString(request.getPriorVersion().getObjectId()),
                    request.getDefinition(),
                    getTagUpdatesInsideBatch(request, batchTagUpdates)
            );
            tags.add(tag);
        }

        dal.savePreallocatedObjects(tenant, tags);

        return tags.stream().map(Tag::getHeader).collect(Collectors.toList());
    }

    private static List<TagUpdate> getTagUpdatesInsideBatch(MetadataWriteRequest r, List<TagUpdate> batchTagUpdates) {
        var tagUpdates = new ArrayList<>(r.getTagUpdatesList());
        tagUpdates.addAll(batchTagUpdates);

        return tagUpdates;
    }

    private List<TagUpdate> commonCreateAttrs(
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

    private List<TagUpdate> commonUpdateAttrs(
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
