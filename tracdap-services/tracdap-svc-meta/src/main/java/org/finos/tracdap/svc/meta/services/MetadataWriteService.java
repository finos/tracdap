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
import java.util.List;
import java.util.UUID;

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

        var objectId = UUID.randomUUID();

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

        dal.saveNewObject(tenant, newTag);

        return newHeader;
    }


    public TagHeader updateObject(
            String tenant, TagSelector priorVersion,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        var userInfo = AuthConstants.USER_INFO_KEY.get();

        var priorTag = dal.loadObject(tenant, priorVersion);

        return updateObject(tenant, userInfo, priorTag, definition, tagUpdates);
    }

    private TagHeader updateObject(
            String tenant, UserInfo userInfo, Tag priorTag,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

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

        dal.saveNewVersion(tenant, newTag);

        return newHeader;
    }

    public TagHeader updateTag(
            String tenant, TagSelector priorVersion,
            List<TagUpdate> tagUpdates) {

        var priorTag = dal.loadObject(tenant, priorVersion);

        return updateTag(tenant, priorTag, tagUpdates);
    }

    private TagHeader updateTag(
            String tenant, Tag priorTag,
            List<TagUpdate> tagUpdates) {

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

        dal.saveNewTag(tenant, newTag);

        return newHeader;
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

    public TagHeader createPreallocatedObject(
            String tenant, TagSelector priorVersion,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        // In this case priorVersion refers to the preallocated ID
        var objectId = UUID.fromString(priorVersion.getObjectId());

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

        dal.savePreallocatedObject(tenant, newTag);

        return newHeader;
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
