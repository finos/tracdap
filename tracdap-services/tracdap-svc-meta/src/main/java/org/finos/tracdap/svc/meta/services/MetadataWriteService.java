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
import org.finos.tracdap.svc.meta.dal.operations.*;

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

    public TagHeader createObject(String tenant, MetadataWriteRequest request) {

        return executeWriteOperation(
                tenant,
                createObjectsWriteOperation(List.of(request))
        ).get(0);
    }

    public TagHeader updateObject(String tenant, MetadataWriteRequest request) {

        return executeWriteOperation(
                tenant,
                updateObjectsWriteOperation(tenant, List.of(request))
        ).get(0);
    }



    public TagHeader updateTag(String tenant, MetadataWriteRequest request) {

        return executeWriteOperation(
                tenant,
                updateTagsWriteOperation(tenant, List.of(request))
        ).get(0);
    }

    public TagHeader preallocateId(String tenant, ObjectType objectType) {

        var objectId = UUID.randomUUID();

        dal.preallocateObjectIds(tenant, List.of(objectType), List.of(objectId));

        return TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .build();
    }

    public TagHeader createPreallocatedObject(String tenant, MetadataWriteRequest request) {

        return executeWriteOperation(
                tenant,
                createPreallocatedObjectsWriteOperation(List.of(request))
        ).get(0);
    }

    public MetadataWriteBatchResponse writeBatch(MetadataWriteBatchRequest request) {

        var tenant = request.getTenant();
        var writeOperations = new ArrayList<WriteOperation>();

        var resultBuilder = MetadataWriteBatchResponse.newBuilder();

        if (request.getCreatePreallocatedCount() > 0) {
            var requests = request.getCreatePreallocatedList();
            var opers = createPreallocatedObjectsWriteOperation(requests);
            resultBuilder.addAllCreatePreallocated(opers.tagHeaders);
            writeOperations.add(opers);
        }

        if (request.getCreateObjectsCount() > 0) {
            var requests = request.getCreateObjectsList();
            var opers = createObjectsWriteOperation(requests);
            resultBuilder.addAllCreateObjects(opers.tagHeaders);
            writeOperations.add(opers);
        }

        if (request.getUpdateObjectsCount() > 0) {
            var requests = request.getUpdateObjectsList();
            var opers = updateObjectsWriteOperation(tenant, requests);
            resultBuilder.addAllUpdateObjects(opers.tagHeaders);
            writeOperations.add(opers);
        }

        if (request.getUpdateTagsCount() > 0) {
            var requests = request.getUpdateTagsList();
            var opers = updateTagsWriteOperation(tenant, requests);
            resultBuilder.addAllUpdateTags(opers.tagHeaders);
            writeOperations.add(opers);
        }

        dal.runWriteOperations(
                tenant,
                writeOperations.stream().map(w -> w.writeOperation).collect(Collectors.toList())
        );

        return resultBuilder.build();
    }

    private List<TagHeader> executeWriteOperation(String tenant, WriteOperation oper) {
        dal.runWriteOperations(tenant, Collections.singletonList(oper.writeOperation));
        return oper.tagHeaders;
    }

    private WriteOperation createObjectsWriteOperation(List<MetadataWriteRequest> requests) {
        var newTags = new ArrayList<Tag>();
        for (var request : requests) {

            var tag = prepareCreateObject(
                    UUID.randomUUID(),
                    request.getDefinition(),
                    request.getTagUpdatesList()
            );
            newTags.add(tag);
        }

        var result = new WriteOperation();
        result.tagHeaders = newTags.stream().map(Tag::getHeader).collect(Collectors.toList());
        result.writeOperation = new SaveNewObject(newTags);
        return result;
    }

    private Tag prepareCreateObject(UUID objectId, ObjectDefinition definition, List<TagUpdate> tagUpdates) {

        var userInfo = AuthHelpers.currentUser();
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
                .setIsLatestTag(true)
                .setIsLatestObject(true)
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

    private WriteOperation updateObjectsWriteOperation(String tenant, List<MetadataWriteRequest> requests) {

        var userInfo = AuthHelpers.currentUser();

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
                    request.getTagUpdatesList()
            );
            newTags.add(newTag);
        }

        var result = new WriteOperation();
        result.tagHeaders = newTags.stream().map(Tag::getHeader).collect(Collectors.toList());
        result.writeOperation = new SaveNewVersion(newTags);
        return result;
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
                .setIsLatestTag(true)
                .setIsLatestObject(true)
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

    private WriteOperation updateTagsWriteOperation(String tenant, List<MetadataWriteRequest> requests) {

        var priorVersions = requests.stream()
                .map(MetadataWriteRequest::getPriorVersion)
                .collect(Collectors.toList());
        var priorTags = dal.loadObjects(tenant, priorVersions);

        var newTags = new ArrayList<Tag>();
        for (int i = 0; i < requests.size(); i++) {
            var request = requests.get(i);

            var tag = prepareUpdateTag(
                    priorTags.get(i),
                    request.getTagUpdatesList()
            );
            newTags.add(tag);
        }


        var result = new WriteOperation();
        result.tagHeaders = newTags.stream().map(Tag::getHeader).collect(Collectors.toList());
        result.writeOperation = new SaveNewTag(newTags);
        return result;
    }

    private static Tag prepareUpdateTag(Tag priorTag, List<TagUpdate> tagUpdates) {
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

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

    private WriteOperation createPreallocatedObjectsWriteOperation(List<MetadataWriteRequest> requests) {

        var tags = new ArrayList<Tag>();

        for (var request : requests) {
            var objectId = UUID.fromString(request.getPriorVersion().getObjectId());
            var tag = prepareCreateObject(
                    objectId,
                    request.getDefinition(),
                    request.getTagUpdatesList()
            );
            tags.add(tag);
        }

        var result = new WriteOperation();
        result.tagHeaders = tags.stream().map(Tag::getHeader).collect(Collectors.toList());
        result.writeOperation = new SavePreallocatedObject(tags);
        return result;
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

    private static class WriteOperation {
        DalWriteOperation writeOperation;
        List<TagHeader> tagHeaders;
    }

}
