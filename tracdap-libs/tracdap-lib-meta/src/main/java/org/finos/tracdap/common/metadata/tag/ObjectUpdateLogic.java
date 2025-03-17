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

package org.finos.tracdap.common.metadata.tag;

import org.finos.tracdap.common.grpc.RequestMetadata;
import org.finos.tracdap.common.grpc.UserMetadata;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.metadata.TagUpdate;

import java.util.List;
import java.util.UUID;


public class ObjectUpdateLogic {

    public static Tag buildNewObject(
            UUID objectId, ObjectDefinition definition, List<TagUpdate> tagUpdates,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var newHeader = TagHeader.newBuilder()
                .setObjectType(definition.getObjectType())
                .setObjectId(objectId.toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp()))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp()))
                .setIsLatestTag(true)
                .setIsLatestObject(true)
                .build();

        var newTag = Tag.newBuilder()
                .setHeader(newHeader)
                .setDefinition(definition)
                .build();

        newTag = TagUpdateLogic.applyTagUpdates(newTag, tagUpdates);

        // Apply the common controlled trac_ tags for newly created objects

        var createAttrs = commonCreateAttrs(requestMetadata, userMetadata);
        var updateAttrs = commonUpdateAttrs(requestMetadata, userMetadata);

        newTag = TagUpdateLogic.applyTagUpdates(newTag, createAttrs);
        newTag = TagUpdateLogic.applyTagUpdates(newTag, updateAttrs);

        return newTag;
    }

    public static Tag buildNewVersion(
            Tag priorTag, ObjectDefinition definition, List<TagUpdate> tagUpdates,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setObjectVersion(oldHeader.getObjectVersion() + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp()))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp()))
                .setIsLatestTag(true)
                .setIsLatestObject(true)
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .setDefinition(definition)
                .build();

        // Apply the common controlled trac_ tags for updated objects

        var commonAttrs = commonUpdateAttrs(requestMetadata, userMetadata);

        newTag = TagUpdateLogic.applyTagUpdates(newTag, tagUpdates);
        newTag = TagUpdateLogic.applyTagUpdates(newTag, commonAttrs);

        return newTag;
    }

    public static Tag buildNewTag(
            Tag priorTag, List<TagUpdate> tagUpdates,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        // TODO: Record user info for tag-only updates
        // Audit history for object revisions is most important
        // Audit for tag updates will be needed too at some point

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setTagVersion(oldHeader.getTagVersion() + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp()))
                .setIsLatestTag(true)
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .build();

        newTag = TagUpdateLogic.applyTagUpdates(newTag, tagUpdates);

        return newTag;
    }

    private static List<TagUpdate> commonCreateAttrs(RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var createTimeAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_CREATE_TIME)
                .setValue(MetadataCodec.encodeValue(requestMetadata.requestTimestamp()))
                .build();

        var createUserIdAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_CREATE_USER_ID)
                .setValue(MetadataCodec.encodeValue(userMetadata.userId()))
                .build();

        var createUserNameAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_CREATE_USER_NAME)
                .setValue(MetadataCodec.encodeValue(userMetadata.userName()))
                .build();

        return List.of(createTimeAttr, createUserIdAttr, createUserNameAttr);
    }

    private static List<TagUpdate> commonUpdateAttrs(RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var updateTimeAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_UPDATE_TIME)
                .setValue(MetadataCodec.encodeValue(requestMetadata.requestTimestamp()))
                .build();

        var updateUserIdAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_UPDATE_USER_ID)
                .setValue(MetadataCodec.encodeValue(userMetadata.userId()))
                .build();

        var updateUserNameAttr = TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_UPDATE_USER_NAME)
                .setValue(MetadataCodec.encodeValue(userMetadata.userName()))
                .build();

        return List.of(updateTimeAttr, updateUserIdAttr, updateUserNameAttr);
    }
}
