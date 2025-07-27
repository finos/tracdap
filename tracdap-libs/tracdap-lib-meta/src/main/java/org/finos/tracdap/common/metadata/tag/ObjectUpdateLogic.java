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
import org.finos.tracdap.metadata.*;

import java.util.ArrayList;
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

        var structuredAttrs = structuredAttrs(definition);
        var createAttrs = commonCreateAttrs(requestMetadata, userMetadata);
        var updateAttrs = commonUpdateAttrs(requestMetadata, userMetadata);

        newTag = TagUpdateLogic.applyTagUpdates(newTag, structuredAttrs);
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

        newTag = TagUpdateLogic.applyTagUpdates(newTag, tagUpdates);

        // Apply the common controlled trac_ tags for updated objects

        var structuredAttrs = structuredAttrs(definition);
        var updateAttrs = commonUpdateAttrs(requestMetadata, userMetadata);

        newTag = TagUpdateLogic.applyTagUpdates(newTag, structuredAttrs);
        newTag = TagUpdateLogic.applyTagUpdates(newTag, updateAttrs);

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

    public static List<TagUpdate> structuredAttrs(ObjectDefinition tracObject) {

        // Set structured attrs that are available directly in the object definition
        // This does not cover everything - e.g. job status is not part of the job definition
        // Other controlled attrs must be passed in by the relevant TRAC services

        switch (tracObject.getObjectType()) {

            case SCHEMA: return structuredSchemaAttrs(tracObject.getSchema());
            case FILE: return structuredFileAttrs(tracObject.getFile());
            case MODEL: return structuredModelAttrs(tracObject.getModel());
            case JOB: return structuredJobAttrs(tracObject.getJob());

            default:
                return List.of();
        }
    }

    public static List<TagUpdate> structuredSchemaAttrs(SchemaDefinition tracSchema) {

        var schemaAttrs = new ArrayList<TagUpdate>();

        schemaAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_SCHEMA_TYPE_ATTR)
                .setValue(MetadataCodec.encodeValue(tracSchema.getSchemaType().name()))
                .build());

        if (tracSchema.getSchemaType() == SchemaType.TABLE_SCHEMA &&
            tracSchema.getTable().getFieldsCount() > 0) {

            schemaAttrs.add(TagUpdate.newBuilder()
                    .setAttrName(MetadataConstants.TRAC_SCHEMA_FIELD_COUNT_ATTR)
                    .setValue(MetadataCodec.encodeValue(tracSchema.getTable().getFieldsCount()))
                    .build());
        }
        else {

            schemaAttrs.add(TagUpdate.newBuilder()
                    .setAttrName(MetadataConstants.TRAC_SCHEMA_FIELD_COUNT_ATTR)
                    .setValue(MetadataCodec.encodeValue(tracSchema.getFieldsCount()))
                    .build());
        }

        return schemaAttrs;
    }

    public static List<TagUpdate> structuredDataAttrs(DataDefinition tracData) {

        var dataAttrs = new ArrayList<TagUpdate>();

        dataAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_DATA_ROW_COUNT_ATTR)
                .setValue(MetadataCodec.encodeValue(tracData.getRowCount()))
                .build());

        return dataAttrs;
    }

    public static List<TagUpdate> structuredFileAttrs(FileDefinition tracFile) {

        var fileAttrs = new ArrayList<TagUpdate>();

        fileAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_FILE_NAME_ATTR)
                .setValue(MetadataCodec.encodeValue(tracFile.getName()))
                .build());

        fileAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_FILE_EXTENSION_ATTR)
                .setValue(MetadataCodec.encodeValue(tracFile.getExtension()))
                .build());

        fileAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_FILE_MIME_TYPE_ATTR)
                .setValue(MetadataCodec.encodeValue(tracFile.getMimeType()))
                .build());

        fileAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_FILE_SIZE_ATTR)
                .setValue(MetadataCodec.encodeValue(tracFile.getSize()))
                .build());

        return fileAttrs;
    }

    public static List<TagUpdate> structuredModelAttrs(ModelDefinition tracModel) {

        var modelAttrs = new ArrayList<TagUpdate>();

        modelAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_MODEL_LANGUAGE)
                .setValue(MetadataCodec.encodeValue(tracModel.getLanguage()))
                .build());

        modelAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_MODEL_REPOSITORY)
                .setValue(MetadataCodec.encodeValue(tracModel.getRepository()))
                .build());

        if (tracModel.hasPackageGroup()) {
            modelAttrs.add(TagUpdate.newBuilder()
                    .setAttrName(MetadataConstants.TRAC_MODEL_PACKAGE_GROUP)
                    .setValue(MetadataCodec.encodeValue(tracModel.getPackageGroup()))
                    .build());
        }

        modelAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_MODEL_PACKAGE)
                .setValue(MetadataCodec.encodeValue(tracModel.getPackage()))
                .build());

        modelAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_MODEL_VERSION)
                .setValue(MetadataCodec.encodeValue(tracModel.getVersion()))
                .build());

        modelAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_MODEL_ENTRY_POINT)
                .setValue(MetadataCodec.encodeValue(tracModel.getEntryPoint()))
                .build());

        if (tracModel.hasPath()) {
            modelAttrs.add(TagUpdate.newBuilder()
                    .setAttrName(MetadataConstants.TRAC_MODEL_PATH)
                    .setValue(MetadataCodec.encodeValue(tracModel.getPath()))
                    .build());
        }

        return modelAttrs;
    }

    public static List<TagUpdate> structuredJobAttrs(JobDefinition tracJob) {

        var jobAttrs = new ArrayList<TagUpdate>();

        jobAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_JOB_TYPE_ATTR)
                .setValue(MetadataCodec.encodeValue(tracJob.getJobType().name()))
                .build());

        return jobAttrs;
    }
}
