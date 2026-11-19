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
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.metadata.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


public class ObjectUpdateLogic {

    public static Tag buildNewObject(
            UUID objectId, ObjectDefinition definition, List<TagUpdate> tagUpdates,
            MetadataBundle references, RequestMetadata requestMetadata, UserMetadata userMetadata) {

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

        var structuredAttrs = structuredAttrs(definition, references);
        var createAttrs = commonCreateAttrs(requestMetadata, userMetadata);
        var updateAttrs = commonUpdateAttrs(requestMetadata, userMetadata);

        newTag = TagUpdateLogic.applyTagUpdates(newTag, structuredAttrs);
        newTag = TagUpdateLogic.applyTagUpdates(newTag, createAttrs);
        newTag = TagUpdateLogic.applyTagUpdates(newTag, updateAttrs);

        return newTag;
    }

    public static Tag buildNewVersion(
            Tag priorTag, ObjectDefinition definition, List<TagUpdate> tagUpdates,
            MetadataBundle references, RequestMetadata requestMetadata, UserMetadata userMetadata) {

        return buildNewVersion(priorTag, null, definition, tagUpdates, references, requestMetadata, userMetadata);
    }

    /**
     * Build a new object version from the prior tag.
     *
     * <p>Controlled attributes for all versions are taken from latestPriorTag, the latest tag of the prior version,
     * when the prior tag is not the latest. If latestPriorTag is null they are copied from the prior tag.</p>
     */
    public static Tag buildNewVersion(
            Tag priorTag, Tag latestPriorTag, ObjectDefinition definition, List<TagUpdate> tagUpdates,
            MetadataBundle references, RequestMetadata requestMetadata, UserMetadata userMetadata) {

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

        newTag = dropVersionScopedAttrs(newTag);

        if (latestPriorTag != null)
            newTag = refreshAllVersionsAttrs(newTag, latestPriorTag);

        newTag = TagUpdateLogic.applyTagUpdates(newTag, tagUpdates);

        // Apply the common controlled trac_ tags for updated objects

        var structuredAttrs = structuredAttrs(definition, references);
        var updateAttrs = commonUpdateAttrs(requestMetadata, userMetadata);

        newTag = TagUpdateLogic.applyTagUpdates(newTag, structuredAttrs);
        newTag = TagUpdateLogic.applyTagUpdates(newTag, updateAttrs);

        return newTag;
    }

    private static Tag dropVersionScopedAttrs(Tag tag) {

        var scopedAttrs = tag.getAttrsMap().keySet().stream()
                .filter(attrName -> MetadataConstants.VERSION_SCOPED_ATTRS.matcher(attrName).matches())
                .collect(Collectors.toList());

        if (scopedAttrs.isEmpty())
            return tag;

        var newTag = tag.toBuilder();
        scopedAttrs.forEach(newTag::removeAttrs);

        return newTag.build();
    }

    private static Tag refreshAllVersionsAttrs(Tag tag, Tag latestPriorTag) {

        var newTag = tag.toBuilder();

        tag.getAttrsMap().keySet().stream()
                .filter(attrName -> MetadataConstants.CONTROLLED_ALL_VERSIONS_ATTRS.matcher(attrName).matches())
                .forEach(newTag::removeAttrs);

        latestPriorTag.getAttrsMap().entrySet().stream()
                .filter(attr -> MetadataConstants.CONTROLLED_ALL_VERSIONS_ATTRS.matcher(attr.getKey()).matches())
                .forEach(attr -> newTag.putAttrs(attr.getKey(), attr.getValue()));

        return newTag.build();
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

    /**
     * Build a new tag for a controlled tag write, which sets or clears one controlled attribute.
     *
     * <p>The attribute is stored under the prefix for its version scope, with lineage attributes for the reason, the
     * user and the time. A clear removes the value and rewrites the lineage. If requireValue is set, a clear of an
     * attribute with no value is refused, otherwise only the lineage is written.</p>
     */
    public static Tag buildNewControlledTag(
            Tag priorTag, TagUpdate update, VersionScope versionScope, Value reason, boolean requireValue,
            RequestMetadata requestMetadata, UserMetadata userMetadata) {

        var storedName = controlledAttrName(update.getAttrName(), versionScope);

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setTagVersion(oldHeader.getTagVersion() + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(requestMetadata.requestTimestamp()))
                .setIsLatestTag(true)
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .build();

        var valueUpdate = update.toBuilder()
                .setAttrName(storedName)
                .build();

        var isClear = update.getOperation() == TagOperation.DELETE_ATTR;

        if (!isClear || requireValue || newTag.containsAttrs(storedName))
            newTag = TagUpdateLogic.applyTagUpdates(newTag, List.of(valueUpdate));

        var lineage = List.of(
                lineageAttr(storedName + MetadataConstants.CONTROLLED_REASON_SUFFIX, reason),
                lineageAttr(storedName + MetadataConstants.CONTROLLED_USER_ID_SUFFIX,
                        MetadataCodec.encodeValue(userMetadata.userId())),
                lineageAttr(storedName + MetadataConstants.CONTROLLED_USER_NAME_SUFFIX,
                        MetadataCodec.encodeValue(userMetadata.userName())),
                lineageAttr(storedName + MetadataConstants.CONTROLLED_TIME_SUFFIX,
                        MetadataCodec.encodeValue(requestMetadata.requestTimestamp())));

        return TagUpdateLogic.applyTagUpdates(newTag, lineage);
    }

    public static String controlledAttrName(String attrName, VersionScope versionScope) {

        var prefix = versionScope == VersionScope.VERSION_SCOPE_ALL_VERSIONS
                ? MetadataConstants.CONTROLLED_ALL_VERSIONS_PREFIX
                : MetadataConstants.CONTROLLED_THIS_VERSION_PREFIX;

        return prefix + attrName;
    }

    private static TagUpdate lineageAttr(String attrName, Value value) {

        return TagUpdate.newBuilder()
                .setOperation(TagOperation.CREATE_OR_REPLACE_ATTR)
                .setAttrName(attrName)
                .setValue(value)
                .build();
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

    public static List<TagUpdate> structuredAttrs(ObjectDefinition tracObject, MetadataBundle references) {

        // Set structured attrs that are available directly in the object definition
        // This does not cover everything - e.g. job status is not part of the job definition
        // Other controlled attrs must be passed in by the relevant TRAC services

        switch (tracObject.getObjectType()) {

            case SCHEMA: return structuredSchemaAttrs(tracObject.getSchema());
            case DATA: return structuredDataAttrs(tracObject.getData(), references);
            case FILE: return structuredFileAttrs(tracObject.getFile());
            case MODEL: return structuredModelAttrs(tracObject.getModel());
            case JOB: return structuredJobAttrs(tracObject.getJob());
            case CUSTOM: return structuredCustomAttrs(tracObject.getCustom());

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

        if (tracSchema.getSchemaType() == SchemaType.TABLE_SCHEMA) {

            schemaAttrs.add(TagUpdate.newBuilder()
                    .setAttrName(MetadataConstants.TRAC_SCHEMA_FIELD_COUNT_ATTR)
                    .setValue(MetadataCodec.encodeValue(tracSchema.getTable().getFieldsCount()))
                    .build());
        }
        else if (tracSchema.getSchemaType() == SchemaType.STRUCT_SCHEMA) {

            schemaAttrs.add(TagUpdate.newBuilder()
                    .setAttrName(MetadataConstants.TRAC_SCHEMA_FIELD_COUNT_ATTR)
                    .setValue(MetadataCodec.encodeValue(tracSchema.getStruct().getFieldsCount()))
                    .build());
        }

        return schemaAttrs;
    }

    public static List<TagUpdate> structuredDataAttrs(DataDefinition tracData, MetadataBundle references) {

        var externalSchema = tracData.hasSchemaId() && references != null
                ? references.getObject(tracData.getSchemaId(), /* required = */ false).getSchema()
                : null;

        var schema = tracData.hasSchema()
                ? tracData.getSchema()
                : externalSchema;

        var dataAttrs = schema != null
                ? structuredSchemaAttrs(schema)
                : new ArrayList<TagUpdate>();

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

        modelAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_MODEL_TYPE)
                .setValue(MetadataCodec.encodeValue(tracModel.getModelType().name()))
                .build());

        return modelAttrs;
    }

    public static List<TagUpdate> structuredJobAttrs(JobDefinition tracJob) {

        var jobAttrs = new ArrayList<TagUpdate>();

        jobAttrs.add(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_JOB_TYPE_ATTR)
                .setValue(MetadataCodec.encodeValue(tracJob.getJobType().name()))
                .build());

        if (tracJob.hasContext())
            jobAttrs.addAll(jobContextAttrs(tracJob.getContext()));

        return jobAttrs;
    }

    private static List<TagUpdate> jobContextAttrs(TagSelector context) {

        return List.of(
                TagUpdate.newBuilder()
                        .setAttrName(MetadataConstants.TRAC_JOB_CONTEXT_ATTR)
                        .setValue(MetadataCodec.encodeValue(context.getObjectId()))
                        .build(),
                TagUpdate.newBuilder()
                        .setAttrName(MetadataConstants.TRAC_JOB_CONTEXT_VERSION_ATTR)
                        .setValue(MetadataCodec.encodeValue(context.getObjectVersion()))
                        .build());
    }

    public static List<TagUpdate> structuredCustomAttrs(CustomDefinition tracCustom) {

        return List.of(TagUpdate.newBuilder()
                .setAttrName(MetadataConstants.TRAC_CUSTOM_SCHEMA_TYPE_ATTR)
                .setValue(MetadataCodec.encodeValue(tracCustom.getCustomSchemaType()))
                .build());
    }
}
