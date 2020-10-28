/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.services;

import com.accenture.trac.common.api.meta.TagUpdate;
import com.accenture.trac.common.exception.ETrac;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.validation.MetadataValidator;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;

import static com.accenture.trac.svc.meta.services.MetadataConstants.*;


public class MetadataWriteService {

    private final IMetadataDal dal;

    public MetadataWriteService(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<TagHeader> createObject(
            String tenant, ObjectType objectType,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {

        var validator = new MetadataValidator();

        var normalDefinition = validator.normalizeObjectType(definition);
        validator.definitionMatchesType(normalDefinition, objectType);
        validator.tagAttributesAreValid(tagUpdates);
        validator.checkAndThrow();

        if (apiTrust == PUBLIC_API) {
            validator.tagAttributesAreNotReserved(tagUpdates);
            validator.checkAndThrowPermissions();
        }

        // Validation complete!


        var objectId = UUID.randomUUID();

        var newHeader = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(OBJECT_FIRST_VERSION)
                .setTagVersion(TAG_FIRST_VERSION)
                .build();

        var newTag = Tag.newBuilder()
                .setHeader(newHeader)
                .setDefinition(normalDefinition)
                .build();

        newTag = applyTagUpdates(newTag, tagUpdates);

        return dal.saveNewObject(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }


    public CompletableFuture<TagHeader> updateObject(
            String tenant, ObjectType objectType,
            TagSelector priorVersion,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {

        var validator = new MetadataValidator();

        // Check whether versioning is supported for this object type
        // If not, we want to raise an error without reporting any other validation issues
        validator.typeSupportsVersioning(objectType);
        validator.checkAndThrow();

        var normalDefinition = validator.normalizeObjectType(definition);
        validator.priorVersionMatchesType(priorVersion, objectType);
        validator.definitionMatchesType(normalDefinition, objectType);
        validator.tagAttributesAreValid(tagUpdates);
        validator.checkAndThrow();

        if (apiTrust == PUBLIC_API) {
            validator.tagAttributesAreNotReserved(tagUpdates);
            validator.checkAndThrowPermissions();
        }

        // Validation complete!


        var objectId = MetadataCodec.decode(priorVersion.getObjectId());
        var objectVersion = priorVersion.getObjectVersion();

        return dal.loadLatestTag(tenant, objectType, objectId, objectVersion)

                .thenCompose(priorTag ->
                updateObject(tenant, priorTag, normalDefinition, tagUpdates));
    }

    private CompletableFuture<TagHeader> updateObject(
            String tenant, Tag priorTag,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        // TODO: Version increment validation

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setObjectVersion(oldHeader.getObjectVersion() + 1)
                .setTagVersion(TAG_FIRST_VERSION)
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .setDefinition(definition)
                .build();

        newTag = applyTagUpdates(newTag, tagUpdates);

        return dal.saveNewVersion(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }

    public CompletableFuture<TagHeader> updateTag(
            String tenant, ObjectType objectType,
            TagSelector priorVersion,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {

        var validator = new MetadataValidator();

        validator.priorVersionMatchesType(priorVersion, objectType);
        validator.tagAttributesAreValid(tagUpdates);
        validator.checkAndThrow();

        if (apiTrust == PUBLIC_API) {
            validator.tagAttributesAreNotReserved(tagUpdates);
            validator.checkAndThrowPermissions();
        }

        // Validation complete!


        var objectId = MetadataCodec.decode(priorVersion.getObjectId());
        var priorObjectVersion = priorVersion.getObjectVersion();
        var priorTagVersion = priorVersion.getTagVersion();

        return dal.loadTag(tenant, objectType, objectId, priorObjectVersion, priorTagVersion)

                .thenCompose(priorTag ->
                updateTag(tenant, priorTag, tagUpdates));
    }

    private CompletableFuture<TagHeader> updateTag(
            String tenant, Tag priorTag,
            List<TagUpdate> tagUpdates) {

        var oldHeader = priorTag.getHeader();

        var newHeader = oldHeader.toBuilder()
                .setTagVersion(oldHeader.getTagVersion() + 1)
                .build();

        var newTag = priorTag.toBuilder()
                .setHeader(newHeader)
                .build();

        newTag = applyTagUpdates(newTag, tagUpdates);

        return dal.saveNewTag(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }

    public CompletableFuture<TagHeader> preallocateId(String tenant, ObjectType objectType) {

        // New random ID
        var objectId = UUID.randomUUID();

        // Header for preallocated IDs does not include an object or tag version
        var preallocatedHeader = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .build();

        // Save as a preallocated ID in the DAL
        return dal.preallocateObjectId(tenant, objectType, objectId)
                .thenApply(_ok -> preallocatedHeader);
    }

    public CompletableFuture<TagHeader> createPreallocatedObject(
            String tenant, ObjectType objectType,
            TagSelector priorVersion,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        var validator = new MetadataValidator();

        var normalDefinition = validator.normalizeObjectType(definition);
        validator.priorVersionMatchesType(priorVersion, objectType);
        validator.definitionMatchesType(normalDefinition, objectType);
        validator.tagAttributesAreValid(tagUpdates);
        validator.checkAndThrow();

        // Preallocated objects are always on the trusted API
        // So no need to check reserved tag attributes

        // Validation complete!


        // In this case priorVersion refers to the preallocated ID
        var objectId = MetadataCodec.decode(priorVersion.getObjectId());

        var newHeader = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(OBJECT_FIRST_VERSION)
                .setTagVersion(TAG_FIRST_VERSION)
                .build();

        var newTag = Tag.newBuilder()
                .setHeader(newHeader)
                .setDefinition(normalDefinition)
                .build();

        newTag = applyTagUpdates(newTag, tagUpdates);

        return dal.savePreallocatedObject(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }

    private Tag applyTagUpdates(Tag priorTag, List<TagUpdate> updates) {

        BinaryOperator<Tag.Builder> SEQUENTIAL_COMBINATION =
                (t1, t2) -> { throw new EUnexpected(); };

        var newTag = updates.stream().reduce(
                priorTag.toBuilder(),
                this::applyTagUpdate,
                SEQUENTIAL_COMBINATION);

        return newTag.build();
    }

    private Tag.Builder applyTagUpdate(Tag.Builder tag, TagUpdate update) {

        switch (update.getOperation()) {

        case CREATE_OR_REPLACE_ATTR:

            return tag.putAttr(update.getAttrName(), update.getValue());

        case CREATE_ATTR:

            if (tag.containsAttr(update.getAttrName()))
                throw new ETrac("");  // attr already exists

            return tag.putAttr(update.getAttrName(), update.getValue());

        case REPLACE_ATTR:

            if (!tag.containsAttr(update.getAttrName()))
                throw new ETrac("");

            var priorType = tag.getAttrOrDefault(update.getAttrName(), update.getValue()).getType();
            var newType = update.getValue().getType();

            if (!attrTypesMatch(priorType, newType))
                throw new ETrac("");

            return tag.putAttr(update.getAttrName(), update.getValue());

        case APPEND_ATTR:

            throw new ETrac("");  // TODO

        case DELETE_ATTR:

            if (!tag.containsAttr(update.getAttrName()))
                throw new ETrac("");

            return tag.removeAttr(update.getAttrName());

        default:
            // Should be picked up by validation
            throw new EUnexpected();
        }
    }

    private boolean attrTypesMatch(TypeDescriptor attr1, TypeDescriptor attr2) {

        // TODO: Array types
        return TypeSystem.isPrimitive(attr1) && attr1.getBasicType() == attr2.getBasicType();

    }

}
