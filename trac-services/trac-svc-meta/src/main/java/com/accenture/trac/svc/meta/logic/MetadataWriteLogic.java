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

package com.accenture.trac.svc.meta.logic;

import com.accenture.trac.common.api.meta.TagUpdate;
import com.accenture.trac.common.exception.ETrac;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.validation.MetadataValidator;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.accenture.trac.svc.meta.logic.MetadataConstants.*;


public class MetadataWriteLogic {

    private final IMetadataDal dal;

    public MetadataWriteLogic(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<TagHeader> createObject(
            String tenant, ObjectType objectType,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {


        // Validation no longer needed (after taking headers out of write requests)
        // .thenApply(v -> v.headerIsNull(definition))
        // .thenApply(v -> v.tagVersionIsBlank(tag))

        var validator = new MetadataValidator();

        validator.definitionMatchesType(definition, objectType);
        // validator.tagAttributesAreValid(attrUpdates);  TODO
        validator.checkAndThrow();

        //if (apiTrust == PUBLIC_API) TODO
        //    checkReservedTagAttributes(attrUpdates, validator, apiTrust);

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
                .setDefinition(definition)
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


        // Validation no longer needed (after taking headers out of write requests)
        // .thenApply(v -> v.headerIsValid(definition))
        // .thenApply(v -> v.headerMatchesType(definition, objectType))
        // .thenApply(v -> v.tagVersionIsBlank(tag))

        var validator = new MetadataValidator();

        // Check whether versioning is supported for this object type
        // If not, we want to raise an error without reporting any other validation issues
        validator.typeSupportsVersioning(objectType);
        validator.checkAndThrow();
        validator.definitionMatchesType(definition, objectType);
        // validator.tagAttributesAreValid(attrUpdates);  TODO
        validator.checkAndThrow();

        //if (apiTrust == PUBLIC_API) TODO
        //    checkReservedTagAttributes(attrUpdates, validator, apiTrust);

        // Validation complete!


        var objectId = MetadataCodec.decode(priorVersion.getObjectId());
        var objectVersion = priorVersion.getObjectVersion();

        return dal.loadLatestTag(tenant, objectType, objectId, objectVersion)

                .thenCompose(priorTag ->
                updateObject(tenant, priorTag, definition, tagUpdates));
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
            TagSelector priorSelector,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {

        // Validation no longer needed (after taking headers out of write requests)
        // .thenApply(v -> v.headerIsValid(definition))
        // .thenApply(v -> v.headerMatchesType(definition, objectType))
        // .thenApply(v -> v.definitionMatchesType(definition, objectType))
        // .thenApply(v -> v.tagVersionIsValid(tag))

        var validator = new MetadataValidator();
        // validator.tagAttributesAreValid(attrUpdates);  TODO
        validator.checkAndThrow();

        //if (apiTrust == PUBLIC_API) TODO
        //    checkReservedTagAttributes(attrUpdates, validator, apiTrust);

        // Validation complete!


        var objectId = MetadataCodec.decode(priorSelector.getObjectId());
        var priorVersion = priorSelector.getObjectVersion();
        var priorTagVersion = priorSelector.getTagVersion();

        return dal.loadTag(tenant, objectType, objectId, priorVersion, priorTagVersion)

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

        // Validation no longer needed (after taking headers out of write requests)
        // .thenApply(v -> v.headerIsValid(definition))
        // .thenApply(v -> v.headerMatchesType(definition, objectType))
        // .thenApply(v -> v.definitionMatchesType(definition, objectType))
        // .thenApply(v -> v.tagVersionIsBlank(tag))

        var validator = new MetadataValidator();
        // .thenApply(v -> v.headerIsOnFirstVersion(definition))  TODO: Selector for preallocation (v = 0)
        // validator.tagAttributesAreValid(attrUpdates);  TODO
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
                .setDefinition(definition)
                .build();

        newTag = applyTagUpdates(newTag, tagUpdates);

        return dal.savePreallocatedObject(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }

    private MetadataValidator checkReservedTagAttributes(Tag tag, MetadataValidator validator, boolean apiTrust) {

        if (apiTrust == PUBLIC_API)

            return validator
                    .tagAttributesAreNotReserved(tag)
                    .checkAndThrowPermissions();

        else  // trust = TRUSTED_API

            return validator;
    }

    private Tag applyTagUpdates(Tag priorTag, List<TagUpdate> updates) {

        var newTag = updates.stream().reduce(
                priorTag.toBuilder(),
                this::applyTagUpdate, null);

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

            tag.removeAttr(update.getAttrName());

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
