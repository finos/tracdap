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

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;
import org.finos.tracdap.svc.meta.validation.MetadataValidator;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.finos.tracdap.common.metadata.MetadataConstants.OBJECT_FIRST_VERSION;
import static org.finos.tracdap.common.metadata.MetadataConstants.TAG_FIRST_VERSION;


public class MetadataWriteService {

    private final Validator newValidator = new Validator();
    private final IMetadataDal dal;

    public MetadataWriteService(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<TagHeader> createObject(
            String tenant, ObjectType objectType,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {

        // Original validation

        var validator = new MetadataValidator();

        var normalDefinition = validator.normalizeObjectType(definition);

        if (apiTrust == MetadataConstants.PUBLIC_API) {
            validator.tagAttributesAreNotReserved(tagUpdates);
            validator.checkAndThrowPermissions();
        }

        // Validation complete!


        var objectId = UUID.randomUUID();
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var newHeader = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        var newTag = Tag.newBuilder()
                .setHeader(newHeader)
                .setDefinition(normalDefinition)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);

        return dal.saveNewObject(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }


    public CompletableFuture<TagHeader> updateObject(
            String tenant,
            TagSelector priorVersion,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {

        var validator = new MetadataValidator();

        var normalDefinition = validator.normalizeObjectType(definition);

        if (apiTrust == MetadataConstants.PUBLIC_API) {
            validator.tagAttributesAreNotReserved(tagUpdates);
            validator.checkAndThrowPermissions();
        }

        // Validation complete!


        return dal.loadObject(tenant, priorVersion)

                .thenCompose(priorTag ->
                updateObject(tenant, priorTag, normalDefinition, tagUpdates));
    }

    private CompletableFuture<TagHeader> updateObject(
            String tenant, Tag priorTag,
            ObjectDefinition definition,
            List<TagUpdate> tagUpdates) {

        // Validate version increment on the object
        newValidator.validateVersion(definition, priorTag.getDefinition());


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

        return dal.saveNewVersion(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }

    public CompletableFuture<TagHeader> updateTag(
            String tenant, ObjectType objectType,
            TagSelector priorVersion,
            List<TagUpdate> tagUpdates,
            boolean apiTrust) {

        if (apiTrust == MetadataConstants.PUBLIC_API) {
            var validator = new MetadataValidator();
            validator.tagAttributesAreNotReserved(tagUpdates);
            validator.checkAndThrowPermissions();
        }

        // Validation complete!


        return dal.loadObject(tenant, priorVersion)

                .thenCompose(priorTag ->
                updateTag(tenant, priorTag, tagUpdates));
    }

    private CompletableFuture<TagHeader> updateTag(
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

        return dal.saveNewTag(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }

    public CompletableFuture<TagHeader> preallocateId(String tenant, ObjectType objectType) {

        // New random ID
        var objectId = UUID.randomUUID();

        // Header for preallocated IDs does not include an object or tag version
        var preallocatedHeader = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
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

        // Preallocated objects are always on the trusted API
        // So no need to check reserved tag attributes

        // Validation complete!


        // In this case priorVersion refers to the preallocated ID
        var objectId = UUID.fromString(priorVersion.getObjectId());
        var timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var newHeader = TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(objectId.toString())
                .setObjectVersion(OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        var newTag = Tag.newBuilder()
                .setHeader(newHeader)
                .setDefinition(normalDefinition)
                .build();

        newTag = TagUpdateService.applyTagUpdates(newTag, tagUpdates);

        return dal.savePreallocatedObject(tenant, newTag)
                .thenApply(_ok -> newHeader);
    }

}
