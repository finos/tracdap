package com.accenture.trac.svc.meta.logic;

import com.accenture.trac.common.metadata.*;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.validation.MetadataValidator;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.accenture.trac.svc.meta.logic.MetadataConstants.OBJECT_FIRST_VERSION;
import static com.accenture.trac.svc.meta.logic.MetadataConstants.PUBLIC_API;


public class MetadataWriteLogic {

    private final IMetadataDal dal;

    public MetadataWriteLogic(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<UUID> saveNewObject(String tenant, ObjectType objectType, Tag tag, boolean apiTrust) {

        var definition = tag.getDefinition();
        var validator = new MetadataValidator();
        var objectId = new Wrapper<UUID>();

        return CompletableFuture.completedFuture(validator)

                // Validation
                .thenApply(v -> v.headerIsNull(definition))
                .thenApply(v -> v.definitionMatchesType(definition, objectType))
                .thenApply(v -> v.tagVersionIsBlank(tag))
                .thenApply(v -> v.tagAttributesAreValid(tag))
                .thenApply(MetadataValidator::checkAndThrow)

                // Tag permissions
                .thenApply(v -> checkReservedTagAttributes(tag, v, apiTrust))

                // Build object to save
                .thenApply(_x -> objectId.value = UUID.randomUUID())
                .thenApply(_x -> setObjectHeader(definition, objectType, objectId.value, OBJECT_FIRST_VERSION))

                .thenApply(defToSave -> tag.toBuilder()
                        .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                        .setDefinition(defToSave).build())

                // Save and return
                .thenCompose(tagToSave -> dal.saveNewObject(tenant, tagToSave))
                .thenApply(_ok -> objectId.value);
    }

    public CompletableFuture<Integer> saveNewVersion(String tenant, ObjectType objectType, Tag tag, boolean apiTrust) {

        var definition = tag.getDefinition();
        var validator = new MetadataValidator();
        var priorTag = new Wrapper<Tag>();
        var objectVersion = new Wrapper<Integer>();

        return CompletableFuture.completedFuture(validator)

                // Check whether versioning is supported for this object type
                // If not, we want to raise an error without reporting any other validation issues
                .thenApply(v -> v.typeSupportsVersioning(objectType))
                .thenApply(MetadataValidator::checkAndThrow)

                // Validate incoming object / tag
                .thenApply(v -> v.headerIsValid(definition))
                .thenApply(v -> v.headerMatchesType(definition, objectType))
                .thenApply(v -> v.definitionMatchesType(definition, objectType))
                .thenApply(v -> v.tagVersionIsBlank(tag))
                .thenApply(v -> v.tagAttributesAreValid(tag))
                .thenApply(MetadataValidator::checkAndThrow)

                // Tag permissions
                .thenApply(v -> checkReservedTagAttributes(tag, v, apiTrust))

                // Load prior object version
                .thenApply(v -> specifierFromTag(tag))
                .thenCompose(prior -> dal.loadLatestTag(tenant,
                        prior.objectType,
                        prior.objectId,
                        prior.objectVersion))
                .thenAccept(pt -> priorTag.value = pt)

                // TODO: Validate version increment
                .thenApply(pt -> validator)
                .thenApply(MetadataValidator::checkAndThrow)

                // Build tag to save
                .thenApply(v -> objectVersion.value = priorTag.value.getDefinition().getHeader().getObjectVersion() + 1)
                .thenApply(v -> bumpObjectVersion(definition, priorTag.value.getDefinition()))

                .thenApply(defToSave -> tag.toBuilder()
                        .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                        .setDefinition(defToSave).build())

                // Save and return
                .thenCompose(tagToSave -> dal.saveNewVersion(tenant, tagToSave))
                .thenApply(_ok -> objectVersion.value);
    }

    public CompletableFuture<Integer> saveNewTag(String tenant, ObjectType objectType, Tag tag, boolean apiTrust) {

        var definition = tag.getDefinition();
        var validator = new MetadataValidator();
        var priorTag = new Wrapper<Tag>();
        var tagVersion = new Wrapper<Integer>();

        return CompletableFuture.completedFuture(validator)

                // Validate incoming object / tag
                .thenApply(v -> v.headerIsValid(definition))
                .thenApply(v -> v.headerMatchesType(definition, objectType))
                // TODO: Allow a null definition when creating a new tag
                .thenApply(v -> v.definitionMatchesType(definition, objectType))
                .thenApply(v -> v.tagVersionIsValid(tag))
                .thenApply(v -> v.tagAttributesAreValid(tag))
                .thenApply(MetadataValidator::checkAndThrow)

                // Tag permissions
                .thenApply(v -> checkReservedTagAttributes(tag, v, apiTrust))

                // Load prior tag version
                .thenApply(v -> specifierFromTag(tag))
                .thenCompose(prior -> dal.loadTag(tenant,
                        prior.objectType,
                        prior.objectId,
                        prior.objectVersion,
                        prior.tagVersion))
                .thenAccept(pt -> priorTag.value = pt)

                // TODO: Validate increment

                // Build tag to save
                .thenApply(pt -> tagVersion.value = priorTag.value.getTagVersion() + 1)
                .thenApply(tv -> definition.toBuilder().clearDefinition().build())

                .thenApply(defToSave -> tag.toBuilder()
                        .setTagVersion(tagVersion.value)
                        .setDefinition(defToSave).build())

                // Save and return
                .thenCompose(tagToSave -> dal.saveNewTag(tenant, tagToSave))
                .thenApply(_ok -> tagVersion.value);
    }

    private MetadataValidator checkReservedTagAttributes(Tag tag, MetadataValidator validator, boolean apiTrust) {

        if (apiTrust == PUBLIC_API)

            return validator
                    .tagAttributesAreNotReserved(tag)
                    .checkAndThrowPermissions();

        else  // trust = TRUSTED_API

            return validator;
    }

    private ObjectDefinition setObjectHeader(ObjectDefinition definition, ObjectType objectType, UUID objectId, int objectVersion) {

        var header = ObjectHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(objectId))
                .setObjectVersion(objectVersion);

        return definition.toBuilder()
                .setHeader(header)
                .build();
    }

    private ObjectDefinition bumpObjectVersion(ObjectDefinition newDefinition, ObjectDefinition priorDefinition) {

        var priorHeader = priorDefinition.getHeader();

        return newDefinition.toBuilder()
                .setHeader(priorHeader.toBuilder()
                .setObjectVersion(priorHeader.getObjectVersion() + 1))
                .build();
    }

    private ObjectSpecifier specifierFromTag(Tag tag) {

        var header = tag.getDefinition().getHeader();

        var spec = new ObjectSpecifier();
        spec.objectType = header.getObjectType();
        spec.objectId = MetadataCodec.decode(header.getObjectId());
        spec.objectVersion = header.getObjectVersion();
        spec.tagVersion = tag.getTagVersion();

        return spec;
    }

    private static class ObjectSpecifier {
        ObjectType objectType;
        UUID objectId;
        int objectVersion;
        int tagVersion;
    }

    private static class Wrapper<T> {

        public T value;
    }
}
