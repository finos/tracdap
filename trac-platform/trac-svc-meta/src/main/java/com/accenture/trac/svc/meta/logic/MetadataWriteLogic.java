package com.accenture.trac.svc.meta.logic;

import com.accenture.trac.common.metadata.ObjectDefinition;
import com.accenture.trac.common.metadata.ObjectHeader;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.exception.InputValidationError;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.accenture.trac.common.metadata.MetadataCodec.encode;
import static com.accenture.trac.svc.meta.logic.MetadataConstants.TRAC_RESERVED_IDENTIFIER;
import static com.accenture.trac.svc.meta.logic.MetadataConstants.VALID_IDENTIFIER;


public class MetadataWriteLogic {

    public static final boolean TRUSTED = true;
    public static final boolean PUBLIC = false;

    private final Set<ObjectType> PUBLIC_WRITABLE_TYPES = Set.of(
            ObjectType.FLOW,
            ObjectType.CUSTOM);

    private final IMetadataDal dal;

    public MetadataWriteLogic(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<UUID> saveNewObject(String tenant, ObjectType objectType, Tag tag, boolean trust) {

        var definition = tag.getDefinition();

        // Validate object definition

        if (trust == PUBLIC && !PUBLIC_WRITABLE_TYPES.contains(objectType))
            throw new RuntimeException();

        if (definition.hasHeader())
            throw new InputValidationError("Object header must be null when saving a new object");

        if (definition.getDefinitionCase() == ObjectDefinition.DefinitionCase.DEFINITION_NOT_SET)
            throw new InputValidationError("Object definition must be set when saving a new object");

        // Validate the tag attrs

        for (String attrKey: tag.getAttrMap().keySet()) {

            if (!VALID_IDENTIFIER.matcher(attrKey).matches())
                throw new InputValidationError(String.format("Tag attribute '%s' is not a valid identifier", attrKey));

            if (trust == PUBLIC && TRAC_RESERVED_IDENTIFIER.matcher(attrKey).matches())
                throw new InputValidationError(String.format("Tag attribute '%s' is not a reserved identifier", attrKey));
        }

        // Build the object / tag to save

        var objectId = UUID.randomUUID();
        var objectVersion = MetadataConstants.OBJECT_FIRST_VERSION;
        var tagVersion = MetadataConstants.TAG_FIRST_VERSION;

        var header = ObjectHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(encode(objectId))
                .setObjectVersion(objectVersion);

        var definitionToSave = tag.getDefinition()
                .toBuilder()
                .setHeader(header);

        var tagToSave = tag.toBuilder()
                .setDefinition(definitionToSave)
                .setTagVersion(tagVersion)
                .build();

        // Go to the DAL

        return dal.saveNewObject(tenant, tagToSave)
                .thenApply(_ok -> objectId);
    }
}
