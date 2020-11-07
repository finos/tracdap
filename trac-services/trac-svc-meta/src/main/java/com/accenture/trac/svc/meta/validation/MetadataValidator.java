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

package com.accenture.trac.svc.meta.validation;

import com.accenture.trac.common.metadata.ObjectDefinition;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.TagSelector;
import com.accenture.trac.common.metadata.TagUpdate;
import com.accenture.trac.common.exception.*;

import java.util.*;

import static com.accenture.trac.svc.meta.services.MetadataConstants.*;


public class MetadataValidator {

    private static final Set<ObjectType> VERSIONED_OBJECT_TYPES = Set.of(
            ObjectType.DATA,
            ObjectType.CUSTOM);

    private static final Map<ObjectDefinition.DefinitionCase, ObjectType> DEFINITION_TYPE_MAPPING = Map.of(
            ObjectDefinition.DefinitionCase.DATA, ObjectType.DATA,
            ObjectDefinition.DefinitionCase.MODEL, ObjectType.MODEL,
            ObjectDefinition.DefinitionCase.FLOW, ObjectType.FLOW,
            ObjectDefinition.DefinitionCase.JOB, ObjectType.JOB,
            ObjectDefinition.DefinitionCase.FILE, ObjectType.FILE,
            ObjectDefinition.DefinitionCase.CUSTOM, ObjectType.CUSTOM);

    private final List<String> validationErrors;

    public MetadataValidator() {

        this.validationErrors = new ArrayList<>();
    }

    public MetadataValidator checkAndThrow() {

        if (!validationErrors.isEmpty()) {

            var message = validationErrors.size() > 1
                    ? "There were multiple validation errors:\n" + String.join("\n", validationErrors)
                    : validationErrors.get(0);

            throw new EInputValidation(message);
        }

        return this;
    }

    public MetadataValidator checkAndThrowPermissions() {

        if (!validationErrors.isEmpty()) {

            var message = validationErrors.size() > 1
                    ? "There were multiple authorisation errors:\n" + String.join("\n", validationErrors)
                    : validationErrors.get(0);

            throw new EAuthorization(message);
        }

        return this;
    }

    public MetadataValidator typeSupportsVersioning(ObjectType objectType) {

        if (!VERSIONED_OBJECT_TYPES.contains(objectType)) {
            var message = String.format("Object type %s does not support versioning", objectType);
            validationErrors.add(message);
        }

        return this;
    }

    public ObjectDefinition normalizeObjectType(ObjectDefinition rawDefinition) {

        var definitionType = DEFINITION_TYPE_MAPPING.getOrDefault(
                rawDefinition.getDefinitionCase(), ObjectType.UNRECOGNIZED);

        if (definitionType == ObjectType.UNRECOGNIZED) {
            validationErrors.add("Type could not be recognised for object definition");
            return rawDefinition;
        }

        if (rawDefinition.getObjectType() != ObjectType.OBJECT_TYPE_NOT_SET) {

            if (rawDefinition.getObjectType() != definitionType) {

                var message = String.format(
                        "Object definition does not match its own object type" +
                                " (type specified is %s, definition is %s)",
                        rawDefinition.getObjectType(), definitionType);

                validationErrors.add(message);
            }

            return rawDefinition;
        }

        return rawDefinition.toBuilder()
                .setObjectType(definitionType)
                .build();
    }

    public MetadataValidator validObjectID(TagSelector tagSelector) {

        try {
            // noinspection ResultOfMethodCallIgnored
            UUID.fromString(tagSelector.getObjectId());
        }
        catch (IllegalArgumentException e) {

            var message = "Tag selector does not contain a valid object ID (requires a standard format UUID)";
            validationErrors.add(message);
        }

        return this;
    }

    public MetadataValidator definitionMatchesType(ObjectDefinition objectDefinition, ObjectType objectType) {

        var definitionType = DEFINITION_TYPE_MAPPING.getOrDefault(
                objectDefinition.getDefinitionCase(), ObjectType.UNRECOGNIZED);

        if (definitionType != objectType) {

            var message = String.format(
                    "Object definition does not match the specified object type" +
                    " (type specified is %s, definition is %s)",
                    objectType, definitionType);

            validationErrors.add(message);
        }

        return this;
    }

    public MetadataValidator priorVersionMatchesType(TagSelector priorVersion, ObjectType objectType) {

        if (priorVersion.getObjectType() != objectType) {

            var message = String.format(
                    "Prior version does not match the specified object type" +
                            " (type specified is %s, prior version type is %s)",
                    objectType, priorVersion.getObjectType());

            validationErrors.add(message);
        }

        return this;
    }

    public MetadataValidator tagAttributesAreValid(List<TagUpdate> updates) {

        for (var update : updates) {

            if (!VALID_IDENTIFIER.matcher(update.getAttrName()).matches()) {

                var message = String.format("Tag attribute is not a valid identifier: '%s'", update.getAttrName());
                validationErrors.add(message);
            }
        }

        return this;
    }

    public MetadataValidator tagAttributesAreNotReserved(List<TagUpdate> updates) {

        for (var update : updates) {

            if (TRAC_RESERVED_IDENTIFIER.matcher(update.getAttrName()).matches()) {

                var message = String.format("Tag attribute is reserved for use by TRAC: '%s'", update.getAttrName());
                validationErrors.add(message);
            }
        }

        return this;
    }
}
