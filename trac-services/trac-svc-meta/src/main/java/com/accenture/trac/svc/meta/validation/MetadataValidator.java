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

import com.accenture.trac.common.api.meta.TagUpdate;
import com.accenture.trac.common.metadata.ObjectDefinition;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.svc.meta.exception.AuthorisationError;
import com.accenture.trac.svc.meta.exception.InputValidationError;
import com.accenture.trac.svc.meta.logic.MetadataConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.accenture.trac.svc.meta.logic.MetadataConstants.*;


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

            throw new InputValidationError(message);
        }

        return this;
    }

    public MetadataValidator checkAndThrowPermissions() {

        if (!validationErrors.isEmpty()) {

            var message = validationErrors.size() > 1
                    ? "There were multiple authorisation errors:\n" + String.join("\n", validationErrors)
                    : validationErrors.get(0);

            throw new AuthorisationError(message);
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
//
//    public MetadataValidator headerIsNull(ObjectDefinition objectDefinition) {
//
//        if (objectDefinition.hasHeader()) {
//            validationErrors.add("Object header must be null");
//        }
//
//        return this;
//    }
//
//    public MetadataValidator headerIsValid(ObjectDefinition objectDefinition) {
//
//        if (!objectDefinition.hasHeader()) {
//            validationErrors.add("Object header is missing");
//        }
//
//        var header = objectDefinition.getHeader();
//
//        if (header.getObjectType() == ObjectType.UNRECOGNIZED){
//            validationErrors.add("Object header does not contain a valid object type");
//        }
//
//        if (!header.hasObjectId()){
//            validationErrors.add("Object header does not contain a valid ID");
//        }
//
//        if (header.getObjectVersion() < MetadataConstants.OBJECT_FIRST_VERSION) {
//            validationErrors.add("Object header does not contain a valid version number");
//        }
//
//        return this;
//    }
//
//    public MetadataValidator headerMatchesType(ObjectDefinition objectDefinition, ObjectType objectType) {
//
//        var header = objectDefinition.getHeader();
//
//        if (header.getObjectType() != objectType) {
//
//            var message = String.format(
//                    "Object header does not match the specified object type" +
//                    " (type specified is %s, header contains %s)",
//                    objectType, header.getObjectType());
//
//            validationErrors.add(message);
//        }
//
//        return this;
//    }

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

//    public MetadataValidator headerIsOnFirstVersion(ObjectDefinition objectDefinition) {
//
//        if (objectDefinition.getHeader().getObjectVersion() != OBJECT_FIRST_VERSION) {
//
//            var message = String.format(
//                    "Object version must be set to %d",
//                    OBJECT_FIRST_VERSION);
//
//            validationErrors.add(message);
//        }
//
//        return this;
//    }
//
//    public MetadataValidator tagVersionIsBlank(Tag tag) {
//
//        // It is not actually possible to set an int value to null in protobuf
//        // Instead we accept 0 or the TAG_FIRST_VERSION when a new tag is being created
//        // Leaving the value unset in client code will create a zero value, which is valid
//
//        if (tag.getTagVersion() != 0 && tag.getTagVersion() != TAG_FIRST_VERSION) {
//            validationErrors.add("Tag version must not be set (allowable values are null, 0 or 1)");
//        }
//
//        return this;
//    }
//
//    public MetadataValidator tagVersionIsValid(Tag tag) {
//
//        if (tag.getTagVersion() < TAG_FIRST_VERSION) {
//            validationErrors.add("Tag does not contain a valid tag version number");
//        }
//
//        return this;
//    }

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
