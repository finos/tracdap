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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.ObjectType;

import com.google.protobuf.Descriptors;

import java.util.Map;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class ObjectValidator {

    private static final Map<ObjectDefinition.DefinitionCase, ObjectType> DEFINITION_CASE_MAPPING = Map.ofEntries(
            Map.entry(ObjectDefinition.DefinitionCase.DATA, ObjectType.DATA),
            Map.entry(ObjectDefinition.DefinitionCase.MODEL, ObjectType.MODEL),
            Map.entry(ObjectDefinition.DefinitionCase.FLOW, ObjectType.FLOW),
            Map.entry(ObjectDefinition.DefinitionCase.JOB, ObjectType.JOB),
            Map.entry(ObjectDefinition.DefinitionCase.FILE, ObjectType.FILE),
            Map.entry(ObjectDefinition.DefinitionCase.CUSTOM, ObjectType.CUSTOM),
            Map.entry(ObjectDefinition.DefinitionCase.STORAGE, ObjectType.STORAGE),
            Map.entry(ObjectDefinition.DefinitionCase.SCHEMA, ObjectType.SCHEMA));

    private static final Descriptors.Descriptor OBJECT_DEFINITION;
    private static final Descriptors.FieldDescriptor OD_OBJECT_TYPE;
    private static final Descriptors.OneofDescriptor OD_DEFINITION;
    private static final Descriptors.FieldDescriptor OD_OBJECT_PROPS;

    static {

        OBJECT_DEFINITION = ObjectDefinition.getDescriptor();
        OD_OBJECT_TYPE = field(OBJECT_DEFINITION, ObjectDefinition.OBJECTTYPE_FIELD_NUMBER);
        OD_DEFINITION = field(OBJECT_DEFINITION, ObjectDefinition.DATA_FIELD_NUMBER).getContainingOneof();
        OD_OBJECT_PROPS = field(OBJECT_DEFINITION, ObjectDefinition.OBJECTPROPS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext objectDefinition(ObjectDefinition msg, ValidationContext ctx) {

        ctx = ctx.push(OD_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ObjectType.class)
                .pop();

        ctx = ctx.pushOneOf(OD_DEFINITION)
                .apply(CommonValidators::required)
                .apply(ObjectValidator::definitionMatchesType)
                .applyRegistered()
                .pop();

        ctx = ctx.pushMap(OD_OBJECT_PROPS)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::standardProps)
                .pop();

        return ctx;
    }

    public static ValidationContext objectType(ObjectDefinition msg, ObjectType expectedType, ValidationContext ctx) {

        var objectType = msg.hasField(OD_OBJECT_TYPE)
                ? msg.getObjectType()
                : DEFINITION_CASE_MAPPING.getOrDefault(msg.getDefinitionCase(), ObjectType.UNRECOGNIZED);

        if (objectType != expectedType) {

            var err = String.format(
                    "The given [%s] has the wrong object type: expected [%s], got [%s]",
                    ctx.fieldName(), expectedType, objectType);

            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext definitionMatchesType(ValidationContext ctx) {

        var objectDef = (ObjectDefinition) ctx.parentMsg();
        var definitionCase = objectDef.getDefinitionCase();

        var objectType = objectDef.getObjectType();
        var definitionType = DEFINITION_CASE_MAPPING.getOrDefault(definitionCase, ObjectType.UNRECOGNIZED);

        if (objectType != definitionType) {

            var err = String.format("Object has type [%s] but contains definition type [%s]",
                    objectType, definitionType);

            return ctx.error(err);
        }

        return ctx;
    }
}
