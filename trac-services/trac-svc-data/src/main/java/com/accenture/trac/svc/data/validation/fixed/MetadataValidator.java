/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.validation.fixed;

import com.accenture.trac.metadata.DatetimeValue;
import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.metadata.TagUpdate;
import com.accenture.trac.svc.data.validation.core.ValidationContext;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtocolMessageEnum;


public class MetadataValidator {

    // Rough implementation that provides only a few validation points
    // Full implementation will be comprehensive across the metadata model
    // Requires generic handling of all message, enum and primitive types, as well as the TRAC type system

    private static final Descriptors.FieldDescriptor TAG_SELECTOR_OBJECT_VERSION_FIELD =
            TagSelector.getDescriptor()
            .findFieldByNumber(TagSelector.OBJECTVERSION_FIELD_NUMBER);

    private static final Descriptors.FieldDescriptor TAG_SELECTOR_OBJECT_ASOF_FIELD =
            TagSelector.getDescriptor()
            .findFieldByNumber(TagSelector.OBJECTASOF_FIELD_NUMBER);

    private static final Descriptors.FieldDescriptor TAG_SELECTOR_TAG_VERSION_FIELD =
            TagSelector.getDescriptor()
            .findFieldByNumber(TagSelector.TAGVERSION_FIELD_NUMBER);

    private static final Descriptors.FieldDescriptor TAG_SELECTOR_TAG_ASOF_FIELD =
            TagSelector.getDescriptor()
            .findFieldByNumber(TagSelector.TAGASOF_FIELD_NUMBER);


    public static ValidationContext validateTagUpdate(TagUpdate msg, ValidationContext ctx) {

        // TODO: Incomplete validation for TagUpdate
        // Requires enum validation for TagOperation
        // Also requires full recursive validation of TRAC Values and TypeDescriptors

        ctx = ctx.push("attrName")
                .apply(Validation::required)
                .apply(Validation::identifier)
                .apply(Validation::notTracReserved)
                .pop();

        return ctx;
    }

    public static ValidationContext validateTagSelector(TagSelector msg, ValidationContext ctx) {

        // TODO: Incomplete validation for TagSelector
        // Requires enum validation for ObjectType
        // There is an issue where protobuf returns EnumValueDescriptor for the field value
        // A generic way is needed to convert these into the actual enum type

        ctx = ctx.push("objectId")
                .apply(Validation::required)
                .apply(Validation::uuid)
                .pop();

        ctx = ctx.pushOneOf("objectVersionCriteria")
                .apply(Validation::required)
                .applyIf(Validation::positive, msg.hasField(TAG_SELECTOR_OBJECT_VERSION_FIELD))
                .applyIf(MetadataValidator::datetimeValue, DatetimeValue.class, msg.hasField(TAG_SELECTOR_OBJECT_ASOF_FIELD))
                .pop();

        ctx = ctx.pushOneOf("tagVersionCriteria")
                .apply(Validation::required)
                .applyIf(Validation::positive, msg.hasField(TAG_SELECTOR_TAG_VERSION_FIELD))
                .applyIf(MetadataValidator::datetimeValue, DatetimeValue.class, msg.hasField(TAG_SELECTOR_TAG_ASOF_FIELD))
                .pop();

        return ctx;
    }

    public static ValidationContext datetimeValue(DatetimeValue msg, ValidationContext ctx) {

        return ctx.push("isoDatetime")
                .apply(Validation::required)
                .apply(Validation::isoDatetime)
                .pop();
    }

    public ValidationContext validateEnum(
            ProtocolMessageEnum enum_,
            ValidationContext ctx) {

        try {
            var value = enum_.getNumber();

            if (value == 0) {

                var typeName = enum_.getDescriptorForType().getName();
                var message = String.format("Value not set for [%s]", typeName);
                return ctx.error(message);
            }
        }
        catch (IllegalStateException e) {

            var typeName = enum_.getDescriptorForType().getName();
            var message = String.format("Invalid value for [%s]", typeName);
            return ctx.error(message);
        }

        return ctx;
    }

}
