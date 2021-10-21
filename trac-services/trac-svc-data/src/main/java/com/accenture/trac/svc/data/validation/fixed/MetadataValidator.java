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

    private static final Descriptors.Descriptor TAG_UPDATE;
    private static final Descriptors.FieldDescriptor TU_ATTR_NAME;

    private static final Descriptors.Descriptor TAG_SELECTOR;
    private static final Descriptors.FieldDescriptor TS_OBJECT_ID;
    private static final Descriptors.FieldDescriptor TS_OBJECT_VERSION;
    private static final Descriptors.FieldDescriptor TS_OBJECT_ASOF;
    private static final Descriptors.FieldDescriptor TS_TAG_VERSION;
    private static final Descriptors.FieldDescriptor TS_TAG_ASOF;
    private static final Descriptors.OneofDescriptor TS_OBJECT_CRITERIA;
    private static final Descriptors.OneofDescriptor TS_TAG_CRITERIA;

    private static final Descriptors.Descriptor DATETIME_VALUE;
    private static final Descriptors.FieldDescriptor DTV_ISO_DATETIME;

    static {

        TAG_UPDATE = TagUpdate.getDescriptor();
        TU_ATTR_NAME = field(TAG_UPDATE, TagUpdate.ATTRNAME_FIELD_NUMBER);

        TAG_SELECTOR = TagSelector.getDescriptor();
        TS_OBJECT_ID = field(TAG_SELECTOR, TagSelector.OBJECTID_FIELD_NUMBER);
        TS_OBJECT_VERSION = field(TAG_SELECTOR, TagSelector.OBJECTVERSION_FIELD_NUMBER);
        TS_OBJECT_ASOF =field(TAG_SELECTOR, TagSelector.OBJECTASOF_FIELD_NUMBER);
        TS_TAG_VERSION = field(TAG_SELECTOR, TagSelector.TAGVERSION_FIELD_NUMBER);
        TS_TAG_ASOF = field(TAG_SELECTOR, TagSelector.TAGASOF_FIELD_NUMBER);
        TS_OBJECT_CRITERIA = TS_OBJECT_VERSION.getContainingOneof();
        TS_TAG_CRITERIA = TS_TAG_VERSION.getContainingOneof();

        DATETIME_VALUE = DatetimeValue.getDescriptor();
        DTV_ISO_DATETIME = field(DATETIME_VALUE, DatetimeValue.ISODATETIME_FIELD_NUMBER);
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }


    public static ValidationContext validateTagUpdate(TagUpdate msg, ValidationContext ctx) {

        // TODO: Incomplete validation for TagUpdate
        // Requires enum validation for TagOperation
        // Also requires full recursive validation of TRAC Values and TypeDescriptors

        ctx = ctx.push(TU_ATTR_NAME)
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

        ctx = ctx.push(TS_OBJECT_ID)
                .apply(Validation::required)
                .apply(Validation::uuid)
                .pop();

        ctx = ctx.pushOneOf(TS_OBJECT_CRITERIA)
                .apply(Validation::required)
                .applyIf(Validation::positive, Integer.class, msg.hasField(TS_OBJECT_VERSION))
                .applyIf(MetadataValidator::datetimeValue, DatetimeValue.class, msg.hasField(TS_OBJECT_ASOF))
                .pop();

        ctx = ctx.pushOneOf(TS_TAG_CRITERIA)
                .apply(Validation::required)
                .applyIf(Validation::positive, Integer.class, msg.hasField(TS_TAG_VERSION))
                .applyIf(MetadataValidator::datetimeValue, DatetimeValue.class, msg.hasField(TS_TAG_ASOF))
                .pop();

        return ctx;
    }

    public static ValidationContext datetimeValue(DatetimeValue msg, ValidationContext ctx) {

        return ctx.push(DTV_ISO_DATETIME)
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
