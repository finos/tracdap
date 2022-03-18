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

package org.finos.tracdap.common.validation.fixed;

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.validation.core.ValidationContext;
import com.google.protobuf.Descriptors;


public class MetadataValidator {

    // Rough implementation that provides only a few validation points
    // Full implementation will be comprehensive across the metadata model
    // Requires generic handling of all message, enum and primitive types, as well as the TRAC type system

    private static final Descriptors.Descriptor TAG_UPDATE;
    private static final Descriptors.FieldDescriptor TU_OPERATION;
    private static final Descriptors.FieldDescriptor TU_ATTR_NAME;
    private static final Descriptors.FieldDescriptor TU_VALUE;

    private static final Descriptors.Descriptor TAG_SELECTOR;
    private static final Descriptors.FieldDescriptor TS_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor TS_OBJECT_ID;
    private static final Descriptors.FieldDescriptor TS_LATEST_OBJECT;
    private static final Descriptors.FieldDescriptor TS_OBJECT_VERSION;
    private static final Descriptors.FieldDescriptor TS_OBJECT_ASOF;
    private static final Descriptors.FieldDescriptor TS_LATEST_TAG;
    private static final Descriptors.FieldDescriptor TS_TAG_VERSION;
    private static final Descriptors.FieldDescriptor TS_TAG_ASOF;
    private static final Descriptors.OneofDescriptor TS_OBJECT_CRITERIA;
    private static final Descriptors.OneofDescriptor TS_TAG_CRITERIA;

    static {

        TAG_UPDATE = TagUpdate.getDescriptor();
        TU_OPERATION = field(TAG_UPDATE, TagUpdate.OPERATION_FIELD_NUMBER);
        TU_ATTR_NAME = field(TAG_UPDATE, TagUpdate.ATTRNAME_FIELD_NUMBER);
        TU_VALUE = field(TAG_UPDATE, TagUpdate.VALUE_FIELD_NUMBER);

        TAG_SELECTOR = TagSelector.getDescriptor();
        TS_OBJECT_TYPE = field(TAG_SELECTOR, TagSelector.OBJECTTYPE_FIELD_NUMBER);
        TS_OBJECT_ID = field(TAG_SELECTOR, TagSelector.OBJECTID_FIELD_NUMBER);
        TS_LATEST_OBJECT = field(TAG_SELECTOR, TagSelector.LATESTOBJECT_FIELD_NUMBER);
        TS_OBJECT_VERSION = field(TAG_SELECTOR, TagSelector.OBJECTVERSION_FIELD_NUMBER);
        TS_OBJECT_ASOF = field(TAG_SELECTOR, TagSelector.OBJECTASOF_FIELD_NUMBER);
        TS_LATEST_TAG = field(TAG_SELECTOR, TagSelector.LATESTTAG_FIELD_NUMBER);
        TS_TAG_VERSION = field(TAG_SELECTOR, TagSelector.TAGVERSION_FIELD_NUMBER);
        TS_TAG_ASOF = field(TAG_SELECTOR, TagSelector.TAGASOF_FIELD_NUMBER);
        TS_OBJECT_CRITERIA = TS_OBJECT_VERSION.getContainingOneof();
        TS_TAG_CRITERIA = TS_TAG_VERSION.getContainingOneof();
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }


    public static ValidationContext validateTagUpdate(TagUpdate msg, ValidationContext ctx) {

        ctx = ctx.push(TU_OPERATION)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::recognizedEnum, TagOperation.class)
                .pop();

        if (msg.getOperation() == TagOperation.CLEAR_ALL_ATTR) {

            ctx = ctx.push(TU_ATTR_NAME)
                    .apply(CommonValidators::omitted)
                    .pop();

            ctx = ctx.push(TU_VALUE)
                    .apply(CommonValidators::omitted)
                    .pop();
        }
        else {

            ctx = ctx.push(TU_ATTR_NAME)
                    .apply(CommonValidators::required)
                    .apply(CommonValidators::identifier)
                    .apply(CommonValidators::notTracReserved)
                    .pop();

            // TODO: Recursive validation of TRAC Values and TypeDescriptors in TypeSystemValidator
        }

        return ctx;
    }

    public static ValidationContext validateTagSelector(TagSelector msg, ValidationContext ctx) {

        // The "required" validation for enums will fail for ordinal = 0
        // For object types, this is OBJECT_TYPE_NOT_SET

        ctx = ctx.push(TS_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::recognizedEnum, ObjectType.class)
                .pop();

        ctx = ctx.push(TS_OBJECT_ID)
                .apply(CommonValidators::required)
                .apply(CommonValidators::uuid)
                .pop();

        ctx = ctx.pushOneOf(TS_OBJECT_CRITERIA)
                .apply(CommonValidators::required)
                .applyIf(CommonValidators::optionalTrue, Boolean.class, msg.hasField(TS_LATEST_OBJECT))
                .applyIf(CommonValidators::positive, Integer.class, msg.hasField(TS_OBJECT_VERSION))
                .applyIf(TypeSystemValidator::datetimeValue, DatetimeValue.class, msg.hasField(TS_OBJECT_ASOF))
                .pop();

        ctx = ctx.pushOneOf(TS_TAG_CRITERIA)
                .apply(CommonValidators::required)
                .applyIf(CommonValidators::optionalTrue, Boolean.class, msg.hasField(TS_LATEST_TAG))
                .applyIf(CommonValidators::positive, Integer.class, msg.hasField(TS_TAG_VERSION))
                .applyIf(TypeSystemValidator::datetimeValue, DatetimeValue.class, msg.hasField(TS_TAG_ASOF))
                .pop();

        return ctx;
    }

}
