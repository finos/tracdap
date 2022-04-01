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

import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.validation.core.ValidationContext;
import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class ObjectIdValidator {

    private static final Descriptors.Descriptor TAG_HEADER;
    private static final Descriptors.FieldDescriptor TH_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor TH_OBJECT_ID;
    private static final Descriptors.FieldDescriptor TH_OBJECT_VERSION;
    private static final Descriptors.FieldDescriptor TH_OBJECT_TIMESTAMP;
    private static final Descriptors.FieldDescriptor TH_TAG_VERSION;
    private static final Descriptors.FieldDescriptor TH_TAG_TIMESTAMP;

    private static final Descriptors.Descriptor TAG_SELECTOR;
    private static final Descriptors.FieldDescriptor TS_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor TS_OBJECT_ID;
    private static final Descriptors.OneofDescriptor TS_OBJECT_CRITERIA;
    private static final Descriptors.FieldDescriptor TS_LATEST_OBJECT;
    private static final Descriptors.FieldDescriptor TS_OBJECT_VERSION;
    private static final Descriptors.FieldDescriptor TS_OBJECT_ASOF;
    private static final Descriptors.OneofDescriptor TS_TAG_CRITERIA;
    private static final Descriptors.FieldDescriptor TS_LATEST_TAG;
    private static final Descriptors.FieldDescriptor TS_TAG_VERSION;
    private static final Descriptors.FieldDescriptor TS_TAG_ASOF;

    static {

        TAG_HEADER = TagHeader.getDescriptor();
        TH_OBJECT_TYPE = field(TAG_HEADER, TagHeader.OBJECTTYPE_FIELD_NUMBER);
        TH_OBJECT_ID = field(TAG_HEADER, TagHeader.OBJECTID_FIELD_NUMBER);
        TH_OBJECT_VERSION = field(TAG_HEADER, TagHeader.OBJECTVERSION_FIELD_NUMBER);
        TH_OBJECT_TIMESTAMP = field(TAG_HEADER, TagHeader.OBJECTTIMESTAMP_FIELD_NUMBER);
        TH_TAG_VERSION = field(TAG_HEADER, TagHeader.TAGVERSION_FIELD_NUMBER);
        TH_TAG_TIMESTAMP = field(TAG_HEADER, TagHeader.TAGTIMESTAMP_FIELD_NUMBER);

        TAG_SELECTOR = TagSelector.getDescriptor();
        TS_OBJECT_TYPE = field(TAG_SELECTOR, TagSelector.OBJECTTYPE_FIELD_NUMBER);
        TS_OBJECT_ID = field(TAG_SELECTOR, TagSelector.OBJECTID_FIELD_NUMBER);
        TS_OBJECT_CRITERIA = field(TAG_SELECTOR, TagSelector.LATESTOBJECT_FIELD_NUMBER).getContainingOneof();
        TS_LATEST_OBJECT = field(TAG_SELECTOR, TagSelector.LATESTOBJECT_FIELD_NUMBER);
        TS_OBJECT_VERSION = field(TAG_SELECTOR, TagSelector.OBJECTVERSION_FIELD_NUMBER);
        TS_OBJECT_ASOF = field(TAG_SELECTOR, TagSelector.OBJECTASOF_FIELD_NUMBER);
        TS_TAG_CRITERIA = field(TAG_SELECTOR, TagSelector.LATESTTAG_FIELD_NUMBER).getContainingOneof();
        TS_LATEST_TAG = field(TAG_SELECTOR, TagSelector.LATESTTAG_FIELD_NUMBER);
        TS_TAG_VERSION = field(TAG_SELECTOR, TagSelector.TAGVERSION_FIELD_NUMBER);
        TS_TAG_ASOF = field(TAG_SELECTOR, TagSelector.TAGASOF_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext tagHeader(TagHeader msg, ValidationContext ctx) {

        ctx = ctx.push(TH_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::recognizedEnum, ObjectType.class)
                .pop();

        ctx = ctx.push(TH_OBJECT_ID)
                .apply(CommonValidators::required)
                .apply(CommonValidators::uuid)
                .pop();

        ctx = ctx.push(TH_OBJECT_VERSION)
                .apply(CommonValidators::required)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        ctx = ctx.push(TH_OBJECT_TIMESTAMP)
                .apply(CommonValidators::required)
                .apply(TypeSystemValidator::datetimeValue, DatetimeValue.class)
                .pop();

        ctx = ctx.push(TH_TAG_VERSION)
                .apply(CommonValidators::required)
                .apply(CommonValidators::positive, Integer.class)
                .pop();

        ctx = ctx.push(TH_TAG_TIMESTAMP)
                .apply(CommonValidators::required)
                .apply(TypeSystemValidator::datetimeValue, DatetimeValue.class)
                .pop();

        if (!ctx.failed()) {

            var objectTimestamp = MetadataCodec.decodeDatetime(msg.getObjectTimestamp());
            var tagTimestamp = MetadataCodec.decodeDatetime(msg.getTagTimestamp());

            if (msg.getTagVersion() == MetadataConstants.TAG_FIRST_VERSION) {

                if (!tagTimestamp.equals(objectTimestamp))
                    ctx.error("Tag timestamp should be the same as object timestamp when tag version = " + MetadataConstants.TAG_FIRST_VERSION);
            }
            else {

                if (!tagTimestamp.isAfter(objectTimestamp))
                    ctx.error("Tag timestamp should be later than object timestamp when tag version > " + MetadataConstants.TAG_FIRST_VERSION);
            }
        }

        return ctx;
    }

    @Validator
    public static ValidationContext tagSelector(TagSelector msg, ValidationContext ctx) {

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
                .applyOneOf(TS_LATEST_OBJECT, CommonValidators::optionalTrue, Boolean.class)
                .applyOneOf(TS_OBJECT_VERSION, CommonValidators::positive, Integer.class)
                .applyOneOf(TS_OBJECT_ASOF, TypeSystemValidator::datetimeValue, DatetimeValue.class)
                .pop();

        ctx = ctx.pushOneOf(TS_TAG_CRITERIA)
                .apply(CommonValidators::required)
                .applyOneOf(TS_LATEST_TAG, CommonValidators::optionalTrue, Boolean.class)
                .applyOneOf(TS_TAG_VERSION, CommonValidators::positive, Integer.class)
                .applyOneOf(TS_TAG_ASOF, TypeSystemValidator::datetimeValue, DatetimeValue.class)
                .pop();

        return ctx;
    }

    public static ValidationContext preallocated(TagSelector selector, ValidationContext ctx) {

        // Use a separate top level validator for preallocated selectors
        // For regular selectors, we want to enforce a positive value for object and tag version
        // But for preallocated selectors, these must both be present and set to zero

        ctx = ctx.push(TS_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::recognizedEnum, ObjectType.class)
                .pop();

        ctx = ctx.push(TS_OBJECT_ID)
                .apply(CommonValidators::required)
                .apply(CommonValidators::uuid)
                .pop();

        var isPreallocated =
                selector.hasObjectVersion() &&
                selector.hasTagVersion() &&
                selector.getObjectVersion() == 0 &&
                selector.getTagVersion() == 0;

        if (!isPreallocated) {

            var err = String.format(
                    "The [%s] selector is not a preallocated object ID (object and tag version must be set to zero)",
                    ctx.fieldName());

            ctx = ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext selectorType(TagSelector selector, ObjectType requiredType, ValidationContext ctx) {

        if (!selector.getObjectType().equals(requiredType)) {
            var err = String.format("Wrong object type in [%s] selector: expected [%s], got [%s]",
                    ctx.fieldName(), requiredType, selector.getObjectType());
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext selectorForLatest(TagSelector selector, ValidationContext ctx) {

        if (!selector.getLatestObject() || !selector.getLatestTag()) {

            var err = String.format(
                    "The [%s] selector must refer to the latest object and tag version, fixed versions are not allowed",
                    ctx.fieldName());

            ctx = ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext fixedObjectVersion(TagSelector selector, ValidationContext ctx) {

        if (selector.hasLatestObject()) {

            var err = String.format(
                    "The [%s] selector must refer to a fixed object version, [latestObject] is not allowed",
                    ctx.fieldName());

            ctx = ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext explicitObjectVersion(TagSelector selector, ValidationContext ctx) {

        if (!selector.hasObjectVersion()) {

            var err = String.format(
                    "The [%s] selector must use an explicit [%s]",
                    ctx.fieldName(), TS_OBJECT_VERSION.getName());

            ctx = ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext explicitTagVersion(TagSelector selector, ValidationContext ctx) {

        if (!selector.hasTagVersion()) {

            var err = String.format(
                    "The [%s] selector must use an explicit [%s]",
                    ctx.fieldName(), TS_TAG_VERSION.getName());

            ctx = ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext versioningSupported(ObjectType objectType, ValidationContext ctx) {

        if (!MetadataConstants.VERSIONED_OBJECT_TYPES.contains(objectType)) {

            var err = String.format(
                    "Object type [%s] does not support versioning",
                    objectType.name());

            return ctx.error(err);
        }

        return ctx;
    }
}
