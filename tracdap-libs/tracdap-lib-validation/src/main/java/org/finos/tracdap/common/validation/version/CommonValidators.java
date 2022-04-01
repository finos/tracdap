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

package org.finos.tracdap.common.validation.version;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.metadata.*;
import com.google.protobuf.Descriptors;

import java.util.Objects;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


public class CommonValidators {

    private static final Descriptors.Descriptor TAG_SELECTOR;
    private static final Descriptors.FieldDescriptor OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor OBJECT_ID;
    private static final Descriptors.OneofDescriptor OBJECT_CRITERIA;
    private static final Descriptors.FieldDescriptor OBJECT_VERSION;
    private static final Descriptors.FieldDescriptor OBJECT_ASOF;
    private static final Descriptors.OneofDescriptor TAG_CRITERIA;
    private static final Descriptors.FieldDescriptor TAG_VERSION;
    private static final Descriptors.FieldDescriptor TAG_ASOF;

    static {

        TAG_SELECTOR = TagSelector.getDescriptor();
        OBJECT_TYPE = field(TAG_SELECTOR, TagSelector.OBJECTTYPE_FIELD_NUMBER);
        OBJECT_ID = field(TAG_SELECTOR, TagSelector.OBJECTID_FIELD_NUMBER);
        OBJECT_CRITERIA = field(TAG_SELECTOR, TagSelector.LATESTOBJECT_FIELD_NUMBER).getContainingOneof();
        OBJECT_VERSION = field(TAG_SELECTOR, TagSelector.OBJECTVERSION_FIELD_NUMBER);
        OBJECT_ASOF = field(TAG_SELECTOR, TagSelector.OBJECTASOF_FIELD_NUMBER);
        TAG_CRITERIA = field(TAG_SELECTOR, TagSelector.LATESTTAG_FIELD_NUMBER).getContainingOneof();
        TAG_VERSION = field(TAG_SELECTOR, TagSelector.TAGVERSION_FIELD_NUMBER);
        TAG_ASOF = field(TAG_SELECTOR, TagSelector.TAGASOF_FIELD_NUMBER);
    }


    public static ValidationContext exactMatch(Object current, Object prior, ValidationContext ctx) {

        var equal = Objects.equals(prior, current);

        if (!equal) {

            var err = String.format(
                    "Value of [%s] must not change between versions: prior = [%s], new = [%s]",
                    ctx.fieldName(), displayValue(prior), displayValue(current));

            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext equalOrGreater(Integer current, Integer prior, ValidationContext ctx) {

        if (current < prior) {

            var err = String.format(
                    "Value of [%s] cannot be lower than the previous version: prior = [%d], new = [%d]",
                    ctx.fieldName(), prior, current);

            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext equalOrAfter(DatetimeValue current, DatetimeValue prior, ValidationContext ctx) {

        var currentTimestamp = MetadataCodec.decodeDatetime(current);
        var priorTimestamp = MetadataCodec.decodeDatetime(prior);

        if (currentTimestamp.isBefore(priorTimestamp)) {

            var err = String.format(
                    "Value of [%s] cannot be before the previous version: prior = [%s], new = [%s]",
                    ctx.fieldName(),
                    MetadataCodec.ISO_DATETIME_INPUT_FORMAT.format(priorTimestamp),
                    MetadataCodec.ISO_DATETIME_INPUT_FORMAT.format(currentTimestamp));

            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext sameOneOf(Object current, Object prior, ValidationContext ctx) {

        if (!ctx.isOneOf())
            throw new EUnexpected();

        if (ctx.field().getNumber() != ctx.prior().field().getNumber()) {

            var err = String.format(
                    "Selected one of [%s] must not change between versions: prior = [%s], new = [%s]",
                    ctx.oneOf().getName(), ctx.prior().fieldName(), ctx.fieldName());

            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext equalOrLaterVersion(TagSelector current, TagSelector prior, ValidationContext ctx) {

        var initialErrCount = ctx.getErrors().size();

        ctx = ctx.push(OBJECT_TYPE).apply(CommonValidators::exactMatch).pop();
        ctx = ctx.push(OBJECT_ID).apply(CommonValidators::exactMatch).pop();

        if (ctx.getErrors().size() > initialErrCount)
            return ctx;

        ctx = ctx.pushOneOf(OBJECT_CRITERIA)
                .apply(CommonValidators::sameOneOf)
                .applyOneOf(OBJECT_VERSION, CommonValidators::equalOrGreater, Integer.class)
                .applyOneOf(OBJECT_ASOF, CommonValidators::equalOrAfter, DatetimeValue.class)
                .pop();

        if (ctx.getErrors().size() > initialErrCount)
            return ctx;

        ctx = ctx.pushOneOf(TAG_CRITERIA)
                .apply(CommonValidators::sameOneOf)
                .applyOneOf(TAG_VERSION, CommonValidators::equalOrGreater, Integer.class)
                .applyOneOf(TAG_ASOF, CommonValidators::equalOrAfter, DatetimeValue.class)
                .pop();

        return ctx;
    }

    private static String displayValue(Object value) {

        if (value instanceof DatetimeValue)
            return ((DatetimeValue) value).getIsoDatetime();

        if (value instanceof Value) {

            if (TypeSystem.isPrimitive((Value) value))
                return MetadataCodec.decodeValue((Value) value).toString();
        }

        return value.toString();
    }
}
