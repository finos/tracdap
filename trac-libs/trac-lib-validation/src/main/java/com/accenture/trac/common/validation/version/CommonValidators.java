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

package com.accenture.trac.common.validation.version;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.validation.core.ValidationContext;
import com.accenture.trac.metadata.DatetimeValue;
import com.accenture.trac.metadata.TagSelector;
import com.google.protobuf.Descriptors;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class CommonValidators {

    private static final Descriptors.Descriptor TAG_SELECTOR;
    private static final Descriptors.FieldDescriptor OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor OBJECT_ID;
    private static final Descriptors.FieldDescriptor LATEST_OBJECT;
    private static final Descriptors.FieldDescriptor LATEST_TAG;
    private static final Descriptors.OneofDescriptor OBJECT_CRITERIA;
    private static final Descriptors.OneofDescriptor TAG_CRITERIA;

    static {

        TAG_SELECTOR = TagSelector.getDescriptor();
        OBJECT_TYPE = field(TAG_SELECTOR, TagSelector.OBJECTTYPE_FIELD_NUMBER);
        OBJECT_ID = field(TAG_SELECTOR, TagSelector.OBJECTID_FIELD_NUMBER);
        LATEST_OBJECT = field(TAG_SELECTOR, TagSelector.LATESTOBJECT_FIELD_NUMBER);
        LATEST_TAG = field(TAG_SELECTOR, TagSelector.LATESTTAG_FIELD_NUMBER);
        OBJECT_CRITERIA = LATEST_OBJECT.getContainingOneof();
        TAG_CRITERIA = LATEST_TAG.getContainingOneof();
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
    }

    public static ValidationContext exactMatch(Object current, Object prior, ValidationContext ctx) {

        var equal = Objects.equals(prior, current);

        if (!equal) {

            var err = String.format(
                    "Value of [%s] must not change between versions: prior = [%s], new = [%s]",
                    ctx.fieldName(), prior.toString(), current.toString());

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

        var log = LoggerFactory.getLogger(CommonValidators.class);

        if (!ctx.isOneOf())
            throw new EUnexpected();

        if (ctx.field().getNumber() != ctx.priorField().getNumber()) {

            log.info("one of does not match: {}", ctx.oneOf().getName());

            var err = String.format(
                    "Selected one of [%s] must not change between versions: prior = [%s], new = [%s]",
                    ctx.oneOf().getName(), ctx.priorFieldName(), ctx.fieldName());

            return ctx.error(err);
        }



        log.info("one of does match: {}", ctx.oneOf().getName());

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
                .applyIf(CommonValidators::equalOrGreater, Integer.class, prior.hasObjectVersion())
                .applyIf(CommonValidators::equalOrAfter, DatetimeValue.class, prior.hasObjectAsOf())
                .pop();

        if (ctx.getErrors().size() > initialErrCount)
            return ctx;

        ctx = ctx.pushOneOf(TAG_CRITERIA)
                .apply(CommonValidators::sameOneOf)
                .applyIf(CommonValidators::equalOrGreater, Integer.class, prior.hasTagVersion())
                .applyIf(CommonValidators::equalOrAfter, DatetimeValue.class, prior.hasTagAsOf())
                .pop();

        return ctx;
    }
}
