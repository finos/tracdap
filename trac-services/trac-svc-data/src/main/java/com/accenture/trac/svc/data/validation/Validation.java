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

package com.accenture.trac.svc.data.validation;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.metadata.DecimalValue;
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.TagSelector;
import com.google.protobuf.Descriptors;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import java.util.regex.Pattern;

public class Validation {


    static ValidationContext required(Object value, ValidationContext ctx) {

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf() && !parentMsg.hasOneof(ctx.oneOf())) {
            var err = String.format("A value is required for field [%s]", ctx.fieldName());
            return ctx.error(err);
        }

        if (!parentMsg.hasField(ctx.field())) {
            var err = String.format("A value is required for field [%s]", ctx.fieldName());
            return ctx.error(err);
        }

        if (ctx.field().getType() == Descriptors.FieldDescriptor.Type.STRING) {

            var str = (String) value;

            if (str.isEmpty()) {
                var err = String.format("A value is required for field [%s]", ctx.fieldName());
                return ctx.error(err);
            }
        }

        return ctx;
    }

    static ValidationContext omitted(Object value, ValidationContext ctx) {

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf()) {

            if (parentMsg.hasOneof(ctx.oneOf())) {
                var err = String.format("A value must not be provided for field [%s]", ctx.fieldName());
                return ctx.error(err);
            }
        }
        else {

            if (parentMsg.hasField(ctx.field())) {
                var err = String.format("A value must not be provided for field [%s]", ctx.fieldName());
                return ctx.error(err);
            }
        }

        return ctx;
    }

    static ValidationContext optional(Object value, ValidationContext ctx) {

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf()) {

            if (!parentMsg.hasOneof(ctx.oneOf()))
                return ctx.skip();
        }
        else {

            if (!parentMsg.hasField(ctx.field()))
                return ctx.skip();
        }

        return ctx;
    }

    static ValidationContext uuid(Object value, ValidationContext ctx) {

        try {
            @SuppressWarnings("unused")
            var uuid = UUID.fromString(value.toString());
        }
        catch (IllegalArgumentException e) {
            var err = String.format("Value of field [%s] is not a valid object ID: [%s]", ctx.fieldName(), value);
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext isoDatetime(Object value, ValidationContext ctx) {

        if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
            throw new EUnexpected();

        var str = (String) value;

        try {

            // Allow for parsing with or without zone offsets

            MetadataCodec.ISO_DATETIME_INPUT_FORMAT.parseBest(str,
                    OffsetDateTime::from,
                    LocalDateTime::from);

            return ctx;
        }
        catch (DateTimeParseException e) {

            var err = String.format("Value of field [%s] is not a valid datetime: [%s] %s",
                    ctx.fieldName(), value, e.getMessage());

            return ctx.error(err);
        }
    }

    static ValidationContext identifier(Object value, ValidationContext ctx) {

        return regexMatch(
                ValidationConstants.VALID_IDENTIFIER, true,
                "is not a valid identifier", value, ctx);
    }

    static ValidationContext notTracReserved(Object value, ValidationContext ctx) {

        return regexMatch(
                ValidationConstants.TRAC_RESERVED_IDENTIFIER, false,
                "is a TRAC reserved identifier", value, ctx);
    }

    static ValidationContext mimeType(Object value, ValidationContext ctx) {

        // First check the value matches the mime type regex, i.e. has the right form
        ctx = regexMatch(
                ValidationConstants.MIME_TYPE, true,
                "is not a valid mime type", value, ctx);

        if (ctx.failed())
            return ctx;

        // Second check the main part of the type is a registered media type
        var str = (String) value;
        var mainType = str.substring(0, str.indexOf("/"));

        if (!ValidationConstants.REGISTERED_MIME_TYPES.contains(mainType)) {
            var err = String.format("Value of field [%s] is not a registered mime type: [%s]", ctx.fieldName(), value);
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext fileName(Object value, ValidationContext ctx) {

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_CHARS, false,
                "contains illegal characters", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_WHITESPACE, false,
                "contains non-standard whitespace (tab, return, form-feed etc.)", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_CTRL, false,
                "contains ASCII control characters", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_START, false,
                "starts with a space character", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_ENDING, false,
                "ends with a space or period character", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_RESERVED, false,
                "is a reserved filename", value, ctx);

        ctx = regexMatch(ValidationConstants.TRAC_RESERVED_IDENTIFIER, false,
                "is a TRAC reserved identifier", value, ctx);

        return ctx;
    }



    private static ValidationContext regexMatch(
            Pattern regex, boolean invertMatch, String desc,
            Object value, ValidationContext ctx) {

        if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
            throw new EUnexpected();

        var matcher = regex.matcher((String) value);

        if (matcher.matches() ^ invertMatch) {
            var err = String.format("Value of field [%s] %s: [%s]", ctx.fieldName(), desc, value);
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext notNegative(Object value, ValidationContext ctx) {

        boolean negative;

        switch (ctx.field().getJavaType()) {

            case INT: negative = (int) value < 0; break;
            case LONG: negative = (long) value < 0; break;
            case FLOAT: negative = (float) value < 0; break;
            case DOUBLE: negative = (double) value < 0; break;

            case MESSAGE:

                var msgType = ctx.field().getMessageType();

                // Handle DecimalValue messages, otherwise drop through to the default case

                if (msgType.equals(DecimalValue.getDescriptor())) {
                    var decimalMsg = (DecimalValue) value;
                    var decimalValue = new BigDecimal(decimalMsg.getDecimal());
                    negative = ! decimalValue.abs().equals(decimalValue);
                    break;
                }

            default:
                throw new EUnexpected();
        }

        if (negative) {
            var err = String.format("Value of field [%s] cannot be negative: [%s]", ctx.fieldName(), value);
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext positive(Object value, ValidationContext ctx) {

        boolean positive;

        switch (ctx.field().getJavaType()) {

            case INT: positive = (int) value > 0; break;
            case LONG: positive = (long) value > 0; break;
            case FLOAT: positive = (float) value > 0; break;
            case DOUBLE: positive = (double) value > 0; break;

            case MESSAGE:

                var msgType = ctx.field().getMessageType();

                // Handle DecimalValue messages, otherwise drop through to the default case

                if (msgType.equals(DecimalValue.getDescriptor())) {
                    var decimalMsg = (DecimalValue) value;
                    var decimalValue = new BigDecimal(decimalMsg.getDecimal());
                    positive = decimalValue.abs().equals(decimalValue) && !decimalValue.equals(BigDecimal.ZERO);
                    break;
                }

            default:
                throw new EUnexpected();
        }

        if (!positive) {
            var err = String.format("Value of field [%s] must be positive: [%s]", ctx.fieldName(), value);
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationFunction.Typed<TagSelector> selectorType(ObjectType requiredType) {

        return (selector, ctx) -> selectorType(requiredType, selector, ctx);
    }

    public static ValidationContext selectorType(ObjectType requiredType, TagSelector selector, ValidationContext ctx) {

        if (!selector.getObjectType().equals(requiredType)) {
            var err = String.format("Wrong object type for [%s]: expected [%s], got [%s]",
                    ctx.fieldName(), requiredType, selector.getObjectType());
            return ctx.error(err);
        }

        return ctx;
    }

}
