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

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.validation.ValidationConstants;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationFunction;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtocolMessageEnum;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class CommonValidators {

    static ValidationContext required(ValidationContext ctx) {

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf() && !parentMsg.hasOneof(ctx.oneOf())) {
            var err = String.format("A value is required for [%s]", ctx.fieldName());
            return ctx.error(err);
        }

        if (!parentMsg.hasField(ctx.field())) {
            var err = String.format("A value is required for [%s]", ctx.fieldName());
            return ctx.error(err);
        }

        if (ctx.field().getType() == Descriptors.FieldDescriptor.Type.STRING) {

            var str = (String) ctx.target();

            if (str.isEmpty()) {
                var err = String.format("A value is required for [%s]", ctx.fieldName());
                return ctx.error(err);
            }
        }

        return ctx;
    }

    static ValidationContext omitted(ValidationContext ctx) {

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf()) {

            if (parentMsg.hasOneof(ctx.oneOf())) {
                var err = String.format("A value must not be provided for [%s]", ctx.fieldName());
                return ctx.error(err);
            }
        }
        else {

            if (parentMsg.hasField(ctx.field())) {
                var err = String.format("A value must not be provided for [%s]", ctx.fieldName());
                return ctx.error(err);
            }
        }

        return ctx;
    }

    static ValidationContext optional(ValidationContext ctx) {

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

    static <T> ValidationFunction.Typed<T> equalTo(T other, String errorMessage) {

        return (value, ctx) -> {

            if (!value.equals(other))
                ctx.error(errorMessage);

            return ctx;
        };
    }

    public static ValidationContext primitiveType(BasicType basicType, ValidationContext ctx) {

        if (!TypeSystem.isPrimitive(basicType)) {
            var err = String.format("Type specified in [%s] is not a primitive type: [%s]", ctx.fieldName(), basicType);
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext primitiveType(TypeDescriptor typeDescriptor, ValidationContext ctx) {

        if (!TypeSystem.isPrimitive(typeDescriptor)) {
            var err = String.format("Type specified in [%s] is not a primitive type: [%s]", ctx.fieldName(), typeDescriptor.getBasicType());
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext primitiveValue(Value value, ValidationContext ctx) {

        if (!TypeSystem.isPrimitive(value)) {
            var err = String.format("Value [%s] is not a primitive value: [%s]", ctx.fieldName(), TypeSystem.basicType(value));
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext uuid(String value, ValidationContext ctx) {

        try {
            @SuppressWarnings("unused")
            var uuid = UUID.fromString(value);
        }
        catch (IllegalArgumentException e) {
            var err = String.format("Value of [%s] is not a valid object ID: [%s]", ctx.fieldName(), value);
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext decimal(String value, ValidationContext ctx) {

        if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
            throw new EUnexpected();

        try {

            MetadataCodec.DECIMAL_FORMAT.parse(value);

            return ctx;
        }
        catch (ParseException e) {

            var err = String.format("Value of [%s] is not a valid decimal: [%s] %s",
                    ctx.fieldName(), value, e.getMessage());

            return ctx.error(err);
        }
    }

    static ValidationContext isoDate(String value, ValidationContext ctx) {

        if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
            throw new EUnexpected();

        try {

            MetadataCodec.ISO_DATE_FORMAT.parse(value, LocalDate::from);

            return ctx;
        }
        catch (DateTimeParseException e) {

            var err = String.format("Value of [%s] is not a valid date: [%s] %s",
                    ctx.fieldName(), value, e.getMessage());

            return ctx.error(err);
        }
    }

    static ValidationContext isoDatetime(String value, ValidationContext ctx) {

        if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
            throw new EUnexpected();

        try {

            // Allow for parsing with or without zone offsets

            MetadataCodec.ISO_DATETIME_INPUT_FORMAT.parseBest(value,
                    OffsetDateTime::from,
                    Instant::from);

            return ctx;
        }
        catch (DateTimeParseException e) {

            var err = String.format("Value of [%s] is not a valid datetime: [%s] %s",
                    ctx.fieldName(), value, e.getMessage());

            return ctx.error(err);
        }
    }

    static ValidationContext identifier(String value, ValidationContext ctx) {

        return regexMatch(
                MetadataConstants.VALID_IDENTIFIER, true,
                "is not a valid identifier", value, ctx);
    }

    static ValidationContext notTracReserved(String value, ValidationContext ctx) {

        return regexMatch(
                MetadataConstants.TRAC_RESERVED_IDENTIFIER, false,
                "is a TRAC reserved identifier", value, ctx);
    }

    static ValidationContext mimeType(String value, ValidationContext ctx) {

        // First check the value matches the mime type regex, i.e. has the right form
        ctx = regexMatch(
                ValidationConstants.MIME_TYPE, true,
                "is not a valid mime type", value, ctx);

        if (ctx.failed())
            return ctx;

        // Second check the main part of the type is a registered media type
        var mainType = value.substring(0, value.indexOf("/"));

        if (!ValidationConstants.REGISTERED_MIME_TYPES.contains(mainType)) {
            var err = String.format("Value of [%s] is not a registered mime type: [%s]", ctx.fieldName(), value);
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext fileName(String value, ValidationContext ctx) {

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_CHARS, false,
                "contains illegal characters", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_WHITESPACE, false,
                "contains non-standard whitespace (tab, return, form-feed etc.)", value, ctx);

        // Only check for ctrl characters if illegal whitespace is not present
        // This is because non-standard whitespace is included in the ctrl chars

        // There is a possibility ctrl chars are present as well and will not be reported
        // In that case the error for ctrl chars will become visible when non-standard whitespace is removed

        if (! ValidationConstants.FILENAME_ILLEGAL_WHITESPACE.matcher(value).matches())

            ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_CTRL, false,
                    "contains ASCII control characters", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_START, false,
                "starts with a space character", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_ENDING, false,
                "ends with a space or period character", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_RESERVED, false,
                "is a reserved filename", value, ctx);

        ctx = regexMatch(MetadataConstants.TRAC_RESERVED_IDENTIFIER, false,
                "is a TRAC reserved identifier", value, ctx);

        return ctx;
    }



    private static ValidationContext regexMatch(
            Pattern regex, boolean invertMatch, String desc,
            String value, ValidationContext ctx) {

        if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
            throw new EUnexpected();

        var matcher = regex.matcher(value);

        if (matcher.matches() ^ invertMatch) {
            var err = String.format("Value of [%s] %s: [%s]", ctx.fieldName(), desc, value);
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
            var err = String.format("Value of [%s] cannot be negative: [%s]", ctx.fieldName(), value);
            return ctx.error(err);
        }

        return ctx;
    }

    static ValidationContext optionalTrue(boolean value, ValidationContext ctx) {

        if (!value) {
            var err = String.format("Optional field [%s] must either be omitted or set to 'true'", ctx.fieldName());
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
            var err = String.format("Value of [%s] must be positive: [%s]", ctx.fieldName(), value);
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

    public static ValidationContext fixedObjectVersion(TagSelector selector, ValidationContext ctx) {

        if (selector.hasLatestObject()) {

            var err = String.format(
                    "The [%s] selector must refer to a fixed object version, [latestObject] is not allowed",
                    ctx.fieldName());

            ctx = ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext recognizedEnum(ProtocolMessageEnum protoEnum, ValidationContext ctx) {

        if (protoEnum.getNumber() < 0) {

            var err = String.format("Unrecognised value specified for [%s]: [%s]",
                    ctx.fieldName(), protoEnum.getValueDescriptor().getName());

            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext nonZeroEnum(ProtocolMessageEnum protoEnum, ValidationContext ctx) {

        if (protoEnum.getNumber() <= 0) {

            var err = String.format("Unrecognised value specified for [%s]: [%s]",
                    ctx.fieldName(), protoEnum.getValueDescriptor().getName());

            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext listNotEmpty(List<?> list, ValidationContext ctx) {

        if (list.isEmpty()) {
            var err = String.format("The list [%s] contains no values", ctx.fieldName());
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext mapNotEmpty(Map<?, ?> list, ValidationContext ctx) {

        if (list.isEmpty()) {
            var err = String.format("The map [%s] contains no values", ctx.fieldName());
            return ctx.error(err);
        }

        return ctx;
    }

}
