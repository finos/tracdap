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

import com.google.protobuf.ByteString;
import com.google.protobuf.MapEntry;
import org.finos.tracdap.common.exception.ETracInternal;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.finos.tracdap.common.validation.ValidationConstants.LABEL_LENGTH_LIMIT;

public class CommonValidators {

    public static ValidationContext required(ValidationContext ctx) {

        return required(ctx, null);
    }

    private static ValidationContext required(ValidationContext ctx, String qualifier) {

        var err = qualifier != null
                ? String.format("A value is required for [%s] %s", ctx.fieldName(), qualifier)
                : String.format("A value is required for [%s]", ctx.fieldName());

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf()) {

            if (!parentMsg.hasOneof(ctx.oneOf()))
                return ctx.error(err);
        }
        else if (ctx.isRepeated()) {

            if (parentMsg.getRepeatedFieldCount(ctx.field()) == 0)
                return ctx.error(err);
        }
        else {

            if (!parentMsg.hasField(ctx.field()))
                return ctx.error(err);

            // For single string values, required means the string cannot be empty
            if (ctx.field().getType() == Descriptors.FieldDescriptor.Type.STRING) {

                var str = (String) ctx.target();

                if (str.isEmpty())
                    return ctx.error(err);
            }
        }

        return ctx;
    }

    public static ValidationContext omitted(ValidationContext ctx) {

        return omitted(ctx, null);
    }

    private static ValidationContext omitted(ValidationContext ctx, String qualifier) {

        var err = qualifier != null
                ? String.format("A value cannot be given for [%s] %s", ctx.fieldName(), qualifier)
                : String.format("A value cannot be given for [%s]", ctx.fieldName());

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf()) {

            if (parentMsg.hasOneof(ctx.oneOf())) {
                return ctx.error(err);
            }
        }
        else if (ctx.isRepeated()) {

            if (parentMsg.getRepeatedFieldCount(ctx.field()) != 0)
                return ctx.error(err);
        }
        else {

            if (parentMsg.hasField(ctx.field()))
                return ctx.error(err);
        }

        // Field is not present, skip remaining validators

        return ctx.skip();
    }

    public static ValidationContext optional(ValidationContext ctx) {

        // If field is not present, skip remaining validators

        var parentMsg = ctx.parentMsg();

        if (ctx.isOneOf()) {

            if (!parentMsg.hasOneof(ctx.oneOf()))
                return ctx.skip();
        }
        else if (ctx.isRepeated()) {

            if (parentMsg.getRepeatedFieldCount(ctx.field()) == 0)
                return ctx.skip();
        }
        else {

            if (!parentMsg.hasField(ctx.field()))
                return ctx.skip();
        }

        return ctx;
    }

    public static ValidationFunction.Basic ifAndOnlyIf(boolean condition, String qualifier) {

        return ifAndOnlyIf(condition, qualifier, false);
    }

    public static ValidationFunction.Basic ifAndOnlyIf(boolean condition, String qualifier, boolean inverted) {

        var positiveQualifier = (inverted ? "unless " : "when ") + qualifier;
        var negativeQualifier = (inverted ? "when " : "unless ") + qualifier;

        if (condition)
            return ctx -> required(ctx, positiveQualifier);
        else
            return ctx -> omitted(ctx, negativeQualifier);
    }

    public static <T> ValidationFunction.Typed<T> equalTo(T other, String errorMessage) {

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

    public static ValidationContext uuid(String value, ValidationContext ctx) {

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

    public static ValidationContext decimal(String value, ValidationContext ctx) {

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

    public static ValidationContext isoDate(String value, ValidationContext ctx) {

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

    public static ValidationContext isoDatetime(String value, ValidationContext ctx) {

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

    public static ValidationContext labelLengthLimit(String value, ValidationContext ctx) {

        if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
            throw new EUnexpected();

        if(value.length()>LABEL_LENGTH_LIMIT) {
            var err = String.format("Label exceeds maximum length limit (%d characters)", LABEL_LENGTH_LIMIT);
            return ctx.error(err);
        }

        return ctx;

    }

    public static ValidationContext identifier(String value, ValidationContext ctx) {

        return regexMatch(
                MetadataConstants.VALID_IDENTIFIER, true,
                "is not a valid identifier", value, ctx);
    }

    public static ValidationContext notTracReserved(String value, ValidationContext ctx) {

        return regexMatch(
                MetadataConstants.TRAC_RESERVED_IDENTIFIER, false,
                "is a TRAC reserved identifier", value, ctx);
    }

    public static ValidationContext mimeType(String value, ValidationContext ctx) {

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

    public static ValidationContext fileName(String value, ValidationContext ctx) {

        ctx = ctx.apply(CommonValidators::pathAlwaysIllegal);

        // Particular constraints for file names

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_CHARS, false,
                "contains characters not allowed in a filename (:, / or \\)", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_START, false,
                "starts with a space character", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_ENDING, false,
                "ends with a space or period character", value, ctx);

        ctx = regexMatch(ValidationConstants.FILENAME_RESERVED, false,
                "is a reserved filename", value, ctx);

        return ctx;
    }

    public static ValidationContext relativePath(String path, ValidationContext ctx) {

        ctx = ctx.apply(CommonValidators::pathAlwaysIllegal);

        if (path.contains(ValidationConstants.UNIX_PATH_SEPARATOR) &&
                path.contains(ValidationConstants.WINDOWS_PATH_SEPARATOR)) {

            ctx = ctx.error("Path contains both Windows and Unix separators (use one or the other, not both)");
        }

        // Particular constraints for relative paths

        ctx = regexMatch(ValidationConstants.RELATIVE_PATH_ILLEGAL_CHARS, false,
                "contains characters not allowed in a relative path (:)", path, ctx);

        ctx = regexMatch(ValidationConstants.RELATIVE_PATH_IS_ABSOLUTE, false,
                "is an absolute path", path, ctx);

        ctx = regexMatch(ValidationConstants.RELATIVE_PATH_DOUBLE_SLASH, false,
                "contains a double slash", path, ctx);

        // Only attempt segment validation if there are no failures at the whole path level

        if (ctx.failed())
            return ctx;

        var segments = path.split(ValidationConstants.PATH_SEPARATORS.pattern());

        var singleDotPredicate = ValidationConstants.PATH_SINGLE_DOT.asPredicate();
        var doubleDotPredicate = ValidationConstants.PATH_SINGLE_DOT.asPredicate();

        for (var segment : segments) {

            ctx = regexMatch(ValidationConstants.PATH_DOUBLE_DOT, false,
                    "segment refers to parent directory", segment, ctx);

            // Sometimes an empty relative path is needed, e.g. for the root of a model directory or storage bin
            // In this case, a path of "." or "./" is allowed
            // However, subdirectories such as ./sub_dir can and should be normalized to just "sub_dir"
            // So, only allow single dot when the path consists of exactly one segment

            if (segments.length > 1) {
                ctx = regexMatch(ValidationConstants.PATH_SINGLE_DOT, false,
                        "segment refers to itself and can be omitted", segment, ctx);
            }

            // Avoid multiple error messages per segment
            if (singleDotPredicate.test(segment) || doubleDotPredicate.test(segment))
                continue;

            ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_CHARS, false,
                    "segment contains characters illegal characters", segment, ctx);

            ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_START, false,
                    "segment starts with a space character", segment, ctx);

            ctx = regexMatch(ValidationConstants.FILENAME_ILLEGAL_ENDING, false,
                    "segment ends with a space or period character", segment, ctx);

            ctx = regexMatch(ValidationConstants.FILENAME_RESERVED, false,
                    "segment is a reserved filename", segment, ctx);
        }

        return ctx;
    }

    private static ValidationContext pathAlwaysIllegal(String path, ValidationContext ctx) {

        ctx = regexMatch(ValidationConstants.PATH_ILLEGAL_CHARS, false,
                "contains illegal characters", path, ctx);

        ctx = regexMatch(ValidationConstants.PATH_ILLEGAL_WHITESPACE, false,
                "contains non-standard whitespace (tab, return, form-feed etc.)", path, ctx);

        // Only check for ctrl characters if illegal whitespace is not present
        // This is because non-standard whitespace is included in the ctrl chars

        // There is a possibility ctrl chars are present as well and will not be reported
        // In that case the error for ctrl chars will become visible when non-standard whitespace is removed

        if (! ValidationConstants.PATH_ILLEGAL_WHITESPACE.matcher(path).matches())

            ctx = regexMatch(ValidationConstants.PATH_ILLEGAL_CTRL, false,
                    "contains ASCII control characters", path, ctx);

        return ctx;
    }

    private static ValidationContext regexMatch(
            Pattern regex, boolean invertMatch, String desc,
            String value, ValidationContext ctx) {

        var matcher = regex.matcher(value);

        if (matcher.matches() ^ invertMatch) {
            var err = String.format("[%s] %s: [%s]", ctx.fieldName(), desc, value);
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext notNegative(Object value, ValidationContext ctx) {

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

    public static ValidationContext optionalTrue(boolean value, ValidationContext ctx) {

        if (!value) {
            var err = String.format("Optional field [%s] must either be omitted or set to 'true'", ctx.fieldName());
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext positive(Object value, ValidationContext ctx) {

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

    public static ValidationContext listNotEmpty(ValidationContext ctx) {

        if (!ctx.field().isRepeated() || ctx.field().isMapField())
            throw new ETracInternal("Validator [listNotEmpty] can only be applied to repeated fields (and not map fields)");

        var nItems = ctx.parentMsg().getRepeatedFieldCount(ctx.field());

        if (nItems == 0) {
            var err = String.format("The list of [%s] contains no items", ctx.fieldName());
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext mapNotEmpty(ValidationContext ctx) {

        if (!ctx.field().isMapField())
            throw new ETracInternal("Validator [mapNotEmpty] can only be applied to map fields");

        var nItems = ctx.parentMsg().getRepeatedFieldCount(ctx.field());

        if (nItems == 0) {
            var err = String.format("The map of [%s] contains no entries", ctx.fieldName());
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext bytesNotEmpty(ByteString content, ValidationContext ctx) {

        if (content.isEmpty()) {

            return ctx.error(String.format("[%s] cannot be empty", ctx.fieldName()));
        }

        return ctx;
    }

    public static ValidationContext caseInsensitiveDuplicates(ValidationContext ctx) {

        var itemCount = ctx.parentMsg().getRepeatedFieldCount(ctx.field());
        var knownItems = new HashMap<String, String>(itemCount);

        try (var items = caseInsensitiveDuplicatesItems(ctx)) {

            items.forEach(itemCase -> {

                var lowerCase = itemCase.toLowerCase();
                var priorCase = knownItems.getOrDefault(lowerCase, null);

                if (priorCase == null) {
                    knownItems.put(lowerCase, itemCase);
                }
                else if (itemCase.equals(priorCase)) {

                    var err = String.format(
                            "[%s] is included more than once in [%s]",
                            itemCase, ctx.fieldName());

                    ctx.error(err);
                }
                else {

                    var err = String.format(
                            "In [%s], [%s] and [%s] differ only by case",
                            ctx.fieldName(), priorCase, itemCase);

                    ctx.error(err);
                }
            });
        }

        return ctx;
    }

    private static Stream<String> caseInsensitiveDuplicatesItems(ValidationContext ctx) {

        if (ctx.isMap()) {

            var keyField = ctx.field().getMessageType().findFieldByNumber(1);

            if (keyField.getType() != Descriptors.FieldDescriptor.Type.STRING)
                throw new ETracInternal("[caseInsensitiveDuplicates] can only be applied to repeated string fields or maps with string keys");

            // Different behaviour depending on whether the field was pushed as a real map or list of entries
            if (ctx.target() instanceof Map) {

                var map = (Map<?, ?>) ctx.target();

                return map.keySet().stream()
                        .map(Object::toString);
            }
            else {

                @SuppressWarnings("unchecked")
                var entries = (List<MapEntry<?, ?>>) ctx.target();

                return entries.stream()
                        .map(MapEntry::getKey)
                        .map(Object::toString);
            }
        }
        else if (ctx.isRepeated()) {

            if (ctx.field().getType() != Descriptors.FieldDescriptor.Type.STRING)
                throw new ETracInternal("[caseInsensitiveDuplicates] can only be applied to repeated string fields or maps with string keys");

            @SuppressWarnings("unchecked")
            var list = (List<String>) ctx.target();

            return list.stream();
        }
        else
            throw new ETracInternal("[caseInsensitiveDuplicates] can only be applied to repeated string fields or maps with string keys");
    }

}
