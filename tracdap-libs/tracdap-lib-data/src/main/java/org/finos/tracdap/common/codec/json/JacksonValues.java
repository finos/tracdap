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

package org.finos.tracdap.common.codec.json;

import org.apache.arrow.vector.types.Types;
import org.finos.tracdap.common.exception.EDataTypeNotSupported;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataCodec;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;


public class JacksonValues {

    private static final Logger log = LoggerFactory.getLogger(JacksonValues.class);

    private static final List<String> NAN_VALUES = List.of("nan, na");
    private static final List<String> INFINITY_VALUES = List.of("inf", "infinity");


    public static void parseAndSet(
            FieldVector vector, int row,
            JsonParser parser, JsonToken token)
            throws IOException {

        var field = vector.getField();

        // Make sure not to write nulls into non-nullable fields
        // At the codec level this is a fatal error; we can't generate invalid Arrow data
        // Higher up the stack there can be options to filter / exclude

        int isSet = (token == JsonToken.VALUE_NULL) ? 0 : 1;
        boolean isNullable = field.isNullable();

        if (isSet == 0 && !isNullable) {

            var msg = "Parsing failed: Got a null value for non-nullable field [" + vector.getField().getName() + "]";
            throw new JsonParseException(parser, msg, parser.currentLocation());
        }


        var minorType = vector.getMinorType();

        switch (minorType) {

            case BIT:

                BitVector boolVec = (BitVector) vector;
                int boolVal;

                if (token == JsonToken.VALUE_NULL)
                    boolVal = 0;
                else if (token == JsonToken.VALUE_TRUE)
                    boolVal = 1;
                else if (token == JsonToken.VALUE_FALSE)
                    boolVal = 0;
                else if (token == JsonToken.VALUE_STRING) {
                    String boolStr = parser.getValueAsString();
                    boolVal = Boolean.parseBoolean(boolStr) ? 1 : 0;
                }
                else if (token == VALUE_NUMBER_INT) {
                    boolVal = parser.getIntValue();
                    if (boolVal != 0 && boolVal != 1)
                        throw new JsonParseException(parser, "Invalid boolean value", parser.currentLocation());
                }
                else
                    throw new JsonParseException(parser, "Invalid boolean value", parser.currentLocation());

                boolVec.set(row, isSet, boolVal);

                break;

            case BIGINT:

                BigIntVector int64Vec = (BigIntVector) vector;
                long int64Val = isSet != 0 ? parser.getLongValue() : 0;

                int64Vec.set(row, isSet, int64Val);

                break;

            case INT:

                IntVector int32Vec = (IntVector) vector;
                int int32Val = isSet != 0 ? parser.getIntValue() : 0;

                int32Vec.set(row, isSet, int32Val);

                break;

            case SMALLINT:

                SmallIntVector int16Vec = (SmallIntVector) vector;
                short int16Val = isSet != 0 ? parser.getShortValue() : 0;

                int16Vec.set(row, isSet, int16Val);

                break;

            case TINYINT:

                TinyIntVector int8Vec = (TinyIntVector) vector;
                byte int8Val = isSet != 0 ? parser.getByteValue() : 0;

                int8Vec.set(row, isSet, int8Val);

                break;

            case FLOAT8:

                Float8Vector doubleVec = (Float8Vector) vector;
                double doubleVal = isSet != 0 ? parseFloat8(parser, token) : 0;

                doubleVec.set(row, isSet, doubleVal);

                break;

            case FLOAT4:

                Float4Vector floatVec = (Float4Vector) vector;
                float floatVal = isSet != 0 ? parseFloat4(parser, token) : 0;

                floatVec.set(row, isSet, floatVal);

                break;

            case DECIMAL:

                DecimalVector decimal128Vec = (DecimalVector) vector;

                if (isSet == 0)
                    decimal128Vec.setNull(row);

                else {
                    BigDecimal decimal128Val = parseBigDecimal(parser, token, decimal128Vec.getScale());
                    decimal128Vec.set(row, decimal128Val);
                }

                break;

            case DECIMAL256:

                Decimal256Vector decimal256Vec = (Decimal256Vector) vector;

                if (isSet == 0)
                    decimal256Vec.setNull(row);

                else {
                    BigDecimal decimal256Val = parseBigDecimal(parser, token, decimal256Vec.getScale());
                    decimal256Vec.set(row, decimal256Val);
                }

                break;

            case VARCHAR:

                VarCharVector varcharVec = (VarCharVector) vector;

                if (isSet == 0)
                    varcharVec.setNull(row);

                else {
                    String varcharVal = parser.getValueAsString();

                    // For variable width vectors, the required size of the content buffer is not known up front
                    // Arrow makes an initial guess, but sometimes it will need to reallocate on write
                    // So, we need to call setSafe() instead of set(), to avoid a buffer overflow

                    varcharVec.setSafe(row, varcharVal.getBytes(StandardCharsets.UTF_8));
                }

                break;

            case DATEDAY:

                DateDayVector dateVec = (DateDayVector) vector;

                if (isSet == 0)
                    dateVec.setNull(row);

                else {

                    LocalDate dateVal = parseLocalDate(parser);
                    int unixEpochDay = (int) dateVal.toEpochDay();

                    dateVec.set(row, isSet, unixEpochDay);
                }

                break;

            case TIMESTAMPMILLI:

                TimeStampMilliVector timeStampMVec = (TimeStampMilliVector) vector;

                if (isSet == 0)
                    timeStampMVec.setNull(row);

                else {

                    LocalDateTime datetimeNoZone = parseDatetimeNoZone(parser);
                    long unixEpochMillis =
                            (datetimeNoZone.toEpochSecond(ZoneOffset.UTC) * 1000) +
                            (datetimeNoZone.getNano() / 1000000);

                    timeStampMVec.set(row, unixEpochMillis);
                }

                break;

                // For handling TZ type:
                // 1. ArrowType.Timestamp mtzType = (ArrowType.Timestamp) field.getType();
                // 2. ZoneOffset mtzOffset = ZoneOffset.of(mtzType.getTimezone());

            default:

                // This error does not relate to the data, only to the target column type
                // So, do not include parse location in the error message

                var err = String.format(
                        "Data type not supported for field: [%s] %s (%s)",
                        field.getName(), field.getType(), vector.getMinorType());

                log.error(err);
                throw new EDataTypeNotSupported(err);
        }
    }

    public static void setEmptyString(FieldVector vector, int row) {

        var minorType = vector.getMinorType();

        if (minorType == Types.MinorType.VARCHAR) {

            VarCharVector varcharVec = (VarCharVector) vector;
            varcharVec.setSafe(row, "".getBytes(StandardCharsets.UTF_8));
        }
        else {

                throw new EUnexpected();
        }
    }

    static private float parseFloat4(JsonParser parser, JsonToken token) throws IOException {

        try {

            if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT ||
                parser.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {

                return parser.getFloatValue();
            }
            else if (parser.currentToken() == JsonToken.VALUE_STRING) {

                // Jackson does not recognise the default infinity value encoded by Arrow, which is "inf"
                // This logic allows for special values encoded using a variety of standard words

                var lowerToken = parser.getValueAsString().toLowerCase();

                if (NAN_VALUES.contains(lowerToken))
                    return Float.NaN;

                if (INFINITY_VALUES.contains(lowerToken))
                    return Float.POSITIVE_INFINITY;

                if (lowerToken.startsWith("-") && INFINITY_VALUES.contains(lowerToken.substring(1)))
                    return Float.NEGATIVE_INFINITY;

                return Float.parseFloat(parser.getValueAsString());
            }
            else {

                var msg = "Parsing failed: Excepted a floating point value, got [" + token.name() + "]";
                throw new JsonParseException(parser, msg, parser.currentLocation());
            }
        }
        catch (NumberFormatException e) {

            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    static private double parseFloat8(JsonParser parser, JsonToken token) throws IOException {

        try {

            if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT ||
                parser.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {

                return parser.getDoubleValue();
            }
            else if (parser.currentToken() == JsonToken.VALUE_STRING) {

                // Jackson does not recognise the default infinity value encoded by Arrow, which is "inf"
                // This logic allows for special values encoded using a variety of standard words

                var lowerToken = parser.getValueAsString().toLowerCase();

                if (NAN_VALUES.contains(lowerToken))
                    return Double.NaN;

                if (INFINITY_VALUES.contains(lowerToken))
                    return Double.POSITIVE_INFINITY;

                if (lowerToken.startsWith("-") && INFINITY_VALUES.contains(lowerToken.substring(1)))
                    return Double.NEGATIVE_INFINITY;

                return Double.parseDouble(parser.getValueAsString());
            }
            else {

                var msg = "Parsing failed: Excepted a floating point value, got [" + token.name() + "]";
                throw new JsonParseException(parser, msg, parser.currentLocation());
            }
        }
        catch (NumberFormatException e) {

            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    static private BigDecimal parseBigDecimal(JsonParser parser, JsonToken token, int scale) throws IOException {

        try {

            BigDecimal decimalVal;

            if (token == VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)
                decimalVal = parser.getDecimalValue();
            else if (token == JsonToken.VALUE_STRING)
                decimalVal = new BigDecimal(parser.getValueAsString());
            else {
                var msg = "Parsing failed: Excepted a decimal, got [" + token.name() + "]";
                throw new JsonParseException(parser, msg, parser.currentLocation());
            }


            if (decimalVal.scale() == scale)
                return decimalVal;

            else
                // Scale the decimal to match the scale of the arrow vector
                return decimalVal.setScale(scale, RoundingMode.UNNECESSARY);
        }
        catch (NumberFormatException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    static private LocalDate parseLocalDate(JsonParser parser) throws IOException {

        try {
            String dateStr = parser.getValueAsString();
            return LocalDate.parse(dateStr, MetadataCodec.ISO_DATE_FORMAT);
        }
        catch (DateTimeParseException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    static private LocalDateTime parseDatetimeNoZone(JsonParser parser) throws IOException {

        try {
            String datetimeStr = parser.getValueAsString();
            return LocalDateTime.parse(datetimeStr, MetadataCodec.ISO_DATETIME_INPUT_NO_ZONE_FORMAT);
        }
        catch (DateTimeParseException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static void getAndGenerate(FieldVector vector, int row, JsonGenerator generator) throws IOException {

        boolean isNull = vector.isNull(row);

        if (isNull) {
            generator.writeNull();
            return;
        }

        var minorType = vector.getMinorType();

        switch (minorType) {

            case BIT:

                BitVector boolVec = (BitVector) vector;
                int boolVal = boolVec.get(row);

                generator.writeBoolean(boolVal != 0);

                break;

            case BIGINT:

                BigIntVector int64Vec = (BigIntVector) vector;
                long int64Val = int64Vec.get(row);

                generator.writeNumber(int64Val);

                break;

            case INT:

                IntVector int32Vec = (IntVector) vector;
                int int32Val = int32Vec.get(row);

                generator.writeNumber(int32Val);

                break;

            case SMALLINT:

                SmallIntVector int16Vec = (SmallIntVector) vector;
                short int16Val = int16Vec.get(row);

                generator.writeNumber(int16Val);

                break;

            case TINYINT:

                TinyIntVector int8Vec = (TinyIntVector) vector;
                byte int8Val = int8Vec.get(row);

                generator.writeNumber(int8Val);

                break;

            case FLOAT8:

                Float8Vector doubleVec = (Float8Vector) vector;
                double doubleVal = doubleVec.get(row);

                generator.writeNumber(doubleVal);

                break;

            case FLOAT4:

                Float4Vector floatVec = (Float4Vector) vector;
                float floatVal = floatVec.get(row);

                generator.writeNumber(floatVal);

                break;

            case DECIMAL:

                DecimalVector decimal128Vec = (DecimalVector) vector;
                BigDecimal decimal128Val = decimal128Vec.getObject(row);

                // This will render zeroes as "0" when the scale is large, preferable to 0e-12
                // For small scales use the default rendering, particularly currency with scale == 2

                if (decimal128Vec.getScale() > 3 && BigDecimal.ZERO.compareTo(decimal128Val) == 0)
                    generator.writeString(BigDecimal.ZERO.toString());
                else
                    generator.writeString(decimal128Val.toString());

                break;

            case DECIMAL256:

                Decimal256Vector decimal256Vec = (Decimal256Vector) vector;
                BigDecimal decimal256Val = decimal256Vec.getObject(row);

                if (decimal256Vec.getScale() > 3 && BigDecimal.ZERO.compareTo(decimal256Val) == 0)
                    generator.writeString(BigDecimal.ZERO.toString());
                else
                    generator.writeString(decimal256Val.toString());

                break;

            case VARCHAR:

                VarCharVector varcharVec = (VarCharVector) vector;
                String varcharVal = new String(varcharVec.get(row), StandardCharsets.UTF_8);

                generator.writeString(varcharVal);

                break;

            case DATEDAY:

                DateDayVector dateVec = (DateDayVector) vector;
                int unixEpochDay = dateVec.get(row);
                LocalDate dateVal = LocalDate.ofEpochDay(unixEpochDay);
                String dateStr = dateVal.format(MetadataCodec.ISO_DATE_FORMAT);

                generator.writeString(dateStr);

                break;

            case TIMESTAMPMILLI:

                TimeStampMilliVector timeStampMVec = (TimeStampMilliVector) vector;

                long unixEpochMillis = timeStampMVec.get(row);
                long unixEpochSec = unixEpochMillis / 1000;
                int nanos = ((int) (unixEpochMillis - (unixEpochSec * 1000))) * 1000000;

                LocalDateTime localDatetimeVal = LocalDateTime.ofEpochSecond(unixEpochSec, nanos, ZoneOffset.UTC);
                OffsetDateTime offsetDatetimeVal = localDatetimeVal.atOffset(ZoneOffset.UTC);
                String datetimeStr = MetadataCodec.ISO_DATETIME_NO_ZONE_FORMAT.format(offsetDatetimeVal);

                generator.writeString(datetimeStr);

                break;

            // For handling TZ type:
            // 1. ArrowType.Timestamp mtzType = (ArrowType.Timestamp) field.getType();
            // 2. ZoneOffset mtzOffset = ZoneOffset.of(mtzType.getTimezone());

            default:

                // This error does not relate to the data, only to the target column type
                // So, do not include parse location in the error message

                var field = vector.getField();

                var err = String.format(
                        "Data type not supported for field: [%s] %s (%s)",
                        field.getName(), field.getType(), vector.getMinorType());

                log.error(err);
                throw new EDataTypeNotSupported(err);
        }
    }
}
