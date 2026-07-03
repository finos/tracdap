/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.codec.text.consumers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.finos.tracdap.common.metadata.MetadataCodec;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;

public class JsonParsing {

    // Values that are recognised as NaN / infinity during decoding
    private static final List<String> NAN_VALUES = List.of("nan", "na");
    private static final List<String> INFINITY_VALUES = List.of("inf", "infinity");

    public static boolean parseBoolean(JsonParser parser) throws IOException {

        if (parser.currentToken() == JsonToken.VALUE_NULL)
            return false;

        if (parser.currentToken() == JsonToken.VALUE_TRUE)
            return true;

        if (parser.currentToken() == JsonToken.VALUE_FALSE)
            return false;

        if (parser.currentToken() == JsonToken.VALUE_STRING) {
            String boolStr = parser.getValueAsString();
            return Boolean.parseBoolean(boolStr);
        }

        if (parser.currentToken() == VALUE_NUMBER_INT) {
            int intValue = parser.getIntValue();
            if (intValue == 0 || intValue == 1)
                return intValue == 1;
        }

        throw new JsonParseException(parser, "Invalid boolean value", parser.currentLocation());
    }

    // Integer parsing helpers
    //
    // These accept both numeric tokens (VALUE_NUMBER_INT / VALUE_NUMBER_FLOAT) and string tokens.
    // String tokens must be handled because the CSV parser presents typed columns as strings
    // (jackson-dataformat-csv stopped emitting numeric tokens for schema NUMBER columns in 2.21).
    // This mirrors the handling already used for floating point and decimal values below.

    public static byte parseByte(JsonParser parser) throws IOException {

        var token = parser.currentToken();

        try {
            if (token == VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)
                return parser.getByteValue();
            else if (token == JsonToken.VALUE_STRING)
                return Byte.parseByte(parser.getValueAsString());
            else
                throw integerParseError(parser, token);
        }
        catch (NumberFormatException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static short parseShort(JsonParser parser) throws IOException {

        var token = parser.currentToken();

        try {
            if (token == VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)
                return parser.getShortValue();
            else if (token == JsonToken.VALUE_STRING)
                return Short.parseShort(parser.getValueAsString());
            else
                throw integerParseError(parser, token);
        }
        catch (NumberFormatException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static int parseInt(JsonParser parser) throws IOException {

        var token = parser.currentToken();

        try {
            if (token == VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)
                return parser.getIntValue();
            else if (token == JsonToken.VALUE_STRING)
                return Integer.parseInt(parser.getValueAsString());
            else
                throw integerParseError(parser, token);
        }
        catch (NumberFormatException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static long parseLong(JsonParser parser) throws IOException {

        var token = parser.currentToken();

        try {
            if (token == VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)
                return parser.getLongValue();
            else if (token == JsonToken.VALUE_STRING)
                return Long.parseLong(parser.getValueAsString());
            else
                throw integerParseError(parser, token);
        }
        catch (NumberFormatException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static BigInteger parseBigInteger(JsonParser parser) throws IOException {

        var token = parser.currentToken();

        try {
            if (token == VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)
                return parser.getBigIntegerValue();
            else if (token == JsonToken.VALUE_STRING)
                return new BigInteger(parser.getValueAsString());
            else
                throw integerParseError(parser, token);
        }
        catch (NumberFormatException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    private static JsonParseException integerParseError(JsonParser parser, JsonToken token) {

        var msg = "Parsing failed: Expected an integer value, got [" + token.name() + "]";
        return new JsonParseException(parser, msg, parser.currentLocation());
    }

    public static float parseFloat4(JsonParser parser) throws IOException {

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

                var msg = "Parsing failed: Excepted a floating point value, got [" + parser.currentToken().name() + "]";
                throw new JsonParseException(parser, msg, parser.currentLocation());
            }
        }
        catch (NumberFormatException e) {

            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static double parseFloat8(JsonParser parser) throws IOException {

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

                var msg = "Parsing failed: Excepted a floating point value, got [" + parser.currentToken().name() + "]";
                throw new JsonParseException(parser, msg, parser.currentLocation());
            }
        }
        catch (NumberFormatException e) {

            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static BigDecimal parseBigDecimal(JsonParser parser, int scale) throws IOException {

        try {

            var token = parser.currentToken();

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

    public static LocalDate parseLocalDate(JsonParser parser) throws IOException {

        try {
            String dateStr = parser.getValueAsString();
            return LocalDate.parse(dateStr, MetadataCodec.ISO_DATE_FORMAT);
        }
        catch (DateTimeParseException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }

    public static LocalDateTime parseDatetimeNoZone(JsonParser parser) throws IOException {

        try {
            String datetimeStr = parser.getValueAsString();
            return LocalDateTime.parse(datetimeStr, MetadataCodec.ISO_DATETIME_INPUT_NO_ZONE_FORMAT);
        }
        catch (DateTimeParseException e) {
            throw new JsonParseException(parser, e.getMessage(), parser.currentLocation(), e);
        }
    }
}
