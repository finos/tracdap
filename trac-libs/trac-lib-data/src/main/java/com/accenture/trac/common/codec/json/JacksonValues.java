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

package com.accenture.trac.common.codec.json;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;


public class JacksonValues {

    public static void parseAndSet(
            FieldVector vector, int row,
            JsonParser parser, JsonToken token)
            throws IOException {

        var field = vector.getField();

        // Make sure not to write nulls into non-nullable fields
        // At the codec level this is a fatal error; we can't generate invalid Arrow data
        // Higher up the stack there can be options to filter / exclude

        var isSet = (token == JsonToken.VALUE_NULL) ? 0 : 1;
        var isNullable = field.isNullable();

        if (isSet == 0 && !isNullable) {

            throw new EUnexpected();  // todo: edatavalidity, field "" cannot be null, field.getName()
        }


        var minorType = vector.getMinorType();

        switch (minorType) {

            case BIT:

                var boolVec = (BitVector) vector;
                var boolVal = (isSet != 0 && parser.getBooleanValue()) ? 1 : 0;

                boolVec.set(row, isSet, boolVal);

                break;

            case BIGINT:

                var int64Vec = (BigIntVector) vector;
                var int64Val = isSet != 0 ? parser.getLongValue() : 0;

                int64Vec.set(row, isSet, int64Val);

                break;

            case INT:

                var int32Vec = (IntVector) vector;
                var int32Val = isSet != 0 ? parser.getIntValue() : 0;

                int32Vec.set(row, isSet, int32Val);

                break;

            case SMALLINT:

                var int16Vec = (SmallIntVector) vector;
                var int16Val = isSet != 0 ? parser.getShortValue() : 0;

                int16Vec.set(row, isSet, int16Val);

                break;

            case TINYINT:

                var int8Vec = (TinyIntVector) vector;
                var int8Val = isSet != 0 ? parser.getByteValue() : 0;

                int8Vec.set(row, isSet, int8Val);

                break;

            case FLOAT8:

                var float64Vec = (Float8Vector) vector;
                var float64Val = isSet != 0 ? parser.getDoubleValue() : 0;

                float64Vec.set(row, isSet, float64Val);

                break;

            case FLOAT4:

                var float32Vec = (Float4Vector) vector;
                var float32Val = isSet != 0 ? parser.getFloatValue() : 0;

                float32Vec.set(row, isSet, float32Val);

                break;

            case DECIMAL:

                var decimal128Vec = (DecimalVector) vector;

                if (isSet == 0)
                    decimal128Vec.setNull(row);

                else {
                    var decimal128Val = parser.getDecimalValue();
                    decimal128Vec.set(row, decimal128Val);
                }

                break;

            case DECIMAL256:

                var decimal256Vec = (Decimal256Vector) vector;

                if (isSet == 0)
                    decimal256Vec.setNull(row);

                else {
                    var decimal256Val = parser.getDecimalValue();
                    decimal256Vec.set(row, decimal256Val);
                }

                break;

            case VARCHAR:

                var varcharVec = (VarCharVector) vector;

                if (isSet == 0)
                    varcharVec.setNull(row);

                else {
                    var varcharVal = parser.getValueAsString();
                    varcharVec.set(row, varcharVal.getBytes(StandardCharsets.UTF_8));
                }

                break;

            case DATEDAY:

                var dateVec = (DateDayVector) vector;

                if (isSet == 0)
                    dateVec.setNull(row);

                else {
                    var dateStr = parser.getValueAsString();
                    var dateVal = LocalDate.parse(dateStr, MetadataCodec.ISO_DATE_FORMAT);
                    var unixEpochDay = (int) dateVal.toEpochDay();

                    dateVec.set(row, isSet, unixEpochDay);
                }

                break;

            default:
                throw new EUnexpected();  // TODO: Data type not supported, field name, arrow type, minor type
        }
    }
}
