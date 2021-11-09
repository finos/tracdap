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

import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.*;

import java.io.IOException;
import java.math.BigDecimal;
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

        int isSet = (token == JsonToken.VALUE_NULL) ? 0 : 1;
        boolean isNullable = field.isNullable();

        if (isSet == 0 && !isNullable) {

            throw new EUnexpected();  // todo: edatavalidity, field "" cannot be null, field.getName()
        }


        var minorType = vector.getMinorType();

        switch (minorType) {

            case BIT:

                BitVector boolVec = (BitVector) vector;
                int boolVal = (isSet != 0 && parser.getBooleanValue()) ? 1 : 0;

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
                double doubleVal = isSet != 0 ? parser.getDoubleValue() : 0;

                doubleVec.set(row, isSet, doubleVal);

                break;

            case FLOAT4:

                Float4Vector floatVec = (Float4Vector) vector;
                float floatVal = isSet != 0 ? parser.getFloatValue() : 0;

                floatVec.set(row, isSet, floatVal);

                break;

            case DECIMAL:

                DecimalVector decimal128Vec = (DecimalVector) vector;

                if (isSet == 0)
                    decimal128Vec.setNull(row);

                else {
                    BigDecimal decimal128Val = parser.getDecimalValue();
                    decimal128Vec.set(row, decimal128Val);
                }

                break;

            case DECIMAL256:

                Decimal256Vector decimal256Vec = (Decimal256Vector) vector;

                if (isSet == 0)
                    decimal256Vec.setNull(row);

                else {
                    BigDecimal decimal256Val = parser.getDecimalValue();
                    decimal256Vec.set(row, decimal256Val);
                }

                break;

            case VARCHAR:

                VarCharVector varcharVec = (VarCharVector) vector;

                if (isSet == 0)
                    varcharVec.setNull(row);

                else {
                    String varcharVal = parser.getValueAsString();
                    varcharVec.set(row, varcharVal.getBytes(StandardCharsets.UTF_8));
                }

                break;

            case DATEDAY:

                DateDayVector dateVec = (DateDayVector) vector;

                if (isSet == 0)
                    dateVec.setNull(row);

                else {
                    String dateStr = parser.getValueAsString();
                    LocalDate dateVal = LocalDate.parse(dateStr, MetadataCodec.ISO_DATE_FORMAT);
                    int unixEpochDay = (int) dateVal.toEpochDay();

                    dateVec.set(row, isSet, unixEpochDay);
                }

                break;

            default:
                throw new EUnexpected();  // TODO: Data type not supported, field name, arrow type, minor type
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

                generator.writeNumber(decimal128Val);

                break;

            case DECIMAL256:

                Decimal256Vector decimal256Vec = (Decimal256Vector) vector;
                BigDecimal decimal256Val = decimal256Vec.getObject(row);

                generator.writeNumber(decimal256Val);

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

            default:

                // TODO: Data type not supported, field name, arrow type, minor type
                var field = vector.getField();
                throw new ETracInternal(field.getName());
        }
    }
}
