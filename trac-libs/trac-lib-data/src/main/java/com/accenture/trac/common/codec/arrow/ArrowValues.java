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

package com.accenture.trac.common.codec.arrow;

import com.accenture.trac.common.exception.EUnexpected;
import org.apache.arrow.vector.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;

public class ArrowValues {

    public static void setValue(VectorSchemaRoot root, int row, int col, Object obj) {

        var vector = root.getVector(col);
        var arrowTypeId = vector.getField().getType().getTypeID();

        var isSet = (obj != null) ? 1 : 0;

        switch (arrowTypeId) {

            case Bool:

                var boolVec = (BitVector) vector;
                var boolVal = (isSet != 0 && (Boolean) obj) ? 1 : 0;

                boolVec.set(row, isSet, boolVal);

                break;

            case Int:

                var intVec = (BigIntVector) vector;
                var intVal = isSet != 0 ? (Long) obj : 0;

                intVec.set(row, isSet, intVal);

                break;

            case FloatingPoint:

                var floatVec = (Float8Vector) vector;
                var floatVal = isSet != 0 ? (Double) obj : 0;

                floatVec.set(row, isSet, floatVal);

                break;

            case Decimal:

                var decimalVec = (Decimal256Vector) vector;
                var decimalVal = (BigDecimal) obj;

                if (isSet == 0)
                    decimalVec.setNull(row);
                else
                    decimalVec.set(row, decimalVal);

                break;

            case Utf8:

                var varcharVec = (VarCharVector) vector;
                var varcharVal = (String) obj;

                if (isSet == 0)
                    varcharVec.setNull(row);
                else
                    varcharVec.set(row, varcharVal.getBytes(StandardCharsets.UTF_8));

                break;

            case Date:

                var dateVec = (DateDayVector) vector;
                var dateVal = (java.time.LocalDate) obj;
                var unixEpochDay = (int) dateVal.toEpochDay();

                dateVec.set(row, isSet, unixEpochDay);

                break;

            case Timestamp:

                var timestampVec = (TimeStampMilliVector) vector;
                var timestampVal = (java.time.OffsetDateTime) obj;

                var epochSecond = timestampVal.toEpochSecond();
                var nanos = timestampVal.getNano();
                var millis = nanos / 1000;
                var epochMillis = (epochSecond * 1000 * 1000) + millis;

                timestampVec.set(row, isSet, epochMillis);

                break;

            default:

                throw new EUnexpected();  // TODO: Error

        }
    }

    public static Object getValue(VectorSchemaRoot root, int row, int col) {

        var vector = root.getVector(col);
        var arrowTypeId = vector.getField().getType().getTypeID();

        var isSet = vector.isNull(row);

        if (!isSet)
            return null;

        switch (arrowTypeId) {

            case Bool:

                var boolVec = (BitVector) vector;
                return (boolVec.get(row) != 0);

            case Int:

                var intVec = (BigIntVector) vector;
                return intVec.get(row);

            case FloatingPoint:

                var floatVec = (Float8Vector) vector;
                return floatVec.get(row);

            case Decimal:

                var decimalVec = (Decimal256Vector) vector;
                return decimalVec.getObject(row);

            case Utf8:

                var varcharVec = (VarCharVector) vector;
                return varcharVec.getObject(row).toString();

            case Date:

                var dateVec = (DateDayVector) vector;
                var epochDays = dateVec.getObject(row);
                return LocalDate.ofEpochDay(epochDays);

            case Timestamp:

                var timestampVec = (TimeStampMilliVector) vector;
                return timestampVec.getObject(row).atOffset(ZoneOffset.UTC);

            default:

                throw new EUnexpected();  // TODO: Error
        }
    }
}
