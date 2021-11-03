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
import com.accenture.trac.metadata.BasicType;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.SchemaType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;

import java.util.ArrayList;
import java.util.Map;


public class ArrowSchema {

    private static final int DECIMAL_PRECISION = 38;
    private static final int DECIMAL_SCALE = 12;
    private static final int DECIMAL_BIT_WIDTH = 256;

    private static final TimeUnit TIMESTAMP_PRECISION = TimeUnit.MILLISECOND;
    private static final String TZ_ZULU = "+00:00";

    private static final Map<BasicType, ArrowType> TRAC_ARROW_TYPE_MAPPING = Map.ofEntries(
            Map.entry(BasicType.BOOLEAN, ArrowType.Bool.INSTANCE),
            Map.entry(BasicType.INTEGER, new ArrowType.Int(8, true)),
            Map.entry(BasicType.FLOAT, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Map.entry(BasicType.DECIMAL, new ArrowType.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE, DECIMAL_BIT_WIDTH)),
            Map.entry(BasicType.STRING, ArrowType.Utf8.INSTANCE),
            Map.entry(BasicType.DATE, new ArrowType.Date(DateUnit.DAY)),
            Map.entry(BasicType.DATETIME, new ArrowType.Timestamp(TIMESTAMP_PRECISION, TZ_ZULU)));


    public static Schema tracToArrow(SchemaDefinition tracSchema) {

        if (tracSchema.getSchemaType() != SchemaType.TABLE)
            throw new EUnexpected();  // TODO: Error

        var tracTableSchema = tracSchema.getTable();
        var arrowFields = new ArrayList<Field>(tracTableSchema.getFieldsCount());

        for (var tracField : tracTableSchema.getFieldsList()) {

            var fieldName = tracField.getFieldName();
            var nullable = !tracField.getBusinessKey();  // only business keys are not nullable

            var arrowType = TRAC_ARROW_TYPE_MAPPING.get(tracField.getFieldType());

            if (arrowType == null)
                throw new EUnexpected();  // TODO: Error

            var arrowFieldType = new FieldType(nullable, arrowType, /* dictionary = */ null);
            var arrowField = new Field(fieldName, arrowFieldType, /* children = */ null);

            arrowFields.add(arrowField);
        }

        return new Schema(arrowFields);
    }
}
