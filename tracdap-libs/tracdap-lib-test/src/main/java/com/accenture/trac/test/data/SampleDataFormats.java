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

package com.accenture.trac.test.data;

import com.accenture.trac.metadata.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;


public class SampleDataFormats {

    public static final String BASIC_CSV_DATA_RESOURCE = "/sample_data_formats/csv_basic.csv";
    public static final String BASIC_CSV_DATA_RESOURCE_V2 = "/sample_data_formats/csv_basic_v2.csv";
    public static final String BASIC_JSON_DATA_RESOURCE = "/sample_data_formats/json_basic.json";

    public static final SchemaDefinition BASIC_TABLE_SCHEMA
            = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.TABLE)
            .setTable(TableSchema.newBuilder()
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("boolean_field")
                    .setFieldOrder(0)
                    .setFieldType(BasicType.BOOLEAN))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("integer_field")
                    .setFieldOrder(1)
                    .setFieldType(BasicType.INTEGER))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("float_field")
                    .setFieldOrder(2)
                    .setFieldType(BasicType.FLOAT))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("decimal_field")
                    .setFieldOrder(3)
                    .setFieldType(BasicType.DECIMAL))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("string_field")
                    .setFieldOrder(4)
                    .setFieldType(BasicType.STRING))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("date_field")
                    .setFieldOrder(5)
                    .setFieldType(BasicType.DATE))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("datetime_field")
                    .setFieldOrder(6)
                    .setFieldType(BasicType.DATETIME)))
            .build();

    public static final SchemaDefinition BASIC_TABLE_SCHEMA_V2
            = BASIC_TABLE_SCHEMA.toBuilder()
            .setTable(BASIC_TABLE_SCHEMA.getTable().toBuilder()
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("extra_string_field")
                    .setFieldOrder(7)
                    .setFieldType(BasicType.STRING)))
            .build();

    public static VectorSchemaRoot generateBasicData(BufferAllocator arrowAllocator) {

        var decimalPrecision = 38;
        var decimalScale = 12;
        var decimalWidth = 16;

        var decimalArrowType = new ArrowType.Decimal(decimalPrecision, decimalScale, decimalWidth * 8);
        var decimalFieldType = FieldType.nullable(decimalArrowType);
        var decimalField = new Field("decimal_field", decimalFieldType, null);

        var booleanVec = new BitVector("boolean_field", arrowAllocator);
        booleanVec.allocateNew(10);
        var integerVec = new BigIntVector("integer_field", arrowAllocator);
        integerVec.allocateNew(10);
        var floatVec = new Float8Vector("float_field", arrowAllocator);
        floatVec.allocateNew(10);
        var decimalVec = new DecimalVector(decimalField, arrowAllocator);
        decimalVec.allocateNew(10);
        var stringVec = new VarCharVector("string_field", arrowAllocator);
        stringVec.allocateNew(10);
        var dateVec = new DateDayVector("date_field", arrowAllocator);
        dateVec.allocateNew(10);
        var datetimeVec = new TimeStampMilliVector("datetime_field", arrowAllocator);
        datetimeVec.allocateNew(10);

        for (int row = 0; row < 10; row++) {

            var decimalVal = new BigDecimal(row);
            var scaledDecimalVal = decimalVal.setScale(decimalScale, RoundingMode.UNNECESSARY);

            var varcharVal = String.format("Hello world %d", row).getBytes(StandardCharsets.UTF_8);

            booleanVec.set(row, row % 2 == 0 ? 1 : 0);
            integerVec.set(row, row);
            floatVec.set(row, 1.0 * row);
            decimalVec.set(row, scaledDecimalVal);
            stringVec.set(row, varcharVal);
            dateVec.set(row, row);  // using epoch day = row index
            datetimeVec.set(row, row * 1000);  // milliseconds after epoch
        }

        var vectors = List.<FieldVector>of(booleanVec, integerVec, floatVec, decimalVec, stringVec, dateVec, datetimeVec);
        var fields = vectors.stream().map(FieldVector::getField).collect(Collectors.toList());

        var root = new VectorSchemaRoot(fields, vectors);
        root.setRowCount(10);

        return root;
    }
}
