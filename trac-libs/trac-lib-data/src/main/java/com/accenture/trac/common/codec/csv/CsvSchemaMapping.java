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

package com.accenture.trac.common.codec.csv;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.metadata.BasicType;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.SchemaType;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

public class CsvSchemaMapping {

    private static final Map<BasicType, CsvSchema.ColumnType> TRAC_CSV_TYPE_MAPPING = Map.ofEntries(
            Map.entry(BasicType.BOOLEAN, CsvSchema.ColumnType.BOOLEAN),
            Map.entry(BasicType.INTEGER, CsvSchema.ColumnType.NUMBER),
            Map.entry(BasicType.FLOAT, CsvSchema.ColumnType.NUMBER),
            Map.entry(BasicType.DECIMAL, CsvSchema.ColumnType.NUMBER_OR_STRING),
            Map.entry(BasicType.STRING, CsvSchema.ColumnType.STRING),
            Map.entry(BasicType.DATE, CsvSchema.ColumnType.STRING),
            Map.entry(BasicType.DATETIME, CsvSchema.ColumnType.STRING));

    private static final Map<ArrowTypeID, CsvSchema.ColumnType> ARROW_CSV_TYPE_MAPPING = Map.ofEntries(
            Map.entry(ArrowTypeID.Bool, CsvSchema.ColumnType.BOOLEAN),
            Map.entry(ArrowTypeID.Int, CsvSchema.ColumnType.NUMBER),
            Map.entry(ArrowTypeID.FloatingPoint, CsvSchema.ColumnType.NUMBER),
            Map.entry(ArrowTypeID.Decimal, CsvSchema.ColumnType.NUMBER_OR_STRING),
            Map.entry(ArrowTypeID.Utf8, CsvSchema.ColumnType.STRING),
            Map.entry(ArrowTypeID.Date, CsvSchema.ColumnType.STRING),
            Map.entry(ArrowTypeID.Timestamp, CsvSchema.ColumnType.STRING));

    public static CsvSchema.Builder tracToCsv(SchemaDefinition tracSchema) {

        // Unexpected error - TABLE is the only TRAC schema type currently available
        if (tracSchema.getSchemaType() != SchemaType.TABLE)
            throw new EUnexpected();

        var tracTableSchema = tracSchema.getTable();
        var csvSchema = CsvSchema.builder();

        for (var tracField : tracTableSchema.getFieldsList()) {

            var fieldName = tracField.getFieldName();
            var csvType = TRAC_CSV_TYPE_MAPPING.get(tracField.getFieldType());

            // Unexpected error - All TRAC primitive types are mapped
            if (csvType == null)
                throw new EUnexpected();

            csvSchema.addColumn(fieldName, csvType);
        }

        return csvSchema;
    }

    public static CsvSchema.Builder arrowToCsv(Schema arrowSchema) {

        var csvSchema = CsvSchema.builder();

        for (var arrowField : arrowSchema.getFields()) {

            // TODO: Dereference dictionary types

            var fieldName = arrowField.getName();
            var arrowType = arrowField.getType().getTypeID();
            var csvType = ARROW_CSV_TYPE_MAPPING.get(arrowType);

            if (csvType == null)
                throw new EUnexpected();  // TODO: Error

            csvSchema.addColumn(fieldName, csvType);
        }

        return csvSchema;
    }
}
