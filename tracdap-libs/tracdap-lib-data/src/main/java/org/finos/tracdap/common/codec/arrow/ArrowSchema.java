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

package org.finos.tracdap.common.codec.arrow;

import org.finos.tracdap.common.exception.EDataTypeNotSupported;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.metadata.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;

import java.util.ArrayList;
import java.util.Map;


public class ArrowSchema {

    // Default decimal settings - 38:12 with 128 bit width
    private static final int DECIMAL_PRECISION = 38;
    private static final int DECIMAL_SCALE = 12;
    private static final int DECIMAL_BIT_WIDTH = 128;

    // Default datetime settings
    private static final TimeUnit TIMESTAMP_PRECISION = TimeUnit.MILLISECOND;
    private static final String NO_ZONE = null;

    public static final ArrowType ARROW_BASIC_BOOLEAN = ArrowType.Bool.INSTANCE;
    public static final ArrowType ARROW_BASIC_INTEGER = new ArrowType.Int(64, true);
    public static final ArrowType ARROW_BASIC_FLOAT = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    public static final ArrowType ARROW_BASIC_DECIMAL =  new ArrowType.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE, DECIMAL_BIT_WIDTH);
    public static final ArrowType ARROW_BASIC_STRING = ArrowType.Utf8.INSTANCE;
    public static final ArrowType ARROW_BASIC_DATE = new ArrowType.Date(DateUnit.DAY);
    public static final ArrowType ARROW_BASIC_DATETIME = new ArrowType.Timestamp(TIMESTAMP_PRECISION, NO_ZONE);  // Using type without timezone

    private static final Map<BasicType, ArrowType> TRAC_ARROW_TYPE_MAPPING = Map.ofEntries(
            Map.entry(BasicType.BOOLEAN, ARROW_BASIC_BOOLEAN),
            Map.entry(BasicType.INTEGER, ARROW_BASIC_INTEGER),
            Map.entry(BasicType.FLOAT, ARROW_BASIC_FLOAT),
            Map.entry(BasicType.DECIMAL, ARROW_BASIC_DECIMAL),
            Map.entry(BasicType.STRING, ARROW_BASIC_STRING),
            Map.entry(BasicType.DATE, ARROW_BASIC_DATE),
            Map.entry(BasicType.DATETIME, ARROW_BASIC_DATETIME));

    private static final Map<ArrowType.ArrowTypeID, BasicType> ARROW_TRAC_TYPE_MAPPING = Map.ofEntries(
            Map.entry(ArrowType.ArrowTypeID.Bool, BasicType.BOOLEAN),
            Map.entry(ArrowType.ArrowTypeID.Int, BasicType.INTEGER),
            Map.entry(ArrowType.ArrowTypeID.FloatingPoint, BasicType.FLOAT),
            Map.entry(ArrowType.ArrowTypeID.Decimal, BasicType.DECIMAL),
            Map.entry(ArrowType.ArrowTypeID.Utf8, BasicType.STRING),
            Map.entry(ArrowType.ArrowTypeID.Date, BasicType.DATE),
            Map.entry(ArrowType.ArrowTypeID.Timestamp, BasicType.DATETIME));


    public static Schema tracToArrow(SchemaDefinition tracSchema) {

        // Unexpected error - TABLE is the only TRAC schema type currently available
        if (tracSchema.getSchemaType() != SchemaType.TABLE)
            throw new EUnexpected();

        var tracTableSchema = tracSchema.getTable();
        var arrowFields = new ArrayList<Field>(tracTableSchema.getFieldsCount());

        for (var tracField : tracTableSchema.getFieldsList()) {

            var fieldName = tracField.getFieldName();
            var nullable = !tracField.getBusinessKey();  // only business keys are not nullable

            var arrowType = TRAC_ARROW_TYPE_MAPPING.get(tracField.getFieldType());

            // Unexpected error - All TRAC primitive types are mapped
            if (arrowType == null)
                throw new EUnexpected();

            var arrowFieldType = new FieldType(nullable, arrowType, /* dictionary = */ null);
            var arrowField = new Field(fieldName, arrowFieldType, /* children = */ null);

            arrowFields.add(arrowField);
        }

        return new Schema(arrowFields);
    }

    public static SchemaDefinition arrowToTrac(Schema arrowSchema) {

        var tracTableSchema = TableSchema.newBuilder();

        for (var arrowField : arrowSchema.getFields()) {

            var fieldName = arrowField.getName();
            var fieldIndex = tracTableSchema.getFieldsCount();

            var arrowTypeId = arrowField.getType().getTypeID();
            var tracType = ARROW_TRAC_TYPE_MAPPING.get(arrowTypeId);

            if (tracType == null) {
                var arrowTypeName = arrowField.getType().getTypeID().name();
                var error = String.format("TRAC type mapping not available for arrow type [%s]", arrowTypeName);
                throw new EDataTypeNotSupported(error);
            }

            tracTableSchema.addFields(FieldSchema.newBuilder()
                    .setFieldName(fieldName)
                    .setFieldOrder(fieldIndex)
                    .setFieldType(tracType));

            // Not attempting to set business key, categorical flag, format code or label
            // Categorical *could* be inferred for Arrow dictionary vectors
            // Other flags could be set in Arrow metadata
            // But since arrow -> trac normally implies an external source,
            // Thought would be needed on how to interpret any metadata that is present
        }

        return SchemaDefinition.newBuilder()
                .setSchemaType(SchemaType.TABLE)
                .setTable(tracTableSchema)
                .build();
    }

    public static VectorSchemaRoot createRoot(Schema arrowSchema, BufferAllocator arrowAllocator) {

        return createRoot(arrowSchema, arrowAllocator, 0);
    }

    public static VectorSchemaRoot createRoot(Schema arrowSchema, BufferAllocator arrowAllocator, int initialCapacity) {

        var fields = arrowSchema.getFields();
        var vectors = new ArrayList<FieldVector>(fields.size());

        for (var field : fields) {

            var vector = field.createVector(arrowAllocator);

            if (initialCapacity > 0)
                vector.setInitialCapacity(initialCapacity);

            vectors.add(vector);
        }

        return new VectorSchemaRoot(fields, vectors);
    }
}
