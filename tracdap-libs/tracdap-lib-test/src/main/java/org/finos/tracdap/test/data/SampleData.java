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

package org.finos.tracdap.test.data;

import org.apache.arrow.vector.types.pojo.Schema;
import org.finos.tracdap.common.data.ArrowSchema;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.metadata.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class SampleData {

    public static final String BASIC_CSV_DATA_RESOURCE = "/sample_data/csv_basic.csv";
    public static final String BASIC_CSV_DATA_RESOURCE_V2 = "/sample_data/csv_basic_v2.csv";
    public static final String BASIC_JSON_DATA_RESOURCE = "/sample_data/json_basic.json";
    public static final String ALT_CSV_DATA_RESOURCE = "/sample_data/csv_alt.csv";

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

    public static final SchemaDefinition ALT_TABLE_SCHEMA
            = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.TABLE)
            .setTable(TableSchema.newBuilder()
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("alt_string_field")
                    .setFieldOrder(0)
                    .setFieldType(BasicType.STRING))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("alt_categorical_field")
                    .setFieldOrder(1)
                    .setFieldType(BasicType.STRING)
                    .setCategorical(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("alt_value_field")
                    .setFieldOrder(2)
                    .setFieldType(BasicType.FLOAT))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("alt_value_2_field")
                    .setFieldOrder(3)
                    .setFieldType(BasicType.FLOAT))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("alt_flag")
                    .setFieldOrder(4)
                    .setFieldType(BasicType.BOOLEAN)))
            .build();


    public static final FlowDefinition SAMPLE_FLOW = FlowDefinition.newBuilder()
            .putNodes("basic_data_input", FlowNode.newBuilder()
                    .setNodeType(FlowNodeType.INPUT_NODE)
                    .build())
            .putNodes("alt_data_input", FlowNode.newBuilder()
                    .setNodeType(FlowNodeType.INPUT_NODE)
                    .build())
            .putNodes("model_1", FlowNode.newBuilder()
                    .setNodeType(FlowNodeType.MODEL_NODE)
                    .addInputs("basic_data_input")
                    .addOutputs("enriched_basic_data")
                    .build())
            .putNodes("model_2", FlowNode.newBuilder()
                    .setNodeType(FlowNodeType.MODEL_NODE)
                    .addInputs("alt_data_input")
                    .addOutputs("enriched_alt_data")
                    .build())
            .putNodes("model_3", FlowNode.newBuilder()
                    .setNodeType(FlowNodeType.MODEL_NODE)
                    .addInputs("enriched_basic_data")
                    .addInputs("enriched_alt_data")
                    .addOutputs("sample_output_data")
                    .build())
            .putNodes("sample_output_data", FlowNode.newBuilder()
                    .setNodeType(FlowNodeType.OUTPUT_NODE)
                    .build())
            .addEdges(FlowEdge.newBuilder()
                    .setSource(FlowSocket.newBuilder()
                    .setNode("basic_data_input"))
                    .setTarget(FlowSocket.newBuilder()
                    .setNode("model_1")
                    .setSocket("basic_data_input")))
            .addEdges(FlowEdge.newBuilder()
                    .setSource(FlowSocket.newBuilder()
                    .setNode("alt_data_input"))
                    .setTarget(FlowSocket.newBuilder()
                    .setNode("model_2")
                    .setSocket("alt_data_input")))
            .addEdges(FlowEdge.newBuilder()
                    .setSource(FlowSocket.newBuilder()
                    .setNode("model_1")
                    .setSocket("enriched_basic_data"))
                    .setTarget(FlowSocket.newBuilder()
                    .setNode("model_3")
                    .setSocket("enriched_basic_data")))
            .addEdges(FlowEdge.newBuilder()
                    .setSource(FlowSocket.newBuilder()
                    .setNode("model_2")
                    .setSocket("enriched_alt_data"))
                    .setTarget(FlowSocket.newBuilder()
                    .setNode("model_3")
                    .setSocket("enriched_alt_data")))
            .addEdges(FlowEdge.newBuilder()
                    .setSource(FlowSocket.newBuilder()
                    .setNode("model_3")
                    .setSocket("sample_output_data"))
                    .setTarget(FlowSocket.newBuilder()
                    .setNode("sample_output_data")))
            .build();

    public static VectorSchemaRoot generateBasicData(BufferAllocator arrowAllocator) {

        var javaData = new HashMap<String, List<Object>>();

        for (var field : BASIC_TABLE_SCHEMA.getTable().getFieldsList()) {

            var javaValues = generateJavaValues(field.getFieldType(), 10);
            javaData.put(field.getFieldName(), javaValues);
        }

        return convertData(BASIC_TABLE_SCHEMA, javaData, 10, arrowAllocator);
    }

    public static List<Object> generateJavaValues(BasicType basicType, int n) {

        // NOTE: These values should match the pre-saved basic test files in resources/sample_data of -lib-test

        switch (basicType) {

            case BOOLEAN:
                return IntStream.range(0, n)
                        .mapToObj(i -> i % 2 == 0)
                        .collect(Collectors.toList());

            case INTEGER:
                return IntStream.range(0, n)
                        .mapToObj(i -> (long) i)
                        .collect(Collectors.toList());

            case FLOAT:
                return IntStream.range(0, n)
                        .mapToObj(i -> (double) i)
                        .collect(Collectors.toList());

            case DECIMAL:
                return IntStream.range(0, n)
                        .mapToObj(BigDecimal::valueOf)
                        .map(d -> d.setScale(12, RoundingMode.UNNECESSARY))
                        .collect(Collectors.toList());

            case STRING:
                return IntStream.range(0, n)
                        .mapToObj(i -> "Hello world " + i)
                        .collect(Collectors.toList());

            case DATE:
                return IntStream.range(0, n)
                        .mapToObj(i -> LocalDate.ofEpochDay(0).plusDays(i))
                        .collect(Collectors.toList());

            case DATETIME:
                return IntStream.range(0, n)
                        .mapToObj(i -> LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusSeconds(i))
                        .collect(Collectors.toList());

            default:
                throw new EUnexpected();
        }
    }

    public static VectorSchemaRoot convertData(
            SchemaDefinition schema, Map<String, List<Object>> data,
            int size, BufferAllocator arrowAllocator) {

        return convertData(ArrowSchema.tracToArrow(schema), data, size, arrowAllocator);
    }

    public static VectorSchemaRoot convertData(
            Schema schema, Map<String, List<Object>> data,
            int size, BufferAllocator arrowAllocator) {

        for (var fieldName : data.keySet()) {
            if (schema.findField(fieldName) == null) {
                throw new ETracInternal("Sample data field " + fieldName + " is not present in the schema");
            }
        }

        var vectors = new ArrayList<FieldVector>(schema.getFields().size());

        FieldVector vector;
        BiConsumer<Integer, Object> setFunc;

        for (var field : schema.getFields()) {

            switch (field.getType().getTypeID()) {

                case Bool:
                    var booleanVec = new BitVector(field, arrowAllocator);
                    vector = booleanVec;
                    setFunc = (i, o) -> {
                        if (o == null)
                            booleanVec.setNull(i);
                        else if (o instanceof Boolean)
                            booleanVec.set(i, (Boolean) o ? 1 : 0);
                        else
                            throw new EUnexpected();
                    };
                    break;

                case Int:
                    var intVec = new BigIntVector(field, arrowAllocator);
                    vector = intVec;
                    setFunc = (i, o) -> {
                        if (o == null)
                            intVec.setNull(i);
                        else if (o instanceof Integer)
                            intVec.set(i, (Integer) o);
                        else if (o instanceof Long)
                            intVec.set(i, (Long) o);
                        else
                            throw new EUnexpected();
                    };
                    break;

                case FloatingPoint:
                    var floatVec = new Float8Vector(field, arrowAllocator);
                    vector = floatVec;
                    setFunc = (i, o) -> {
                        if (o == null)
                            floatVec.setNull(i);
                        else if (o instanceof Float)
                            floatVec.set(i, (Float) o);
                        else if (o instanceof Double)
                            floatVec.set(i, (Double) o);
                        else
                            throw new EUnexpected();
                    };
                    break;

                case Decimal:
                    var decimalVec = new DecimalVector(field, arrowAllocator);
                    vector = decimalVec;
                    setFunc = (i, o) -> {
                        if (o == null)
                            decimalVec.setNull(i);
                        else if (o instanceof BigDecimal)
                            decimalVec.set(i, (BigDecimal) o);
                        else
                            throw new EUnexpected();
                    };
                    break;

                case Utf8:
                    var stringVec = new VarCharVector(field, arrowAllocator);
                    vector = stringVec;
                    setFunc = (i, o) -> {
                        if (o == null)
                            stringVec.setNull(i);
                        else if (o instanceof String)
                            stringVec.set(i, ((String) o).getBytes(StandardCharsets.UTF_8));
                        else
                            throw new EUnexpected();
                    };
                    break;

                case Date:
                    var dateVec = new DateDayVector(field, arrowAllocator);
                    vector = dateVec;
                    setFunc = (i, o) -> {
                        if (o == null)
                            dateVec.setNull(i);
                        else if (o instanceof LocalDate)
                            dateVec.set(i, (int) ((LocalDate) o).toEpochDay());
                        else
                            throw new EUnexpected();
                    };
                    break;

                case Timestamp:
                    var timestampVec = new TimeStampMilliVector(field, arrowAllocator);
                    vector = timestampVec;
                    setFunc = (i, o) -> {
                        if (o == null)
                            timestampVec.setNull(i);
                        else if (o instanceof LocalDateTime) {
                            LocalDateTime datetimeNoZone = (LocalDateTime) o;
                            long unixEpochMillis =
                                    (datetimeNoZone.toEpochSecond(ZoneOffset.UTC) * 1000) +
                                            (datetimeNoZone.getNano() / 1000000);

                            timestampVec.set(i, unixEpochMillis);
                        }
                        else
                            throw new EUnexpected();
                    };
                    break;

                default:
                    throw new EUnexpected();
            }

            vector.allocateNew();
            vector.setInitialCapacity(size);

            var values = data.get(field.getName());
            if (values == null)
                values = Collections.nCopies(size, null);

            for (int i = 0; i < size; i++) {
                setFunc.accept(i, values.get(i));
            }

            vectors.add(vector);
        }

        var root = new VectorSchemaRoot(schema.getFields(), vectors);
        root.setRowCount(size);

        return root;
    }
}
