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

package org.finos.tracdap.test.data;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.ArrowVsrSchema;
import org.finos.tracdap.common.data.ArrowVsrStaging;
import org.finos.tracdap.common.data.SchemaMapping;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.TypeSystem;
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


public class SampleData {

    public static final String BASIC_CSV_DATA_RESOURCE = "/sample_data/csv_basic.csv";
    public static final String BASIC_CSV_DATA_RESOURCE_V2 = "/sample_data/csv_basic_v2.csv";
    public static final String BASIC_JSON_DATA_RESOURCE = "/sample_data/json_basic.json";
    public static final String STRUCT_JSON_DATA_RESOURCE = "/sample_data/json_struct.json";
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
                    .setFieldName("categorical_field")
                    .setFieldOrder(5)
                    .setFieldType(BasicType.STRING)
                    .setCategorical(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("date_field")
                    .setFieldOrder(6)
                    .setFieldType(BasicType.DATE))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("datetime_field")
                    .setFieldOrder(7)
                    .setFieldType(BasicType.DATETIME)))
            .build();

    public static final SchemaDefinition BASIC_TABLE_SCHEMA_V2
            = BASIC_TABLE_SCHEMA.toBuilder()
            .setTable(BASIC_TABLE_SCHEMA.getTable().toBuilder()
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("extra_string_field")
                    .setFieldOrder(8)
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

    public static final SchemaDefinition ALT_TABLE_SCHEMA_V2
            = ALT_TABLE_SCHEMA.toBuilder()
            .setTable(ALT_TABLE_SCHEMA.getTable().toBuilder()
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("alt_extra_flag")
                    .setFieldOrder(5)
                    .setFieldType(BasicType.BOOLEAN)))
            .build();

    public static final SchemaDefinition BASIC_STRUCT_SCHEMA
            = SchemaDefinition.newBuilder()
            .setSchemaType(SchemaType.STRUCT_SCHEMA)
            .setPartType(PartType.NOT_PARTITIONED)
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("boolField")
                    .setFieldType(BasicType.BOOLEAN)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("intField")
                    .setFieldType(BasicType.INTEGER)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("floatField")
                    .setFieldType(BasicType.FLOAT)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("decimalField")
                    .setFieldType(BasicType.DECIMAL)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("strField")
                    .setFieldType(BasicType.STRING)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("dateField")
                    .setFieldType(BasicType.DATE)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("datetimeField")
                    .setFieldType(BasicType.DATETIME)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("enumField")
                    .setFieldType(BasicType.STRING)
                    .setCategorical(true)
                    .setNotNull(true)
                    .setNamedEnum("ExampleEnum"))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("quotedField")
                    .setFieldType(BasicType.STRING)
                    .setNotNull(true))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("optionalField")
                    .setFieldType(BasicType.STRING)
                    .setNotNull(false))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("optionalQuotedField")
                    .setFieldType(BasicType.STRING)
                    .setNotNull(false))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("listField")
                    .setFieldType(BasicType.ARRAY)
                    .setNotNull(true)
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldType(BasicType.INTEGER)
                            .setNotNull(true)))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("dictField")
                    .setFieldType(BasicType.MAP)
                    .setNotNull(true)
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldType(BasicType.STRING)
                            .setNotNull(true))
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldType(BasicType.DATETIME)
                            .setNotNull(true)))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("anonymousStructField")
                    .setFieldType(BasicType.STRUCT)
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldName("field1")
                            .setFieldType(BasicType.STRING)
                            .setNotNull(true))
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldName("field2")
                            .setFieldType(BasicType.INTEGER)
                            .setNotNull(true))
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldName("enumField")
                            .setFieldType(BasicType.STRING)
                            .setCategorical(true)
                            .setNotNull(true)
                            .setNamedEnum("ExampleEnum")))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("structField")
                    .setFieldType(BasicType.STRUCT)
                    .setNamedType("DataClassSubStruct"))
            .addFields(FieldSchema.newBuilder()
                    .setFieldName("nestedStructField")
                    .setFieldType(BasicType.MAP)
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldType(BasicType.STRING)
                            .setNotNull(true))
                    .addChildren(FieldSchema.newBuilder()
                            .setFieldType(BasicType.STRUCT)
                            .setNamedType("DataClassSubStruct")
                            .setNotNull(true)))
            .putNamedTypes("DataClassSubStruct", SchemaDefinition.newBuilder()
                    .setSchemaType(SchemaType.STRUCT_SCHEMA)
                    .setPartType(PartType.NOT_PARTITIONED)
                    .addFields(FieldSchema.newBuilder()
                            .setFieldName("field1")
                            .setFieldType(BasicType.STRING)
                            .setNotNull(true))
                    .addFields(FieldSchema.newBuilder()
                            .setFieldName("field2")
                            .setFieldType(BasicType.INTEGER)
                            .setNotNull(true))
                    .addFields(FieldSchema.newBuilder()
                            .setFieldName("enumField")
                            .setFieldType(BasicType.STRING)
                            .setCategorical(true)
                            .setNotNull(true)
                            .setNamedEnum("ExampleEnum"))
                    .build())
            .putNamedEnums("ExampleEnum", EnumValues.newBuilder()
                    .addValues("RED")
                    .addValues("BLUE")
                    .addValues("GREEN")
                    .build())
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

    public static ArrowVsrContext generateBasicData(BufferAllocator arrowAllocator) {

        return generateTestData(BASIC_TABLE_SCHEMA, arrowAllocator, 0);
    }

    public static ArrowVsrContext generateStructData(BufferAllocator arrowAllocator) {

        return generateTestData(BASIC_STRUCT_SCHEMA, arrowAllocator, 2);
    }

    public static ArrowVsrContext generateTestData(SchemaDefinition schema, BufferAllocator arrowAllocator, int offset) {

        var arrowSchema = SchemaMapping.tracToArrow(schema, arrowAllocator);

        var nRows = schema.getSchemaType() == SchemaType.STRUCT_SCHEMA ? 1 : 10;
        var fields = schema.getSchemaType() == SchemaType.TABLE_SCHEMA
                ? schema.getTable().getFieldsList()
                : schema.getFieldsList();

        var javaData = new ArrayList<Map<String, Object>>();

        for (int row = 0; row < nRows; row++) {

            var record = new HashMap<String, Object>();

            for (var field :fields) {

                var javaValue = generateJavaValue(field, row + offset, schema);
                record.put(field.getFieldName(), javaValue);
            }

            javaData.add(record);
        }

        return convertData(arrowSchema, javaData, arrowAllocator);
    }

    public static Object generateJavaValue(FieldSchema field, int n, SchemaDefinition schema) {

        if (TypeSystem.isPrimitive(field.getFieldType())) {
            if (field.hasNamedEnum()) {
                var categories = schema.getNamedEnumsOrThrow(field.getNamedEnum()).getValuesList();
                return generateJavaPrimitive(field.getFieldType(), field.getCategorical(), n, categories);
            }
            else {
                return generateJavaPrimitive(field.getFieldType(), field.getCategorical(), n, null);
            }
        }

        if (field.getFieldType() == BasicType.ARRAY) {

            var itemsField = field.getChildren(0);

            var list = new ArrayList<>(5);

            for (int j = 0; j < 5; j++) {
                var item =  generateJavaValue(itemsField, j, schema);
                list.add(item);
            }

            return list;
        }

        if (field.getFieldType() == BasicType.MAP) {

            var keyField =  field.getChildren(0);
            var valuesField = field.getChildren(1);

            var map = new HashMap<String, Object>();

            for (int j = 0; j < 3; j++) {
                var key = "key_" + j;
                var value = generateJavaValue(valuesField, j, schema);
                map.put(key, value);
            }

            return map;
        }

        if (field.getFieldType() == BasicType.STRUCT) {

            var structFields = field.hasNamedType()
                    ? schema.getNamedTypesOrThrow(field.getNamedType()).getFieldsList()
                    : field.getChildrenList();

            var struct = new HashMap<String, Object>(n);

            for (var structField : structFields) {

                var javaValue = generateJavaValue(structField, n, schema);
                struct.put(structField.getFieldName(), javaValue);
            }

            return struct;
        }

        throw new EUnexpected();
    }

    public static Object generateJavaPrimitive(BasicType basicType, boolean categorical, int index, List<String> enumValues) {

        // NOTE: These values should match the pre-saved basic test files in resources/sample_data of -lib-test

        switch (basicType) {

            case BOOLEAN:
                return index % 2 == 0;

            case INTEGER:
                return index;

            case FLOAT:
                return (double) index;

            case DECIMAL:
                return BigDecimal.valueOf(index).setScale(12, RoundingMode.UNNECESSARY);

            case STRING:
                if (categorical) {
                    var categories = enumValues != null ? enumValues : List.of("RED", "BLUE", "GREEN");
                    return categories.get(index % categories.size());
                }
                else {
                    return "Hello world " + index;
                }

            case DATE:
                return LocalDate.ofEpochDay(0).plusDays(index);

            case DATETIME:
                return LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusSeconds(index);

            default:
                throw new EUnexpected();
        }
    }

    public static ArrowVsrContext convertData(
            ArrowVsrSchema schema, List<Map<String, Object>> data,
            BufferAllocator arrowAllocator) {


        var vectors = schema.physical().getFields()
                .stream()
                .map(field -> field.createVector(arrowAllocator))
                .collect(Collectors.toList());

        vectors.forEach(ValueVector::allocateNew);
        vectors.forEach(v -> v.setInitialCapacity(data.size()));

        var vsr = new VectorSchemaRoot(vectors);
        var dictionaries = new DictionaryProvider.MapDictionaryProvider();

        if (schema.dictionaries() != null) {
            for (var id : schema.dictionaries().getDictionaryIds()) {
                var prebuilt = schema.dictionaries().lookup(id);
                if (prebuilt != null && prebuilt.getVector().getValueCount() > 0)
                    dictionaries.put(prebuilt);
            }
        }

        var staging = new ArrayList<ArrowVsrStaging<?>>();

        var consumers = vectors.stream()
                .map(vector -> buildConsumer(vector, schema.dictionaryFields(), dictionaries, staging, arrowAllocator))
                .collect(Collectors.toList());

        for (int row = 0; row < data.size(); row++) {

            var record = data.get(row);
            var nFields = schema.physical().getFields().size();

            for (int col = 0; col < nFields; col++) {

                var field = schema.physical().getFields().get(col);
                var value = record.get(field.getName());
                var consumer = consumers.get(col);

                consumer.accept(row, value);
            }
        }

        staging.forEach(ArrowVsrStaging::encodeVector);
        vectors.forEach(vector -> vector.setValueCount(data.size()));

        var context = ArrowVsrContext.forSource(vsr, dictionaries, arrowAllocator, /* ownership = */ true);
        context.setRowCount(data.size());
        context.setLoaded();

        // Do not flip, client code may modify back buffer before flipping

        return context;
    }

    private static BiConsumer<Integer, Object> buildConsumer(
            FieldVector vector,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider.MapDictionaryProvider dictionaries,
            List<ArrowVsrStaging<?>> stagedFields,
            BufferAllocator allocator) {

        Field field = vector.getField();

        if (field.getDictionary() != null) {

            var encoding = field.getDictionary();
            var dictionaryField =  dictionaryFields.get(encoding.getId());

            var stagingVector = (ElementAddressableVector) dictionaryField.createVector(allocator);
            stagingVector.allocateNew();

            var dictionary = dictionaries.lookup(encoding.getId());

            if (dictionary != null) {

                var staging = new ArrowVsrStaging(stagingVector, (BaseIntVector) vector, dictionary);
                stagedFields.add(staging);
            }
            else {

                var staging = new ArrowVsrStaging(stagingVector, (BaseIntVector) vector);
                stagedFields.add(staging);
                dictionaries.put(staging.getDictionary());
            }

            var stagingConsumer = buildConsumer((FieldVector) stagingVector, null, null, null, allocator);

            return stagingConsumer;
        }

        switch (field.getType().getTypeID()) {

            case Bool:
                var booleanVec = (BitVector) vector;
                return (i, o) -> booleanVec.set(i, (Boolean) o ? 1 : 0);

            case Int:
                var intVec = (BigIntVector) vector;
                return  (i, o) -> {
                    if (o instanceof Integer)
                        intVec.set(i, (Integer) o);
                    else if (o instanceof Long)
                        intVec.set(i, (Long) o);
                    else
                        throw new EUnexpected();
                };

            case FloatingPoint:
                var floatVec = (Float8Vector) vector;
                return (i, o) -> {
                    if (o instanceof Float)
                        floatVec.set(i, (Float) o);
                    else if (o instanceof Double)
                        floatVec.set(i, (Double) o);
                    else
                        throw new EUnexpected();
                };

            case Decimal:
                var decimalVec = (DecimalVector) vector;
                return (i, o) -> decimalVec.set(i, (BigDecimal) o);

            case Utf8:
                var stringVec = (VarCharVector) vector;
                return (i, o) -> {
                    stringVec.setSafe(i, ((String) o).getBytes(StandardCharsets.UTF_8));
                    stringVec.setValueCount(i + 1);
                };

            case Date:
                var dateVec = (DateDayVector) vector;
                return (i, o) -> dateVec.set(i, (int) ((LocalDate) o).toEpochDay());

            case Timestamp:
                var timestampVec = (TimeStampMilliVector) vector;
                return (i, o) -> {
                    LocalDateTime datetimeNoZone = (LocalDateTime) o;
                    long unixEpochMillis =
                            (datetimeNoZone.toEpochSecond(ZoneOffset.UTC) * 1000) +
                            (datetimeNoZone.getNano() / 1000000);
                    timestampVec.set(i, unixEpochMillis);
                };

            case List:

                var listVector = (ListVector) vector;
                var dataVector = listVector.getDataVector();
                var itemConsumer = buildConsumer(dataVector, dictionaryFields, dictionaries, stagedFields, allocator);

                return (i, o) -> {
                    @SuppressWarnings("unchecked")
                    List<Object> list = (List<Object>) o;
                    int currentSize = dataVector.getValueCount();
                    int currenCapacity = dataVector.getValueCapacity();
                    if (currentSize == 0) {
                        dataVector.setInitialCapacity(list.size());
                        currenCapacity = dataVector.getValueCapacity();
                    }
                    while (currentSize + list.size() > currenCapacity) {
                        dataVector.reAlloc();
                        currenCapacity =  dataVector.getValueCapacity();
                    }
                    listVector.startNewValue(i);
                    for (int j = 0; j < list.size(); j++) {
                        itemConsumer.accept(currentSize + j, list.get(j));
                    }
                    listVector.endValue(i, list.size());
                };

            case Map:

                var mapVector = (MapVector) vector;
                var entryVector = (StructVector) mapVector.getDataVector();
                var keyVector = (VarCharVector) entryVector.getChildrenFromFields().get(0);
                var valueVector = entryVector.getChildrenFromFields().get(1);
                var keyConsumer = buildConsumer(keyVector, dictionaryFields, dictionaries, stagedFields, allocator);
                var valueConsumer = buildConsumer(valueVector, dictionaryFields, dictionaries, stagedFields, allocator);

                return (i, o) -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) o;
                    List<Object> mapKeys = new ArrayList<>(map.keySet());
                    List<Object> mapValues = mapKeys.stream().map(Object::toString).map(map::get).collect(Collectors.toList());
                    int currentSize = entryVector.getValueCount();
                    int currenCapacity = entryVector.getValueCapacity();
                    if (currentSize == 0) {
                        entryVector.setInitialCapacity(map.size());
                        currenCapacity = entryVector.getValueCapacity();
                    }
                    while (currentSize + map.size() > currenCapacity) {
                        entryVector.reAlloc();
                        currenCapacity = entryVector.getValueCapacity();
                    }
                    mapVector.startNewValue(i);
                    for (int j = 0; j < mapKeys.size(); j++) {
                        var mapKey = mapKeys.get(j);
                        var mapValue = mapValues.get(j);
                        keyConsumer.accept(currentSize + j, mapKey);
                        valueConsumer.accept(currentSize + j, mapValue);
                        entryVector.setIndexDefined(currentSize + j);
                    }
                    entryVector.setValueCount(currentSize + mapKeys.size());
                    entryVector.getChildrenFromFields().get(1).setValueCount(currentSize + mapKeys.size());
                    mapVector.endValue(i, mapKeys.size());
                };

            case Struct:

                var structVector = (StructVector) vector;
                var childVectors = structVector.getChildrenFromFields();
                var childConsumers = new HashMap<String, BiConsumer<Integer, Object>>();
                for (var child : childVectors) {
                    var childConsumer = buildConsumer(child, dictionaryFields, dictionaries, stagedFields, allocator);
                    childConsumers.put(child.getName(), childConsumer);
                }

                return (i, o) -> {

                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) o;

                    for (FieldVector childVector : childVectors) {
                        var childValue = map.get(childVector.getName());
                        childConsumers.get(childVector.getName()).accept(i, childValue);
                    }

                    structVector.setIndexDefined(i);
                };

            default:
                throw new EUnexpected();
        }
    }
}
