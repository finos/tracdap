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

package org.finos.tracdap.common.data;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.exception.EValidationGap;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.metadata.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SchemaMapping {

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

    public static final ArrowType ARROW_LIST = new ArrowType.List();
    public static final ArrowType ARROW_MAP = new ArrowType.Map(/* keysSorted = */ false);
    public static final ArrowType ARROW_STRUCT = new ArrowType.Struct();

    private static final Map<BasicType, ArrowType> TRAC_ARROW_TYPE_MAPPING = Map.ofEntries(
            Map.entry(BasicType.BOOLEAN, ARROW_BASIC_BOOLEAN),
            Map.entry(BasicType.INTEGER, ARROW_BASIC_INTEGER),
            Map.entry(BasicType.FLOAT, ARROW_BASIC_FLOAT),
            Map.entry(BasicType.DECIMAL, ARROW_BASIC_DECIMAL),
            Map.entry(BasicType.STRING, ARROW_BASIC_STRING),
            Map.entry(BasicType.DATE, ARROW_BASIC_DATE),
            Map.entry(BasicType.DATETIME, ARROW_BASIC_DATETIME),
            Map.entry(BasicType.ARRAY, ARROW_LIST),
            Map.entry(BasicType.MAP, ARROW_MAP),
            Map.entry(BasicType.STRUCT, ARROW_STRUCT));

    private static final Map<ArrowType.ArrowTypeID, BasicType> ARROW_TRAC_TYPE_MAPPING = Map.ofEntries(
            Map.entry(ArrowType.ArrowTypeID.Bool, BasicType.BOOLEAN),
            Map.entry(ArrowType.ArrowTypeID.Int, BasicType.INTEGER),
            Map.entry(ArrowType.ArrowTypeID.FloatingPoint, BasicType.FLOAT),
            Map.entry(ArrowType.ArrowTypeID.Decimal, BasicType.DECIMAL),
            Map.entry(ArrowType.ArrowTypeID.Utf8, BasicType.STRING),
            Map.entry(ArrowType.ArrowTypeID.Date, BasicType.DATE),
            Map.entry(ArrowType.ArrowTypeID.Timestamp, BasicType.DATETIME));

    public static final FieldSchema DEFAULT_MAP_KEY_FIELD = FieldSchema.newBuilder()
            .setFieldName("key")  // Match Arrow's MapVector.KEY_NAME
            .setFieldType(BasicType.STRING)
            .setNotNull(true)
            .build();

    private final SchemaDefinition topLevelSchema;
    private final Map<Long, Field> dictionaryFields;
    private final DictionaryProvider.MapDictionaryProvider dictionaries;
    private final Map<String, Long> namedEnums;

    public static ArrowVsrSchema tracToArrow(SchemaDefinition tracSchema) {

        var mapping = new SchemaMapping(tracSchema);
        return mapping.tracToArrowSchema(tracSchema, null);
    }

    public static ArrowVsrSchema tracToArrow(SchemaDefinition tracSchema, BufferAllocator allocator) {

        var mapping = new SchemaMapping(tracSchema);
        return mapping.tracToArrowSchema(tracSchema, allocator);
    }

    private SchemaMapping(SchemaDefinition tracSchema) {

        this.topLevelSchema = tracSchema;
        this.dictionaryFields = new HashMap<>();
        this.dictionaries = new DictionaryProvider.MapDictionaryProvider();
        this.namedEnums = new HashMap<>();
    }

    private ArrowVsrSchema tracToArrowSchema(SchemaDefinition tracSchema, BufferAllocator allocator) {

        // Build named enums first so they are available when processing fields
        if (tracSchema.getNamedEnumsCount() > 0)
            processNamedEnums(tracSchema, allocator);

        var tracFields = tracSchema.getSchemaType() == SchemaType.TABLE_SCHEMA && tracSchema.getFieldsCount() == 0
                ? tracSchema.getTable().getFieldsList()
                : tracSchema.getFieldsList();

        if (tracFields.isEmpty()) {
            throw new EValidationGap("Schema contains no fields");
        }

        var arrowFields = new ArrayList<Field>(tracFields.size());

        for (var tracField : tracFields) {
            var arrowField = tracToArrowField(tracField);
            arrowFields.add(arrowField);
        }

        var metadata = new HashMap<String, String>();
        var singleRecord = tracSchema.getSchemaType() == SchemaType.STRUCT_SCHEMA;

        if (singleRecord) {
            metadata.put("trac.schema.singleRecord", "true");
        }

        var schema = new Schema(arrowFields, metadata);

        return new ArrowVsrSchema(schema, dictionaryFields, dictionaries, singleRecord);
    }

    private Field tracToArrowField(FieldSchema tracField) {

        return tracToArrowField(tracField, tracField.getFieldName());
    }

    private Field tracToArrowField(FieldSchema tracField, String fieldName) {

        var arrowType = TRAC_ARROW_TYPE_MAPPING.get(tracField.getFieldType());
        var nullable = !(tracField.getBusinessKey() || tracField.getNotNull());

        // Unexpected error - All TRAC primitive types are mapped
        if (arrowType == null)
            throw new EUnexpected();

        // For complex types, process the child fields
        var children = ! TypeSystem.isPrimitive(tracField.getFieldType())
                ? tracToArrowFieldChildren(tracField)
                : null;

        if (tracField.hasNamedEnum()) {

            // Named enums have pre-built dictionaries
            var dictionaryId = namedEnums.get(tracField.getNamedEnum());
            var dictionary = dictionaries.lookup(dictionaryId);

            // Create an index field for the existing dictionary
            var encoding = dictionary.getEncoding();
            var indexFieldType = new FieldType(nullable, encoding.getIndexType(), encoding);
            return new Field(fieldName, indexFieldType, /* children = */ null);
        }

        else if (tracField.getCategorical()) {

            // Categorical without named enum requires a dedicated dictionary
            // Values not known ahead of time, so assign 32-bit index
            var dictionaryId = (long) dictionaryFields.size();
            var indexType = new ArrowType.Int(32, true);

            // Use the non-null version of the original type for the dictionary field
            var dictionaryFieldType = new FieldType(false, arrowType, /* dictionary = */ null);
            var dictionaryFiled = new Field(fieldName, dictionaryFieldType, children);
            dictionaryFields.put(dictionaryId, dictionaryFiled);

            // Create an index field for the newly defined dictionary
            var encoding = new DictionaryEncoding(dictionaryId, false, indexType);
            var indexFieldType = new FieldType(nullable, indexType, encoding);
            return new Field(fieldName, indexFieldType, /* children = */ null);
        }

        else {

            // Regular field, not dictionary encoded
            var fieldType = new FieldType(nullable, arrowType, /* dictionary = */ null);
            return new Field(fieldName, fieldType, children);
        }
    }

    private List<Field> tracToArrowFieldChildren(FieldSchema tracField) {

        switch (tracField.getFieldType()) {

            case ARRAY:
                return tracToArrowListChildren(tracField);

            case MAP:
                return tracToArrowMapChildren(tracField);

            case STRUCT:
                return tracToArrowStructChildren(tracField);

            default:
                return null;
        }
    }

    private List<Field> tracToArrowListChildren(FieldSchema tracField) {

        // List children are items = 0
        var tracItems = tracField.getChildren(0);
        var arrowItems = tracToArrowField(tracItems, ListVector.DATA_VECTOR_NAME);

        return List.of(arrowItems);
    }

    private List<Field> tracToArrowMapChildren(FieldSchema tracField) {

        // Map children are keys = 0, values = 1
        var tracKeys = tracField.getChildren(0);
        var tracValues = tracField.getChildren(1);
        var arrowKeys = tracToArrowField(tracKeys, MapVector.KEY_NAME);
        var arrowValues = tracToArrowField(tracValues, MapVector.VALUE_NAME);

        // Arrow maps are a list of entries, where the entry has keys and values as children
        var entriesType = new FieldType(false, new ArrowType.Struct(), null);
        var entriesField = new Field(MapVector.DATA_VECTOR_NAME, entriesType, List.of(arrowKeys, arrowValues));

        return List.of(entriesField);
    }

    private List<Field> tracToArrowStructChildren(FieldSchema tracField) {

        if (tracField.hasNamedType()) {

            if (!topLevelSchema.containsNamedTypes(tracField.getNamedType())) {
                var message = String.format("Named type is missing in the schema: [%s]",  tracField.getNamedType());
                throw new EValidationGap(message);
            }

            var childSchema = topLevelSchema.getNamedTypesOrThrow(tracField.getNamedType());
            var childFields = new ArrayList<Field>(childSchema.getFieldsCount());

            for (var tracChildField : childSchema.getFieldsList()) {
                var arrowChildField = tracToArrowField(tracChildField);
                childFields.add(arrowChildField);
            }

            return childFields;
        }
        else {

            var childFields = new ArrayList<Field>(tracField.getChildrenCount());

            for (var tracChildField : tracField.getChildrenList()) {
                var arrowChildField = tracToArrowField(tracChildField);
                childFields.add(arrowChildField);
            }

            return childFields;
        }
    }

    private void processNamedEnums(SchemaDefinition tracSchema, BufferAllocator allocator) {

        if (tracSchema.getNamedEnumsCount() > 0 && allocator == null) {
            throw new ETracInternal("Arrow buffer allocator cannot be null for schemas with named enums");
        }

        for (var entry : tracSchema.getNamedEnumsMap().entrySet()) {

            var name =  entry.getKey();
            var namedEnum = entry.getValue();

            if (namedEnum.getValuesCount() == 0) {
                var message = String.format("Named enum [%s] must have at least one value", name);
                throw new EValidationGap(message);
            }

            // Named enums are always string values
            var tracType = BasicType.STRING;
            var arrowType = TRAC_ARROW_TYPE_MAPPING.get(tracType);
            var fieldType = new FieldType(false, arrowType, /* dictionary = */ null);
            var field = new Field(name, fieldType, /* children = */ null);

            var dictionaryId = (long) dictionaryFields.size();
            dictionaryFields.put(dictionaryId, field);

            var dictionaryVector = field.createVector(allocator);
            dictionaryVector.setInitialCapacity(namedEnum.getValuesCount());

            for (int i = 0; i < namedEnum.getValuesCount(); i++) {
                var value = namedEnum.getValues(i);
                setDictionaryValue(name, dictionaryVector, i, value, tracType);
            }

            dictionaryVector.setValueCount(namedEnum.getValuesCount());

            var indexType = chooseIndexType(namedEnum.getValuesCount());
            var encoding = new DictionaryEncoding(dictionaryId, false, indexType);
            var dictionary = new Dictionary(dictionaryVector, encoding);

            dictionaries.put(dictionary);
            namedEnums.put(name, dictionaryId);
        }
    }

    private void setDictionaryValue(String name, FieldVector dictionaryVector, int index, Object value, BasicType tracType) {

        switch (tracType) {

            case STRING:
                if (dictionaryVector instanceof VarCharVector) {
                    var stringVector = (VarCharVector) dictionaryVector;
                    var stringBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                    stringVector.setSafe(index, stringBytes);
                }
                break;

            case INTEGER:
                if (dictionaryVector instanceof BaseIntVector) {
                    var intVector = (BaseIntVector) dictionaryVector;
                    var intValue = (Long) value;
                    intVector.setWithPossibleTruncate(index, intValue);
                }
                break;

            case DATE:
                if (dictionaryVector instanceof DateDayVector) {
                    var dateVector = (DateDayVector) dictionaryVector;
                    var dateValue = ((LocalDate) value).toEpochDay();
                    dateVector.set(index, (int) dateValue);
                }
                break;

            default:
                var message = String.format("Named enum [%s] has invalid type %s", name, tracType.name());
                throw new EValidationGap(message);
        }
    }

    private ArrowType.Int chooseIndexType(int nValues) {

        if (nValues <= (int) Byte.MAX_VALUE)
            return new ArrowType.Int(8, true);

        else if (nValues <= (int) Short.MAX_VALUE)
            return new ArrowType.Int(16, true);

        else
            return new ArrowType.Int(32, true);
    }
}
