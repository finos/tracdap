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

import org.apache.arrow.memory.RootAllocator;
import org.finos.tracdap.test.data.SampleData;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Function;
import java.util.stream.Collectors;


class SchemaMappingTest {

    @Test
    void testTableSchemaConversion() {

        // Get the BASIC_TABLE_SCHEMA from SampleData
        var tracSchema = SampleData.BASIC_TABLE_SCHEMA;

        // Convert TRAC schema to Arrow schema
        var arrowSchema = SchemaMapping.tracToArrow(tracSchema);

        // Verify the schema is not null
        Assertions.assertNotNull(arrowSchema, "Arrow schema should not be null");

        // Get the logical and physical fields for verification
        var logicalFields = arrowSchema.logical()
                .getFields().stream()
                .collect(Collectors.toMap(
                        Field::getName,
                        Function.identity()));

        var physicalFields = arrowSchema.physical()
                .getFields().stream()
                .collect(Collectors.toMap(
                        Field::getName,
                        Function.identity()));

        // Verify basic field types in logical schema
        Assertions.assertEquals(ArrowType.Bool.INSTANCE, logicalFields.get("boolean_field").getType(),
                "boolean_field should be BOOLEAN");

        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_INTEGER, logicalFields.get("integer_field").getType(),
                "integer_field should be INTEGER");

        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_FLOAT, logicalFields.get("float_field").getType(),
                "float_field should be FLOAT");

        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_DECIMAL, logicalFields.get("decimal_field").getType(),
                "decimal_field should be DECIMAL");

        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, logicalFields.get("string_field").getType(),
                "string_field should be STRING");

        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_DATE, logicalFields.get("date_field").getType(),
                "date_field should be DATE");

        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_DATETIME, logicalFields.get("datetime_field").getType(),
                "datetime_field should be DATETIME");

        // Verify categorical field in both logical and physical schemas
        var logicalCategoricalField = logicalFields.get("categorical_field");
        var physicalCategoricalField = physicalFields.get("categorical_field");

        Assertions.assertNull(logicalCategoricalField.getDictionary(),
                "Logical categorical_field should not have a dictionary");

        Assertions.assertEquals(ArrowType.Utf8.INSTANCE, logicalCategoricalField.getType(),
                "Logical categorical_field should be STRING type");

        Assertions.assertNotNull(physicalCategoricalField.getDictionary(),
                "Physical categorical_field should have a dictionary");

        Assertions.assertInstanceOf(ArrowType.Int.class, physicalCategoricalField.getType(),
                "Physical categorical_field should be INT type");
    }

    @Test
    void testStructSchemaConversion() {

        var allocator = new RootAllocator();
        var tracSchema = SampleData.BASIC_STRUCT_SCHEMA;
        var arrowSchema = SchemaMapping.tracToArrow(tracSchema, allocator);

        Assertions.assertNotNull(arrowSchema, "Arrow schema should not be null");

        var logicalFields = arrowSchema.logical()
                .getFields().stream()
                .collect(Collectors.toMap(
                        Field::getName,
                        Function.identity()));

        var physicalFields = arrowSchema.physical()
                .getFields().stream()
                .collect(Collectors.toMap(
                        Field::getName,
                        Function.identity()));
        
        // Verify basic field types
        Assertions.assertEquals(ArrowType.Bool.INSTANCE, logicalFields.get("boolField").getType(), "boolField should be BOOLEAN");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_INTEGER, logicalFields.get("intField").getType(), "intField should be INTEGER");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_FLOAT, logicalFields.get("floatField").getType(), "floatField should be FLOAT");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_DECIMAL, logicalFields.get("decimalField").getType(), "decimalField should be DECIMAL");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, logicalFields.get("strField").getType(), "strField should be STRING");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_DATE, logicalFields.get("dateField").getType(), "dateField should be DATE");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_DATETIME, logicalFields.get("datetimeField").getType(), "datetimeField should be DATETIME");
        
        // Verify enum field is a string with dictionary encoding
        var logicalEnumField = logicalFields.get("enumField");
        var physicalEnumField = physicalFields.get("enumField");
        Assertions.assertNull(logicalEnumField.getDictionary(), "logical enumField should not have a dictionary");
        Assertions.assertEquals(ArrowType.Utf8.INSTANCE, logicalEnumField.getType(), "Logical enumField should be STRING type");
        Assertions.assertNotNull(physicalEnumField.getDictionary(), "logical enumField should have a dictionary");
        Assertions.assertInstanceOf(ArrowType.Int.class, physicalEnumField.getType(), "Physical enumField should be INT type");
        
        // Verify list field and its child schema
        var listField = logicalFields.get("listField");
        Assertions.assertInstanceOf(ArrowType.List.class, listField.getType(), "listField should be a LIST type");
        var listFieldChildren = listField.getChildren();
        Assertions.assertEquals(1, listFieldChildren.size(), "listField should have exactly one child field");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_INTEGER, listFieldChildren.get(0).getType(),
                "listField child should be INTEGER type");
        
        // Verify map field and its key/value schemas
        var mapField = logicalFields.get("dictField");
        Assertions.assertInstanceOf(ArrowType.Map.class, mapField.getType(), "dictField should be a MAP type");
        var mapFieldChildren = mapField.getChildren();
        Assertions.assertEquals(1, mapFieldChildren.size(), "mapField should have exactly one child field (entries)");
        
        var mapEntryField = mapFieldChildren.get(0);
        Assertions.assertEquals("entries", mapEntryField.getName(), "Map entries field should be named 'entries'");
        Assertions.assertEquals(2, mapEntryField.getChildren().size(), "Map entries should have key and value fields");
        
        var mapKeyField = mapEntryField.getChildren().get(0);
        var mapValueField = mapEntryField.getChildren().get(1);
        
        Assertions.assertEquals("key", mapKeyField.getName(), "Map key field should be named 'key'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, mapKeyField.getType(), 
                "Map key should be STRING type");
                
        Assertions.assertEquals("value", mapValueField.getName(), "Map value field should be named 'value'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_DATETIME, mapValueField.getType(),
                "Map value should be DATETIME type");
        
        // Verify anonymous struct field and its fields
        var structField = logicalFields.get("anonymousStructField");
        Assertions.assertInstanceOf(ArrowType.Struct.class, structField.getType(), "anonymousStructField should be a STRUCT type");
        
        var structFields = structField.getChildren();
        Assertions.assertEquals(3, structFields.size(), "anonymousStructField should have 3 fields");
        
        var field1 = structFields.get(0);
        var field2 = structFields.get(1);
        var enumField = structFields.get(2);
        
        Assertions.assertEquals("field1", field1.getName(), "First struct field should be named 'field1'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, field1.getType(), 
                "field1 should be STRING type");
        
        Assertions.assertEquals("field2", field2.getName(), "Second struct field should be named 'field2'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_INTEGER, field2.getType(), 
                "field2 should be INTEGER type");
                
        Assertions.assertEquals("enumField", enumField.getName(), "Third struct field should be named 'enumField'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, enumField.getType(),
                "enumField should be STRING type");

        var physicalAnonStructEnumField = physicalFields.get("anonymousStructField").getChildren().get(2);
        Assertions.assertEquals("enumField", physicalAnonStructEnumField.getName(), "Third struct field should be named 'enumField'");
        Assertions.assertNotNull(physicalAnonStructEnumField.getDictionary(), "enumField should have a dictionary");

        // Verify struct field (named type) is dereferenced to match anonymous struct
        var namedStructField = logicalFields.get("structField");
        Assertions.assertInstanceOf(ArrowType.Struct.class, namedStructField.getType(), "structField should be a STRUCT type");
        
        var namedStructFields = namedStructField.getChildren();
        Assertions.assertEquals(3, namedStructFields.size(), "structField should have 3 fields (dereferenced from named type)");
        
        var namedField1 = namedStructFields.get(0);
        var namedField2 = namedStructFields.get(1);
        var namedEnumField = namedStructFields.get(2);
        
        Assertions.assertEquals("field1", namedField1.getName(), "First named struct field should be named 'field1'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, namedField1.getType(), 
                "field1 in named struct should be STRING type");
        
        Assertions.assertEquals("field2", namedField2.getName(), "Second named struct field should be named 'field2'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_INTEGER, namedField2.getType(), 
                "field2 in named struct should be INTEGER type");
                
        Assertions.assertEquals("enumField", namedEnumField.getName(), "Third named struct field should be named 'enumField'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, namedEnumField.getType(),
                "enumField in named struct should be STRING type");

        var physicalStructEnumField = physicalFields.get("structField").getChildren().get(2);
        Assertions.assertEquals("enumField", physicalStructEnumField.getName(), "Third named struct field should be named 'enumField'");
        Assertions.assertNotNull(physicalStructEnumField.getDictionary(), "enumField in named struct should have a dictionary");
        
        // Verify nested struct field (as map) and its key/value schemas
        var nestedStructField = logicalFields.get("nestedStructField");
        Assertions.assertInstanceOf(ArrowType.Map.class, nestedStructField.getType(), "nestedStructField should be a MAP type");
        
        var nestedStructChildren = nestedStructField.getChildren();
        Assertions.assertEquals(1, nestedStructChildren.size(), "nestedStructField should have exactly one child field (entries)");
        
        var nestedStructEntryField = nestedStructChildren.get(0);
        Assertions.assertEquals("entries", nestedStructEntryField.getName(), "Nested struct entries field should be named 'entries'");
        
        var nestedStructValueField = nestedStructEntryField.getChildren().get(1);
        Assertions.assertEquals("value", nestedStructValueField.getName(), "Nested struct value field should be named 'value'");
        
        // Verify the nested struct's fields (should be dereferenced from the named type 'DataClassSubStruct')
        var nestedStructFields = nestedStructValueField.getChildren();
        Assertions.assertEquals(3, nestedStructFields.size(), "Nested struct should have 3 fields (dereferenced from named type)");
        
        var nestedField1 = nestedStructFields.get(0);
        var nestedField2 = nestedStructFields.get(1);
        var nestedEnumField = nestedStructFields.get(2);
        
        Assertions.assertEquals("field1", nestedField1.getName(), "First nested struct field should be named 'field1'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, nestedField1.getType(), 
                "field1 in nested struct should be STRING type");
        
        Assertions.assertEquals("field2", nestedField2.getName(), "Second nested struct field should be named 'field2'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_INTEGER, nestedField2.getType(), 
                "field2 in nested struct should be INTEGER type");
                
        Assertions.assertEquals("enumField", nestedEnumField.getName(), "Third nested struct field should be named 'enumField'");
        Assertions.assertEquals(SchemaMapping.ARROW_BASIC_STRING, nestedEnumField.getType(),
                "enumField in nested struct should be STRING type");
        
        // Verify the physical schema for the nested struct's enum field has a dictionary
        var physicalNestedStructField = physicalFields.get("nestedStructField");
        var physicalNestedEntries = physicalNestedStructField.getChildren().get(0);
        var physicalNestedValue = physicalNestedEntries.getChildren().get(1);
        var physicalNestedEnumField = physicalNestedValue.getChildren().get(2);
        
        Assertions.assertEquals("enumField", physicalNestedEnumField.getName(), "Third nested struct field should be named 'enumField'");
        Assertions.assertNotNull(physicalNestedEnumField.getDictionary(), "enumField in nested struct should have a dictionary");
    }
}
