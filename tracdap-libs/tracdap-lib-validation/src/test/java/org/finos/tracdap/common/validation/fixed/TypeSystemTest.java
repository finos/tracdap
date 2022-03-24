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

package org.finos.tracdap.common.validation.fixed;

import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.meta.TestData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.LocalDate;


class TypeSystemTest {

    static Validator validator;

    @BeforeAll
    static void setupValidator() {

        validator = new Validator();
    }

    static <TMsg extends Message> void expectValid(TMsg msg) {

        Assertions.assertDoesNotThrow(
                () -> validator.validateFixedObject(msg),
                "Validation failed for a valid message");
    }

    static <TMsg extends Message> void expectInvalid(TMsg msg) {

        Assertions.assertThrows(EInputValidation.class,
                () -> validator.validateFixedObject(msg),
                "Validation passed for an invalid message");
    }

    @Test
    void typeDescriptor_okPrimitive() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build());
    }

    @Test
    void typeDescriptor_okArray() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING))
                .build());
    }

    @Test
    void typeDescriptor_okArrayNested() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.INTEGER)))
                .build());
    }

    @Test
    void typeDescriptor_okMap() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING))
                .build());
    }

    @Test
    void typeDescriptor_okMapNested() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.INTEGER)))
                .build());
    }

    @Test
    void typeDescriptor_okMapOfArray() {

        expectValid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)))
                .build());
    }

    @Test
    void typeDescriptor_noBasicType() {

        expectInvalid(TypeDescriptor.newBuilder().build());
    }

    @Test
    void typeDescriptor_badArrayType() {

        expectInvalid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .build());

        expectInvalid(TypeDescriptor.newBuilder()
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build());

        expectInvalid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build());

        expectInvalid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build());
    }

    @Test
    void typeDescriptor_badMapType() {

        expectInvalid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .build());

        expectInvalid(TypeDescriptor.newBuilder()
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build());

        expectInvalid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build());

        expectInvalid(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build());
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY", "MAP"})
    void primitive_typeExplicit(BasicType basicType) {

        // Basic primitive values

        var value = TestData.randomPrimitive(basicType);
        expectValid(value);
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY", "MAP"})
    void primitive_typeInferred(BasicType basicType) {

        // If a primitive value is supplied, type can be omitted and will be inferred

        var value = TestData.randomPrimitive(basicType)
                .toBuilder()
                .clearType()
                .build();

        expectValid(value);
    }

    @Test
    void primitive_typeMismatch() {

        // if value and type are both supplied, they must match

        var value = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.INTEGER))
                .setFloatValue(3.4)
                .build();

        expectInvalid(value);
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY", "MAP"})
    void primitive_null(BasicType basicType) {

        // All primitive type values can be set to null, so long as a type descriptor is supplied

        var value = Value.newBuilder()
                .setType(TypeSystem.descriptor(basicType))
                .build();

        expectValid(value);
    }

    @Test
    void date_valid() {

        var value = TestData.randomPrimitive(BasicType.DATE);

        expectValid(value);
    }

    @Test
    void date_invalid() {

        var value1 = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATE))
                .setDateValue(DateValue.newBuilder().setIsoDate("not_a_valid_date"))
                .build();

        expectInvalid(value1);

        var value2 = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATE))
                .setDateValue(DateValue.newBuilder())   // missing ISO date
                .build();

        expectInvalid(value2);
    }

    @Test
    void datetime_valid() {

        var value = TestData.randomPrimitive(BasicType.DATETIME);

        expectValid(value);
    }

    @Test
    void datetime_invalid() {

        var value1 = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATETIME))
                .setDatetimeValue(DatetimeValue.newBuilder().setIsoDatetime("not_a_valid_datetime"))
                .build();

        expectInvalid(value1);

        var value2 = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATETIME))
                .setDatetimeValue(DatetimeValue.newBuilder())  // missing ISO datetime
                .build();

        expectInvalid(value2);
    }

    @Test
    void decimal_valid() {

        var value = TestData.randomPrimitive(BasicType.DECIMAL);

        expectValid(value);
    }

    @Test
    void decimal_invalid() {

        var value1 = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DECIMAL))
                .setDecimalValue(DecimalValue.newBuilder().setDecimal("not_a_valid_decimal"))
                .build();

        expectInvalid(value1);

        var value2 = Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DECIMAL))
                .setDecimalValue(DecimalValue.newBuilder())  // missing decimal string
                .build();

        expectInvalid(value2);
    }

    @Test
    void array_TypeExplicit() {

        // Array items with explicit types in each item

        var itemType = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(itemType)
                .build();

        var array = ArrayValue.newBuilder();

        for (var i = 0; i < 10; i++)
            array.addItems(Value.newBuilder().setType(itemType).setStringValue("array_value_" + i));

        var value = Value.newBuilder()
                .setType(type)
                .setArrayValue(array)
                .build();

        expectValid(value);
    }

    @Test
    void array_TypeInferred() {

        // Array with one type descriptor, type is inferred for individual items

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var array = ArrayValue.newBuilder();

        for (var i = 0; i < 10; i++)
            array.addItems(Value.newBuilder().setStringValue("array_value_" + i));

        var value = Value.newBuilder()
                .setType(type)
                .setArrayValue(array)
                .build();

        expectValid(value);
    }

    @Test
    void array_nested() {

        // Nested array with one type descriptor, type is inferred for individual items

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                        .setBasicType(BasicType.ARRAY)
                        .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING)))
                .build();

        var array = ArrayValue.newBuilder();

        for (var i = 0; i < 10; i++) {

            var subArray = ArrayValue.newBuilder();

            for (var j = 0; j < 10; j++)
                subArray.addItems(Value.newBuilder().setStringValue("array_value_" + i + "_" + j));

            array.addItems(Value.newBuilder().setArrayValue(subArray));
        }

        var value = Value.newBuilder()
                .setType(type)
                .setArrayValue(array)
                .build();

        expectValid(value);
    }

    @Test
    void array_itemInvalid() {

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.DECIMAL))
                .build();

        var array = ArrayValue.newBuilder();
        array.addItems(MetadataCodec.encodeValue(BigDecimal.ONE));
        array.addItems(MetadataCodec.encodeValue(BigDecimal.valueOf(3.4567)));
        array.addItems(Value.newBuilder().setDecimalValue(DecimalValue.newBuilder().setDecimal("invalid_decimal")));

        var value = Value.newBuilder()
                .setType(type)
                .setArrayValue(array)
                .build();

        expectInvalid(value);
    }

    @Test
    void array_itemWrongType() {

        // An array can contain null values

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.INTEGER))
                .build();

        // Array with a float value, no type descriptor

        var array1 = ArrayValue.newBuilder();
        array1.addItems(Value.newBuilder().setIntegerValue(1));
        array1.addItems(Value.newBuilder().setIntegerValue(2));
        array1.addItems(Value.newBuilder().setFloatValue(3.0));

        var value1 = Value.newBuilder()
                .setType(type)
                .setArrayValue(array1)
                .build();

        expectInvalid(value1);

        // Array with a null float value

        var array2 = ArrayValue.newBuilder();
        array2.addItems(Value.newBuilder().setIntegerValue(1));
        array2.addItems(Value.newBuilder().setIntegerValue(2));
        array2.addItems(Value.newBuilder().setType(TypeSystem.descriptor(BasicType.FLOAT)));

        var value2 = Value.newBuilder()
                .setType(type)
                .setArrayValue(array2)
                .build();

        expectInvalid(value2);
    }

    @Test
    void array_canIncludeNull() {

        // An array can contain null values

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var array = ArrayValue.newBuilder();
        array.addItems(Value.newBuilder().setStringValue("array_value_1"));
        // null value with explicit type
        array.addItems(Value.newBuilder().setType(TypeSystem.descriptor(BasicType.STRING)));
        // null value with inferred type
        array.addItems(Value.newBuilder());
        array.addItems(Value.newBuilder().setStringValue("array_value_4"));

        var value = Value.newBuilder()
                .setType(type)
                .setArrayValue(array)
                .build();

        expectValid(value);
    }

    @Test
    void array_noTypeInfo() {

        var array = ArrayValue.newBuilder();
        array.addItems(Value.newBuilder().setStringValue("array_value_1"));
        array.addItems(Value.newBuilder().setStringValue("array_value_2"));

        var value = Value.newBuilder()
                .setArrayValue(array)
                .build();

        expectInvalid(value);
    }

    @Test
    void array_isNull() {

        // An array cannot itself be null - an ArrayValue must be provided, even if it has zero elements

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .build();

        expectInvalid(value);
    }

    @Test
    void array_isEmpty() {

        // Empty arrays are allowed

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .setArrayValue(ArrayValue.newBuilder())
                .build();

        expectValid(value);
    }

    @Test
    void map_typeExplicit() {

        // Map entries with explicit types in each item

        var entryType = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(entryType)
                .build();

        var map = MapValue.newBuilder();

        for (var i = 0; i < 10; i++)
            map.putEntries("key_" + i, Value.newBuilder().setType(entryType).setStringValue("map_value_" + i).build());

        var value = Value.newBuilder()
                .setType(type)
                .setMapValue(map)
                .build();

        expectValid(value);
    }

    @Test
    void map_typeInferred() {

        // Map with one type descriptor, type is inferred for individual entries

        var entryType = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(entryType)
                .build();

        var map = MapValue.newBuilder();

        for (var i = 0; i < 10; i++)
            map.putEntries("key_" + i, Value.newBuilder().setStringValue("map_value_" + i).build());

        var value = Value.newBuilder()
                .setType(type)
                .setMapValue(map)
                .build();

        expectValid(value);
    }

    @Test
    void map_nested() {

        // Nested map with one type descriptor, type is inferred for individual entries

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                        .setBasicType(BasicType.MAP)
                        .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING)))
                .build();

        var map = MapValue.newBuilder();

        for (var i = 0; i < 10; i++) {

            var subMap = MapValue.newBuilder();

            for (var j = 0; j < 10; j++)
                subMap.putEntries("key_" + j, Value.newBuilder().setStringValue("map_value_" + i + "_" + j).build());

            map.putEntries("key_" + i, Value.newBuilder().setMapValue(subMap).build());
        }

        var value = Value.newBuilder()
                .setType(type)
                .setMapValue(map)
                .build();

        expectValid(value);
    }

    @Test
    void map_entryInvalid() {

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.DATE))
                .build();

        var map = MapValue.newBuilder();
        map.putEntries("key_1", MetadataCodec.encodeValue(LocalDate.now()));
        map.putEntries("key_2", MetadataCodec.encodeValue(LocalDate.now().plusDays(1)));
        map.putEntries("key_3", Value.newBuilder().setDateValue(DateValue.newBuilder().setIsoDate("not_a_valid_date")).build());

        var value = Value.newBuilder()
                .setType(type)
                .setMapValue(map)
                .build();

        expectInvalid(value);
    }

    @Test
    void map_entryWrongType() {

        // An array can contain null values

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.INTEGER))
                .build();

        // Map with a float value, no type descriptor

        var map1 = MapValue.newBuilder();
        map1.putEntries("key_1", Value.newBuilder().setIntegerValue(1).build());
        map1.putEntries("key_2", Value.newBuilder().setIntegerValue(2).build());
        map1.putEntries("key_3", Value.newBuilder().setFloatValue(3.0).build());

        var value1 = Value.newBuilder()
                .setType(type)
                .setMapValue(map1)
                .build();

        expectInvalid(value1);

        // Map with a null float value

        var map2 = MapValue.newBuilder();
        map2.putEntries("key_1", Value.newBuilder().setIntegerValue(1).build());
        map2.putEntries("key_2", Value.newBuilder().setIntegerValue(2).build());
        map2.putEntries("key_3", Value.newBuilder().setType(TypeSystem.descriptor(BasicType.FLOAT)).build());

        var value2 = Value.newBuilder()
                .setType(type)
                .setMapValue(map2)
                .build();

        expectInvalid(value2);
    }

    @Test
    void map_canIncludeNull() {

        // A map can contain null values

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var map = MapValue.newBuilder();
        map.putEntries("key_1", Value.newBuilder().setStringValue("map_value_1").build());
        // null value with explicit type
        map.putEntries("key_2", Value.newBuilder().setType(TypeSystem.descriptor(BasicType.STRING)).build());
        // null value with inferred type
        map.putEntries("key_3", Value.newBuilder().build());
        map.putEntries("key_4", Value.newBuilder().setStringValue("map_value_4").build());

        var value = Value.newBuilder()
                .setType(type)
                .setMapValue(map)
                .build();

        expectValid(value);
    }

    @Test
    void map_keyNotBlank() {

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var map = MapValue.newBuilder();
        map.putEntries("key_1", Value.newBuilder().setStringValue("map_value_1").build());
        map.putEntries("", Value.newBuilder().setStringValue("map_value_2").build());

        var value = Value.newBuilder()
                .setType(type)
                .setMapValue(map)
                .build();

        expectInvalid(value);
    }

    @Test
    void map_noTypeInfo() {

        var map = MapValue.newBuilder();
        map.putEntries("key_1", Value.newBuilder().setStringValue("map_value_1").build());
        map.putEntries("key_2", Value.newBuilder().setStringValue("map_value_2").build());

        var value = Value.newBuilder()
                .setMapValue(map)
                .build();

        expectInvalid(value);
    }

    @Test
    void map_isNull() {

        // A map cannot itself be null - MapValue must be provided, even if it has zero elements

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .build();

        expectInvalid(value);
    }

    @Test
    void map_isEmpty() {

        // Empty maps are allowed

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING))
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .setMapValue(MapValue.newBuilder())
                .build();

        expectValid(value);
    }

    @Test
    void value_nestingValid() {

        var validDate = MetadataCodec.encodeDate(LocalDate.now());
        var validValue = Value.newBuilder().setDateValue(validDate);

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.DATE)))))
                .build();

        var level1 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(validValue));
        var level2 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(level1));
        var level3 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(level2));
        var level4 = Value.newBuilder().setMapValue(MapValue.newBuilder().putEntries("map_key", level3.build()));

        var value = level4.setType(type).build();

        expectValid(value);
    }

    @Test
    void value_nestingInvalid1() {

        // Type descriptor does not match value type after four levels of nesting

        var validDate = MetadataCodec.encodeDate(LocalDate.now());
        var validValue = Value.newBuilder().setDateValue(validDate);

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.STRING)))))
                .build();

        var level1 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(validValue));
        var level2 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(level1));
        var level3 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(level2));
        var level4 = Value.newBuilder().setMapValue(MapValue.newBuilder().putEntries("map_key", level3.build()));

        var value = level4.setType(type).build();

        expectInvalid(value);
    }

    @Test
    void value_nestingInvalid2() {

        // Type matches, but value is invalid after four levels of nesting

        var invalidDate = DateValue.newBuilder().setIsoDate("not_a_valid_date");
        var invalidValue = Value.newBuilder().setDateValue(invalidDate);

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.MAP)
                .setMapType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder().setBasicType(BasicType.DATE)))))
                .build();

        var level1 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(invalidValue));
        var level2 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(level1));
        var level3 = Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addItems(level2));
        var level4 = Value.newBuilder().setMapValue(MapValue.newBuilder().putEntries("map_key", level3.build()));

        var value = level4.setType(type).build();

        expectInvalid(value);
    }

    @Test
    void value_nullWithoutType() {

        // Values with no type or explicit value are not allowed
        // Because the type cannot be inferred

        var value = Value.newBuilder().build();

        expectInvalid(value);
    }
}
