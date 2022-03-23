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
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.*;
import org.finos.tracdap.test.meta.TestData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


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
    void value_primitive(BasicType basicType) {

        // Basic primitive values

        var value = TestData.randomPrimitive(basicType);
        expectValid(value);
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY", "MAP"})
    void value_primitiveTypeInferred(BasicType basicType) {

        // If a primitive value is supplied, type can be omitted and will be inferred

        var value = TestData.randomPrimitive(basicType)
                .toBuilder()
                .clearType()
                .build();

        expectValid(value);
    }

    @Test
    void value_primitiveTypeMismatch() {

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
    void value_nullPrimitive(BasicType basicType) {

        // All primitive type values can be set to null, so long as a type descriptor is supplied

        var value = Value.newBuilder()
                .setType(TypeSystem.descriptor(basicType))
                .build();

        expectValid(value);
    }

    @Test
    void value_nullWithoutType() {

        // Values with no type or explicit value are not allowed
        // Because the type cannot be inferred

        var value = Value.newBuilder().build();

        expectInvalid(value);
    }

    @Test
    void value_arrayTypeExplicit() {

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
    void value_arrayTypeInferred() {

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
    void value_arrayNested() {

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
    void value_arrayCanIncludeNull() {

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
        array.addItems(Value.newBuilder().setStringValue("array_value_3"));

        var value = Value.newBuilder()
                .setType(type)
                .setArrayValue(array)
                .build();

        expectValid(value);
    }

    @Test
    void value_arrayIsNull() {

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
    void value_arrayIsEmpty() {

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
    void value_okMap() {

    }

    @Test
    void value_okMapNested() {

    }

    @Test
    void value_okMapOfArray() {

    }

    @Test
    void value_okNull() {

    }
}
