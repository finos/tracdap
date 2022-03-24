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

package org.finos.tracdap.common.validation.core;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.metadata.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;


public class ValidationContextTest {

    private static final Descriptors.Descriptor TYPE_DESCRIPTOR;
    private static final Descriptors.FieldDescriptor TD_BASIC_TYPE;
    private static final Descriptors.FieldDescriptor TD_ARRAY_TYPE;
    private static final Descriptors.FieldDescriptor TD_MAP_TYPE;

    private static final Descriptors.Descriptor VALUE;
    private static final Descriptors.FieldDescriptor V_TYPE;
    private static final Descriptors.OneofDescriptor V_VALUE;
    private static final Descriptors.FieldDescriptor V_BOOLEAN;
    private static final Descriptors.FieldDescriptor V_INTEGER;
    private static final Descriptors.FieldDescriptor V_FLOAT;
    private static final Descriptors.FieldDescriptor V_DECIMAL;
    private static final Descriptors.FieldDescriptor V_STRING;
    private static final Descriptors.FieldDescriptor V_DATE;
    private static final Descriptors.FieldDescriptor V_DATETIME;
    private static final Descriptors.FieldDescriptor V_ARRAY;
    private static final Descriptors.FieldDescriptor V_MAP;

    private static final Descriptors.Descriptor DECIMAL_VALUE;
    private static final Descriptors.FieldDescriptor DCV_DECIMAL;

    private static final Descriptors.Descriptor DATE_VALUE;
    private static final Descriptors.FieldDescriptor DV_ISO_DATE;

    private static final Descriptors.Descriptor DATETIME_VALUE;
    private static final Descriptors.FieldDescriptor DTV_ISO_DATETIME;

    private static final Descriptors.Descriptor ARRAY_VALUE;
    private static final Descriptors.FieldDescriptor AV_ITEMS;

    private static final Descriptors.Descriptor MAP_VALUE;
    private static final Descriptors.FieldDescriptor MV_ENTRIES;


    static {

        TYPE_DESCRIPTOR = TypeDescriptor.getDescriptor();
        TD_BASIC_TYPE = field(TYPE_DESCRIPTOR, TypeDescriptor.BASICTYPE_FIELD_NUMBER);
        TD_ARRAY_TYPE = field(TYPE_DESCRIPTOR, TypeDescriptor.ARRAYTYPE_FIELD_NUMBER);
        TD_MAP_TYPE = field(TYPE_DESCRIPTOR, TypeDescriptor.MAPTYPE_FIELD_NUMBER);

        VALUE = Value.getDescriptor();
        V_TYPE = field(VALUE, Value.TYPE_FIELD_NUMBER);
        V_BOOLEAN = field(VALUE, Value.BOOLEANVALUE_FIELD_NUMBER);
        V_INTEGER = field(VALUE, Value.INTEGERVALUE_FIELD_NUMBER);
        V_FLOAT = field(VALUE, Value.FLOATVALUE_FIELD_NUMBER);
        V_DECIMAL = field(VALUE, Value.DECIMALVALUE_FIELD_NUMBER);
        V_STRING = field(VALUE, Value.STRINGVALUE_FIELD_NUMBER);
        V_DATE = field(VALUE, Value.DATEVALUE_FIELD_NUMBER);
        V_DATETIME = field(VALUE, Value.DATETIMEVALUE_FIELD_NUMBER);
        V_ARRAY = field(VALUE, Value.ARRAYVALUE_FIELD_NUMBER);
        V_MAP = field(VALUE, Value.MAPVALUE_FIELD_NUMBER);
        V_VALUE = V_BOOLEAN.getContainingOneof();

        DECIMAL_VALUE = DecimalValue.getDescriptor();
        DCV_DECIMAL = field(DECIMAL_VALUE, DecimalValue.DECIMAL_FIELD_NUMBER);

        DATE_VALUE = DateValue.getDescriptor();
        DV_ISO_DATE = field(DATE_VALUE, DateValue.ISODATE_FIELD_NUMBER);

        DATETIME_VALUE = DatetimeValue.getDescriptor();
        DTV_ISO_DATETIME = field(DATETIME_VALUE, DatetimeValue.ISODATETIME_FIELD_NUMBER);

        ARRAY_VALUE = ArrayValue.getDescriptor();
        AV_ITEMS = field(ARRAY_VALUE, ArrayValue.ITEMS_FIELD_NUMBER);

        MAP_VALUE = MapValue.getDescriptor();
        MV_ENTRIES = field(MAP_VALUE, MapValue.ENTRIES_FIELD_NUMBER);
    }

    @Test
    void pushPop_basic() {

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .setStringValue("asdf")
                .build();

        var ctx = ValidationContext.forMessage(value);
        ctx.push(V_TYPE);

        Assertions.assertEquals(V_TYPE.getName(), ctx.fieldName());
        Assertions.assertSame(type, ctx.target());

        ctx.pop();

        Assertions.assertSame(value, ctx.target());
    }

    @Test
    void pushPop_oneOf() {

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var stringValue = "asdf";

        var value = Value.newBuilder()
                .setType(type)
                .setStringValue(stringValue)
                .build();

        var ctx = ValidationContext.forMessage(value);
        ctx.pushOneOf(V_VALUE);

        Assertions.assertEquals(V_STRING.getName(), ctx.fieldName());
        Assertions.assertSame(stringValue, ctx.target());

        ctx.pop();

        Assertions.assertSame(value, ctx.target());
    }

    @Test
    void pushPop_oneOfNotSet() {

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .build();

        var ctx = ValidationContext.forMessage(value);
        ctx.pushOneOf(V_VALUE);

        Assertions.assertEquals(V_VALUE.getName(), ctx.fieldName());
        Assertions.assertNull(ctx.target());

        ctx.pop();

        Assertions.assertSame(value, ctx.target());
    }

    @Test
    void applyStatic_typedArg() {

        @SuppressWarnings("unchecked")
        var validator = (ValidationFunction.TypedArg<TypeDescriptor, Object>)
                mock(ValidationFunction.TypedArg.class);

        doAnswer(invoke -> invoke.getArgument(2))
                .when(validator)
                .apply(any(), any(), any());

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .build();

        var arg = new Object();

        var ctx = ValidationContext.forMessage(value);

        ctx.push(V_TYPE)
            .applyWith(validator, TypeDescriptor.class, arg)
            .pop();

        verify(validator).apply(same(type), same(arg), same(ctx));
    }

    @Test
    void applyStatic_typed() {

        @SuppressWarnings("unchecked")
        var validator = (ValidationFunction.Typed<TypeDescriptor>)
                mock(ValidationFunction.Typed.class);

        doAnswer(invoke -> invoke.getArgument(1))
                .when(validator)
                .apply(any(), any());

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .build();

        var ctx = ValidationContext.forMessage(value);

        ctx.push(V_TYPE)
                .apply(validator, TypeDescriptor.class)
                .pop();

        verify(validator).apply(same(type), same(ctx));
    }

    @Test
    void applyStatic_basic() {

        var validator = mock(ValidationFunction.Basic.class);
        doAnswer(invoke -> invoke.getArgument(0))
                .when(validator)
                .apply(any());

        var type = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.STRING)
                .build();

        var value = Value.newBuilder()
                .setType(type)
                .build();

        var ctx = ValidationContext.forMessage(value);

        ctx.push(V_TYPE)
                .apply(validator)
                .pop();

        verify(validator).apply(same(ctx));
    }
}
