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

import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.metadata.*;
import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.FIXED)
public class TypeSystemValidator {

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
    }

    @Validator
    public static ValidationContext typeDescriptor(TypeDescriptor typeDescriptor, ValidationContext ctx) {

        ctx = ctx.push(TD_BASIC_TYPE)
            .apply(CommonValidators::required)
            .apply(CommonValidators::nonZeroEnum, BasicType.class)
            .pop();

        var isArray = typeDescriptor.getBasicType() == BasicType.ARRAY;
        var isMap = typeDescriptor.getBasicType() == BasicType.MAP;

        ctx = ctx.push(TD_ARRAY_TYPE)
                .applyIf(CommonValidators::omitted, !isArray)
                .applyIf(CommonValidators::required, isArray)
                .applyIf(TypeSystemValidator::typeDescriptor, TypeDescriptor.class, isArray)
                .pop();

        ctx = ctx.push(TD_MAP_TYPE)
                .applyIf(CommonValidators::omitted, !isMap)
                .applyIf(CommonValidators::required, isMap)
                .applyIf(TypeSystemValidator::typeDescriptor, TypeDescriptor.class, isMap)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext value(Value value, ValidationContext ctx) {

        if (value.hasType())
            return ctx.applyWith(TypeSystemValidator::value, Value.class, value.getType());

        if (!value.hasOneof(V_VALUE))
            return ctx.error(String.format("Type cannot be inferred for null value [%s]", ctx.fieldName()));

        if (!TypeSystem.isPrimitive(value))
            return ctx.error(String.format("Type cannot be inferred for non-primitive value [%s]", ctx.fieldName()));

        var type = TypeSystem.descriptor(value);
        return ctx.applyWith(TypeSystemValidator::value, Value.class, type);
    }

    public static ValidationContext value(Value value, TypeDescriptor expectedType, ValidationContext ctx) {

        var wrongTypeMessage = String.format("Wrong type supplied for [%s]", ctx.fieldName());

        ctx = ctx.push(V_TYPE)
                .apply(CommonValidators::optional)
                .apply(TypeSystemValidator::typeDescriptor, TypeDescriptor.class)
                .apply(CommonValidators.equalTo(expectedType, wrongTypeMessage), TypeDescriptor.class)
                .pop();

        if (TypeSystem.isPrimitive(expectedType)) {

            var valueType = TypeSystem.valueCaseType(value);

            ctx = ctx.pushOneOf(V_VALUE)
                    .apply(CommonValidators::optional)
                    .applyIf(TypeSystemValidator::decimalValue, DecimalValue.class, valueType == BasicType.DECIMAL)
                    .applyIf(TypeSystemValidator::dateValue, DateValue.class, valueType == BasicType.DATE)
                    .applyIf(TypeSystemValidator::datetimeValue, DatetimeValue.class, valueType == BasicType.DATETIME)
                    .pop();

            // If the value is non-null, make sure the value matches its type descriptor
            if (value.hasOneof(V_VALUE) && valueType != expectedType.getBasicType())
                ctx = ctx.error(wrongTypeMessage);

            return ctx;
        }

        if (expectedType.getBasicType() == BasicType.ARRAY) {

            var arrayType = expectedType.getArrayType();

            return ctx.push(V_ARRAY)
                    .apply(CommonValidators::required)
                    .applyWith(TypeSystemValidator::arrayValue, ArrayValue.class, arrayType)
                    .pop();
        }

        ctx = ctx.error("Maps not implemented yet");

        return ctx;
    }

    public static ValidationContext decimalValue(DecimalValue msg, ValidationContext ctx) {

        return ctx.push(DCV_DECIMAL)
                .apply(CommonValidators::required)
                .apply(CommonValidators::decimal)
                .pop();
    }

    public static ValidationContext dateValue(DateValue msg, ValidationContext ctx) {

        return ctx.push(DV_ISO_DATE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::isoDate)
                .pop();
    }

    public static ValidationContext datetimeValue(DatetimeValue msg, ValidationContext ctx) {

        return ctx.push(DTV_ISO_DATETIME)
                .apply(CommonValidators::required)
                .apply(CommonValidators::isoDatetime)
                .pop();
    }

    public static ValidationContext arrayValue(ArrayValue msg, TypeDescriptor arrayType, ValidationContext ctx) {

        return ctx.push(AV_ITEMS)
                .applyListWith(TypeSystemValidator::value, Value.class, arrayType)
                .pop();
    }


    public static ValidationContext primitive(BasicType basicType, ValidationContext ctx) {

        if (!TypeSystem.isPrimitive(basicType)) {
            var err = String.format("Type specified in [%s] is not a primitive type: [%s]", ctx.fieldName(), basicType);
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext primitive(TypeDescriptor typeDescriptor, ValidationContext ctx) {

        if (!TypeSystem.isPrimitive(typeDescriptor)) {
            var err = String.format("Type specified in [%s] is not a primitive type: [%s]", ctx.fieldName(), typeDescriptor.getBasicType());
            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext primitiveValue(Value value, ValidationContext ctx) {

        if (!TypeSystem.isPrimitive(value)) {
            var err = String.format("Value [%s] is not a primitive value: [%s]", ctx.fieldName(), TypeSystem.basicType(value));
            return ctx.error(err);
        }

        return ctx;
    }


}
