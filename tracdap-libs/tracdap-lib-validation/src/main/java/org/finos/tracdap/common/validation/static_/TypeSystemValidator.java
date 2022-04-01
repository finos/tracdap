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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.metadata.*;
import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class TypeSystemValidator {

    private static final Descriptors.Descriptor TYPE_DESCRIPTOR;
    private static final Descriptors.FieldDescriptor TD_BASIC_TYPE;
    private static final Descriptors.FieldDescriptor TD_ARRAY_TYPE;
    private static final Descriptors.FieldDescriptor TD_MAP_TYPE;

    private static final Descriptors.Descriptor VALUE;
    private static final Descriptors.FieldDescriptor V_TYPE;
    private static final Descriptors.OneofDescriptor V_VALUE;
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
        V_VALUE = field(VALUE, Value.BOOLEANVALUE_FIELD_NUMBER).getContainingOneof();
        V_ARRAY = field(VALUE, Value.ARRAYVALUE_FIELD_NUMBER);
        V_MAP = field(VALUE, Value.MAPVALUE_FIELD_NUMBER);

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

    @Validator
    public static ValidationContext typeDescriptor(TypeDescriptor typeDescriptor, ValidationContext ctx) {

        ctx = ctx.push(TD_BASIC_TYPE)
            .apply(CommonValidators::required)
            .apply(CommonValidators::nonZeroEnum, BasicType.class)
            .pop();

        var isArray = typeDescriptor.getBasicType() == BasicType.ARRAY;
        var isMap = typeDescriptor.getBasicType() == BasicType.MAP;

        ctx = ctx.push(TD_ARRAY_TYPE)
                .apply(CommonValidators.ifAndOnlyIf(isArray))
                .apply(TypeSystemValidator::typeDescriptor, TypeDescriptor.class)
                .pop();

        ctx = ctx.push(TD_MAP_TYPE)
                .apply(CommonValidators.ifAndOnlyIf(isMap))
                .apply(TypeSystemValidator::typeDescriptor, TypeDescriptor.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext value(Value value, ValidationContext ctx) {

        if (!value.hasType()) {

            if (!value.hasOneof(V_VALUE))
                return ctx.error(String.format("Type cannot be inferred for null value [%s]", ctx.fieldName()));

            if (!TypeSystem.isPrimitive(value))
                return ctx.error(String.format("Type cannot be inferred for non-primitive value [%s]", ctx.fieldName()));
        }

        var expectedType = TypeSystem.descriptor(value);

        return ctx.apply(TypeSystemValidator::innerValue, Value.class, expectedType);
    }

    @Validator
    public static ValidationContext decimalValue(DecimalValue msg, ValidationContext ctx) {

        return ctx.push(DCV_DECIMAL)
                .apply(CommonValidators::required)
                .apply(CommonValidators::decimal)
                .pop();
    }

    @Validator
    public static ValidationContext dateValue(DateValue msg, ValidationContext ctx) {

        return ctx.push(DV_ISO_DATE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::isoDate)
                .pop();
    }

    @Validator
    public static ValidationContext datetimeValue(DatetimeValue msg, ValidationContext ctx) {

        return ctx.push(DTV_ISO_DATETIME)
                .apply(CommonValidators::required)
                .apply(CommonValidators::isoDatetime)
                .pop();
    }

    public static ValidationContext valueWithType(Value value, TypeDescriptor expectedType, ValidationContext ctx) {

        if (!value.hasType()) {

            if (!value.hasOneof(V_VALUE))
                return ctx.error(String.format("Type cannot be inferred for null value [%s]", ctx.fieldName()));

            if (!TypeSystem.isPrimitive(value))
                return ctx.error(String.format("Type cannot be inferred for non-primitive value [%s]", ctx.fieldName()));
        }

        return ctx.apply(TypeSystemValidator::innerValue, Value.class, expectedType);
    }

    private static ValidationContext innerValue(Value value, TypeDescriptor expectedType, ValidationContext ctx) {

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
                    .applyIf(valueType == BasicType.DECIMAL, TypeSystemValidator::decimalValue, DecimalValue.class)
                    .applyIf(valueType == BasicType.DATE, TypeSystemValidator::dateValue, DateValue.class)
                    .applyIf(valueType == BasicType.DATETIME, TypeSystemValidator::datetimeValue, DatetimeValue.class)
                    .pop();

            // If the value is non-null, make sure the value matches its type descriptor
            if (value.hasOneof(V_VALUE) && valueType != expectedType.getBasicType())
                ctx = ctx.error(wrongTypeMessage);

            return ctx;
        }

        else if (expectedType.getBasicType() == BasicType.ARRAY) {

            var arrayType = expectedType.getArrayType();

            return ctx.push(V_ARRAY)
                    .apply(CommonValidators::required)
                    .apply(TypeSystemValidator::arrayValue, ArrayValue.class, arrayType)
                    .pop();
        }

        else if (expectedType.getBasicType() == BasicType.MAP) {

            var mapType = expectedType.getMapType();

            return ctx.push(V_MAP)
                    .apply(CommonValidators::required)
                    .apply(TypeSystemValidator::mapValue, MapValue.class, mapType)
                    .pop();
        }

        else {

            var unknownTypeError = String.format("Unknown type for [%s]", ctx.fieldName());
            ctx = ctx.error(unknownTypeError);
        }

        return ctx;
    }

    // Array and map values do not have public validators
    // This is because the array type needs to be known
    // So, an array value must always be wrapped in a value

    public static ValidationContext arrayValue(ArrayValue msg, TypeDescriptor arrayType, ValidationContext ctx) {

        return ctx.pushRepeated(AV_ITEMS)
                .applyRepeated(TypeSystemValidator::innerValue, Value.class, arrayType)
                .pop();
    }

    private static ValidationContext mapValue(MapValue msg, TypeDescriptor mapType, ValidationContext ctx) {

        return ctx.pushMap(MV_ENTRIES)
                .applyMapKeys(TypeSystemValidator::mapKey)
                .applyMapValues(TypeSystemValidator::innerValue, Value.class, mapType)
                .pop();
    }

    private static ValidationContext mapKey(String key, ValidationContext ctx) {

        if (key == null || key.isEmpty())
            ctx.error("Map keys cannot be null or empty");

        return ctx;
    }
}
