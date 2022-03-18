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

    private static final Descriptors.Descriptor DATE_VALUE;
    private static final Descriptors.FieldDescriptor DV_ISO_DATE;

    private static final Descriptors.Descriptor DATETIME_VALUE;
    private static final Descriptors.FieldDescriptor DTV_ISO_DATETIME;


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
        V_VALUE = V_BOOLEAN.getContainingOneof();

        DATE_VALUE = DateValue.getDescriptor();
        DV_ISO_DATE = field(DATE_VALUE, DateValue.ISODATE_FIELD_NUMBER);

        DATETIME_VALUE = DatetimeValue.getDescriptor();
        DTV_ISO_DATETIME = field(DATETIME_VALUE, DatetimeValue.ISODATETIME_FIELD_NUMBER);
    }

    static Descriptors.FieldDescriptor field(Descriptors.Descriptor msg, int fieldNo) {
        return msg.findFieldByNumber(fieldNo);
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

    public static ValidationContext value(Value value, ValidationContext ctx) {

        if (!value.hasOneof(V_VALUE)) {

            return ctx.push(V_TYPE)
                    .apply(CommonValidators::required)
                    .apply(TypeSystemValidator::typeDescriptor, TypeDescriptor.class)
                    .apply(TypeSystemValidator::primitive, TypeDescriptor.class)
                    .pop();
        }

        ctx = ctx.push(V_TYPE)
                .apply(CommonValidators::optional)
                .apply(TypeSystemValidator::typeDescriptor, TypeDescriptor.class)
                .pop();

        // ctx = ctx.pushOneOf(V_VALUE)

        return ctx;
    }



    public static ValidationContext dateValue(DatetimeValue msg, ValidationContext ctx) {

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


}
