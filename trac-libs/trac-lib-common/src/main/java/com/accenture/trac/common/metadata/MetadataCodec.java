/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.common.metadata;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


public class MetadataCodec {

    public static com.accenture.trac.common.metadata.UUID encode(java.util.UUID uuid) {

        return com.accenture.trac.common.metadata.UUID.newBuilder()
                .setHi(uuid.getMostSignificantBits())
                .setLo(uuid.getLeastSignificantBits())
                .build();
    }

    public static java.util.UUID decode(com.accenture.trac.common.metadata.UUID uuid) {

        return new UUID(uuid.getHi(), uuid.getLo());
    }

    public static Value encodeNativeObject(Object value) {

        // We need to handle int/long and float/double separately
        // So we need to know the native class, the associated BasicType is not enough

        Class<?> clazz = value.getClass();

        return encodeNativeObject(value, clazz);
    }

    public static Value encodeNativeObject(Object value, Class<?> clazz) {

        if (Boolean.class.equals(clazz))
            return encodeValue((boolean) value);

        if (Long.class.equals(clazz))
            return encodeValue((long) value);

        if (Integer.class.equals(clazz))
            return encodeValue((int) value);

        if (Double.class.equals(clazz))
            return encodeValue((double) value);

        if (Float.class.equals(clazz))
            return encodeValue((float) value);

        if (String.class.equals(clazz))
            return encodeValue((String) value);

        if (BigDecimal.class.equals(clazz))
            return encodeValue((BigDecimal) value);

        if (LocalDate.class.equals(clazz))
            return encodeValue((LocalDate) value);

        if (OffsetDateTime.class.equals(clazz))
            return encodeValue((OffsetDateTime) value);

        throw new RuntimeException("Unsupported object type");  // TODO: error message
    }

    public static Value encodeValue(Object value, TypeDescriptor typeDescriptor) {

        if (TypeSystem.isPrimitive(typeDescriptor))
            return encodeValue(value, typeDescriptor.getBasicType());

        if (typeDescriptor.getBasicType() == BasicType.ARRAY && value instanceof List)
            return encodeArrayValue((List<?>) value, typeDescriptor.getArrayType());

        throw new RuntimeException("");  // TODO
    }

    public static Value encodeValue(Object value, BasicType basicType) {

        var typeCheck = TypeSystem.basicType(value.getClass());

        if (typeCheck != basicType)
            throw new RuntimeException(""); // TODO

        switch (basicType) {

            case BOOLEAN:
                return encodeValue((boolean) value);

            case INTEGER:
                if (value instanceof Integer)
                    return encodeValue((int) value);
                else
                    return encodeValue((long) value);

            case FLOAT:
                if (value instanceof Float)
                    return encodeValue((float) value);
                else
                    return encodeValue((double) value);

            case STRING:
                return encodeValue((String) value);

            case DECIMAL:
                return encodeValue((BigDecimal) value);

            case DATE:
                return encodeValue((LocalDate) value);

            case DATETIME:
                return encodeValue((OffsetDateTime) value);

            default:
                throw new RuntimeException("");  // TODO
        }
    }

    public static Value encodeValue(boolean booleanValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.BOOLEAN))
                .setBooleanValue(booleanValue)
                .build();
    }

    public static Value encodeValue(int integerValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.INTEGER))
                .setIntegerValue(integerValue)
                .build();
    }

    public static Value encodeValue(long integerValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.INTEGER))
                .setIntegerValue(integerValue)
                .build();
    }

    public static Value encodeValue(float floatValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.FLOAT))
                .setFloatValue(floatValue)
                .build();
    }

    public static Value encodeValue(double floatValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.FLOAT))
                .setFloatValue(floatValue)
                .build();
    }

    public static Value encodeValue(BigDecimal decimalValue) {

        var plainString = decimalValue.toPlainString();

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DECIMAL))
                .setDecimalValue(DecimalValue.newBuilder()
                .setStr(plainString))
                .build();
    }

    public static Value encodeValue(String stringValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.STRING))
                .setStringValue(stringValue)
                .build();
    }

    public static Value encodeValue(LocalDate dateValue) {

        var iso = ISO_DATE_FORMAT.format(dateValue);

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATE))
                .setDateValue(DateValue.newBuilder()
                .setIsoDate(iso))
                .build();
    }

    public static Value encodeValue(OffsetDateTime dateTimeValue) {

        var iso = ISO_DATE_TIME_FORMAT.format(dateTimeValue);

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATETIME))
                .setDatetimeValue(DateTimeValue.newBuilder()
                .setIsoDateTime(iso))
                .build();
    }

    public static <T> Value encodeArrayValue(List<T> arrayValue, TypeDescriptor arrayType) {

        var encodedArray = ArrayValue.newBuilder()
                .addAllItem(arrayValue.stream()
                .map(x -> encodeValue(x, arrayType))
                .collect(Collectors.toList()));

        var typeDescriptor = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(arrayType);

        return Value.newBuilder()
                .setType(typeDescriptor)
                .setArrayValue(encodedArray)
                .build();
    }

    public static <T> Value encodeArrayValue(List<T> arrayValue, Class<T> arrayClass) {

        var arrayType = TypeSystem.descriptor(arrayClass);

        return encodeArrayValue(arrayValue, arrayType);
    }

    public static Object decodeValue(Value value) {

        switch(value.getType().getBasicType()) {

            case BOOLEAN:
                return value.getBooleanValue();

            case INTEGER:
                return value.getIntegerValue();

            case FLOAT:
                return value.getFloatValue();

            case STRING:
                return value.getStringValue();

            case DECIMAL:
                return decodeDecimalValue(value);

            case DATE:
                return decodeDateValue(value);

            case DATETIME:
                return decodeDateTimeValue(value);

            default:
                throw new RuntimeException("Type not supported");  // TODO: Error
        }
    }

    public static boolean decodeBooleanValue(Value value) {

        if (value.getType().getBasicType() != BasicType.BOOLEAN)
            throw new RuntimeException("");  // TODO: Error

        return value.getBooleanValue();
    }

    public static long decodeIntegerValue(Value value) {

        if (value.getType().getBasicType() != BasicType.INTEGER)
            throw new RuntimeException("");  // TODO: Error

        return value.getIntegerValue();
    }

    public static double decodeFloatValue(Value value) {

        if (value.getType().getBasicType() != BasicType.FLOAT)
            throw new RuntimeException("");  // TODO: Error

        return value.getFloatValue();
    }

    public static BigDecimal decodeDecimalValue(Value value) {

        if (value.getType().getBasicType() != BasicType.DECIMAL)
            throw new RuntimeException("");  // TODO: Error

        return new BigDecimal(value.getDecimalValue().getStr());
    }

    public static String decodeStringValue(Value value) {

        if (value.getType().getBasicType() != BasicType.STRING)
            throw new RuntimeException("");  // TODO: Error

        return value.getStringValue();
    }

    public static LocalDate decodeDateValue(Value value) {

        if (value.getType().getBasicType() != BasicType.DATE)
            throw new RuntimeException("");  // TODO: Error

        return LocalDate.parse(value.getDateValue().getIsoDate(), ISO_DATE_FORMAT);
    }

    public static OffsetDateTime decodeDateTimeValue(Value value) {

        if (value.getType().getBasicType() != BasicType.DATETIME)
            throw new RuntimeException("");  // TODO: Error

        return OffsetDateTime.parse(value.getDatetimeValue().getIsoDateTime(), ISO_DATE_TIME_FORMAT);
    }

    private static final DateTimeFormatter ISO_DATE_FORMAT = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter ISO_DATE_TIME_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
}
