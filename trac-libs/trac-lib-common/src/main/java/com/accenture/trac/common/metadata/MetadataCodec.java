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
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.stream.Collectors;


public class MetadataCodec {

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

        var message = "Cannot encode value with Java class [%s]";
        throw new IllegalArgumentException(String.format(message, clazz.getName()));
    }

    public static Value encodeValue(Object value, TypeDescriptor descriptor) {

        var basicType = TypeSystem.basicType(descriptor);

        if (TypeSystem.isPrimitive(basicType))
            return encodeValue(value, basicType);

        if (basicType == BasicType.ARRAY) {

            if (value instanceof List)
                return encodeArrayValue((List<?>) value, descriptor.getArrayType());
            else
                throw new IllegalArgumentException("Object does not match type");
        }

        var message = "Cannot encode value with type [%s]";
        throw new IllegalArgumentException(String.format(message, basicType.name()));
    }

    public static Value encodeValue(Object value, BasicType basicType) {

        var typeCheck = TypeSystem.basicType(value);

        if (typeCheck != basicType)
            throw new IllegalArgumentException("Object does not match type");

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
                var message = "Cannot encode value with type [%s]";
                throw new IllegalArgumentException(String.format(message, basicType.name()));
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

    public static Value encodeValue(String stringValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.STRING))
                .setStringValue(stringValue)
                .build();
    }

    public static Value encodeValue(BigDecimal decimalValue) {

        var decimalString = decimalValue.toPlainString();

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DECIMAL))
                .setDecimalValue(DecimalValue.newBuilder()
                .setDecimal(decimalString))
                .build();
    }

    public static Value encodeValue(LocalDate dateValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATE))
                .setDateValue(encodeDate(dateValue))
                .build();
    }

    public static Value encodeValue(OffsetDateTime datetimeValue) {

        return Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.DATETIME))
                .setDatetimeValue(encodeDatetime(datetimeValue))
                .build();
    }

    public static DateValue encodeDate(LocalDate date) {

        var isoDate = ISO_DATE_FORMAT.format(date);

        return DateValue.newBuilder()
                .setIsoDate(isoDate)
                .build();
    }

    public static DatetimeValue encodeDatetime(OffsetDateTime datetime) {

        var isoDatetime = ISO_DATETIME_FORMAT.format(datetime);

        return DatetimeValue.newBuilder()
                .setIsoDatetime(isoDatetime)
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

        var basicType = TypeSystem.basicType(value);

        switch(basicType) {

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
                var message = "Cannot decode value of type [%s]";
                throw new IllegalArgumentException(String.format(message, basicType.name()));
        }
    }

    public static boolean decodeBooleanValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.BOOLEAN)
            throw new IllegalArgumentException("Value is not a boolean");

        return value.getBooleanValue();
    }

    public static long decodeIntegerValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.INTEGER)
            throw new IllegalArgumentException("Value is not an integer");

        return value.getIntegerValue();
    }

    public static double decodeFloatValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.FLOAT)
            throw new IllegalArgumentException("Value is not a float");

        return value.getFloatValue();
    }

    public static String decodeStringValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.STRING)
            throw new IllegalArgumentException("Value is not a string");

        return value.getStringValue();
    }

    public static BigDecimal decodeDecimalValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.DECIMAL)
            throw new IllegalArgumentException("Value is not a decimal");

        return new BigDecimal(value.getDecimalValue().getDecimal());
    }

    public static LocalDate decodeDateValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.DATE)
            throw new IllegalArgumentException("Value is not a date");

        return decodeDate(value.getDateValue());
    }

    public static OffsetDateTime decodeDateTimeValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.DATETIME)
            throw new IllegalArgumentException("Value is not a date-time");

        return decodeDatetime(value.getDatetimeValue());
    }

    public static List<?> decodeArrayValue(Value value) {

        if (TypeSystem.basicType(value) != BasicType.ARRAY)
            throw new IllegalArgumentException("Value is not an array");

        return value.getArrayValue().getItemList().stream()
                .map(MetadataCodec::decodeValue)
                .collect(Collectors.toList());
    }

    public static LocalDate decodeDate(DateValue date) {

        return LocalDate.from(ISO_DATE_FORMAT.parse(date.getIsoDate()));
    }

    public static OffsetDateTime decodeDatetime(DatetimeValue datetime) {

        return OffsetDateTime.from(ISO_DATETIME_FORMAT.parse(datetime.getIsoDatetime()));
    }

    private static final DateTimeFormatter ISO_DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    // Using DateTimeFormatter.ISO_OFFSET_DATE_TIME results in > 6 decimal points.
    // TRAC metadata tags are stored with timestamps at precision 6
    // To avoid discrepancies, define a formatter that always uses 6 d.p.

    private static final DateTimeFormatter ISO_DATETIME_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd'T'kk:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 6, 6, true)
            .appendOffsetId()
            .toFormatter();

    // private static final DateTimeFormatter ISO_DATETIME_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
}
