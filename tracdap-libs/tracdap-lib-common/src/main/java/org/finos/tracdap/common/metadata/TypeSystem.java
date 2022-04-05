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

package org.finos.tracdap.common.metadata;

import org.finos.tracdap.metadata.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;


public class TypeSystem {

    private static final Set<BasicType> PRIMITIVE_TYPES = Set.of(
            BasicType.BOOLEAN,
            BasicType.INTEGER,
            BasicType.FLOAT,
            BasicType.STRING,
            BasicType.DECIMAL,
            BasicType.DATE,
            BasicType.DATETIME);

    private static final Map<Class<?>, BasicType> BASIC_TYPE_MAPPING = Map.ofEntries(
            Map.entry(Boolean.class, BasicType.BOOLEAN),
            Map.entry(Integer.class, BasicType.INTEGER),
            Map.entry(Long.class, BasicType.INTEGER),
            Map.entry(Float.class, BasicType.FLOAT),
            Map.entry(Double.class, BasicType.FLOAT),
            Map.entry(String.class, BasicType.STRING),
            Map.entry(BigDecimal.class, BasicType.DECIMAL),
            Map.entry(LocalDate.class, BasicType.DATE),
            Map.entry(OffsetDateTime.class, BasicType.DATETIME));

    private static final Map<Value.ValueCase, BasicType> VALUE_CASE_MAPPING = Map.ofEntries(
            Map.entry(Value.ValueCase.BOOLEANVALUE, BasicType.BOOLEAN),
            Map.entry(Value.ValueCase.INTEGERVALUE, BasicType.INTEGER),
            Map.entry(Value.ValueCase.FLOATVALUE, BasicType.FLOAT),
            Map.entry(Value.ValueCase.STRINGVALUE, BasicType.STRING),
            Map.entry(Value.ValueCase.DECIMALVALUE, BasicType.DECIMAL),
            Map.entry(Value.ValueCase.DATEVALUE, BasicType.DATE),
            Map.entry(Value.ValueCase.DATETIMEVALUE, BasicType.DATETIME),
            Map.entry(Value.ValueCase.ARRAYVALUE, BasicType.ARRAY),
            Map.entry(Value.ValueCase.MAPVALUE, BasicType.MAP));

    public static BasicType basicType(Class<?> clazz) {

        if (clazz == null)
            throw new NullPointerException("Basic type requested for null class");

        var basicType = BASIC_TYPE_MAPPING.getOrDefault(clazz, BasicType.UNRECOGNIZED);

        if (basicType == BasicType.UNRECOGNIZED)
            throw new IllegalArgumentException("No type mapping available for Java class " + clazz.getName());

        return basicType;
    }

    public static BasicType basicType(Object object) {

        if (object == null)
            throw new NullPointerException("Basic type requested for null object");

        return basicType(object.getClass());
    }

    public static BasicType basicType(TypeDescriptor descriptor) {

        if (descriptor == null)
            throw new NullPointerException("Basic type requested for null type descriptor");

        var basicType = descriptor.getBasicType();

        if (basicType == BasicType.BASIC_TYPE_NOT_SET || basicType == BasicType.UNRECOGNIZED)
            throw new IllegalArgumentException("Invalid type descriptor: Basic type not set");

        return basicType;
    }

    public static BasicType basicType(Value value) {

        if (value == null)
            throw new NullPointerException("Basic type requested for null value");

        if (value.hasType())
            return basicType(value.getType());

        var valueCase = value.getValueCase();
        var basicType = VALUE_CASE_MAPPING.getOrDefault(valueCase, BasicType.UNRECOGNIZED);

        if (basicType == BasicType.UNRECOGNIZED)
            throw new IllegalArgumentException("No type mapping available for value with value case " + valueCase);

        return basicType;
    }

    public static TypeDescriptor descriptor(Class<?> clazz) {

        if (clazz == null)
            throw new NullPointerException("Descriptor requested for null class");

        var basicType = basicType(clazz);

        if (!isPrimitive(basicType)) {
            var message = "Cannot create type descriptor from Java class %s: Sub-type information is not available";
            throw new IllegalArgumentException(String.format(message, clazz.getName()));
        }

        return TypeDescriptor.newBuilder()
                .setBasicType(basicType)
                .build();
    }

    public static TypeDescriptor descriptor(Object object) {

        if (object == null)
            throw new NullPointerException("Descriptor requested for null object");

        return descriptor(object.getClass());
    }

    public static TypeDescriptor descriptor(BasicType basicType) {

        if (basicType == null)
            throw new NullPointerException("Descriptor requested for null type");

        if (basicType == BasicType.BASIC_TYPE_NOT_SET || basicType == BasicType.UNRECOGNIZED) {
            var message = "Descriptor requested for unknown basic type [%s]";
            throw new IllegalArgumentException(String.format(message, basicType.name()));
        }

        if (!isPrimitive(basicType)) {
            var message = "Cannot create type descriptor for basic type [%s]: Sub-type information is not available";
            throw new IllegalArgumentException(String.format(message, basicType.name()));
        }

        return TypeDescriptor.newBuilder()
                .setBasicType(basicType)
                .build();
    }

    public static TypeDescriptor descriptor(Value value) {

        if (value == null)
            throw new NullPointerException("Descriptor requested for null value");

        if (value.hasType())
            return value.getType();

        var basicType = basicType(value);

        if (!isPrimitive(basicType))
            throw new IllegalArgumentException("Non-primitive values must provide a valid type descriptor");

        return descriptor(basicType);
    }

    public static boolean isPrimitive(Value value) {

        if (value == null)
            throw new NullPointerException("Is-primitive check requested for null value");

        return isPrimitive(basicType(value));
    }

    public static boolean isPrimitive(TypeDescriptor descriptor) {

        if (descriptor == null)
            throw new NullPointerException("Is-primitive check requested for null type descriptor");

        return isPrimitive(basicType(descriptor));
    }

    public static boolean isPrimitive(BasicType basicType) {

        if (basicType == null)
            throw new NullPointerException("Is-primitive check requested for null type");

        return PRIMITIVE_TYPES.contains(basicType);
    }

    public static BasicType valueCaseType(Value value) {

        return VALUE_CASE_MAPPING.getOrDefault(value.getValueCase(), BasicType.UNRECOGNIZED);
    }
}
