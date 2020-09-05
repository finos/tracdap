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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
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

    private static final Map<BasicType, Class<?>> NATIVE_TYPE_MAPPING = Map.ofEntries(
            Map.entry(BasicType.BOOLEAN, Boolean.class),
            Map.entry(BasicType.INTEGER, Long.class),
            Map.entry(BasicType.FLOAT, Double.class),
            Map.entry(BasicType.STRING, String.class),
            Map.entry(BasicType.DECIMAL, BigDecimal.class),
            Map.entry(BasicType.DATE, LocalDate.class),
            Map.entry(BasicType.DATETIME, OffsetDateTime.class));

    public static BasicType basicType(Class<?> clazz) {

        var basicType = BASIC_TYPE_MAPPING.getOrDefault(clazz, BasicType.UNRECOGNIZED);

        if (!PRIMITIVE_TYPES.contains(basicType))
            ;  // TODO: Throw

        return basicType;
    }

    public static TypeDescriptor descriptor(Class<?> clazz) {

        var basicType = BASIC_TYPE_MAPPING.getOrDefault(clazz, BasicType.UNRECOGNIZED);

        if (!PRIMITIVE_TYPES.contains(basicType))
            ;  // TODO: Throw

        return TypeDescriptor.newBuilder()
                .setBasicType(basicType)
                .build();
    }

    public static TypeDescriptor descriptor(BasicType basicType) {

        if (!PRIMITIVE_TYPES.contains(basicType))
            ; // TODO: throw

        return TypeDescriptor.newBuilder()
                .setBasicType(basicType)
                .build();
    }

    public static Class<?> nativeClass(BasicType basicType) {

        if (!PRIMITIVE_TYPES.contains(basicType))
            ; // TODO: throw

        return NATIVE_TYPE_MAPPING.get(basicType);
    }

    public static boolean isPrimitive(TypeDescriptor typeDescriptor) {

        return PRIMITIVE_TYPES.contains(typeDescriptor.getBasicType());
    }

    public static boolean isPrimitive(BasicType basicType) {

        return PRIMITIVE_TYPES.contains(basicType);
    }
}
