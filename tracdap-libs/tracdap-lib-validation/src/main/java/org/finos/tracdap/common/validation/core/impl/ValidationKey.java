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

package org.finos.tracdap.common.validation.core.impl;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.common.validation.core.ValidationType;


public class ValidationKey implements Comparable<ValidationKey> {

    private static final String OPAQUE_KEY_TEMPLATE = "%s:%s:%s";
    private static final String DISPLAY_NAME_TEMPLATE = "%s %s";
    private static final String METHOD_SHORT_NAME_TEMPLATE = "%s()";
    private static final String METHOD_DISPLAY_NAME_TEMPLATE = "%s, %s %s";

    private final ValidationType validationType;
    private final Descriptors.Descriptor messageType;
    private final Descriptors.MethodDescriptor method;
    private final String opaqueKey;
    private final String shortName;
    private final String displayName;

    public static ValidationKey forObject(Descriptors.Descriptor messageType) {

        return new ValidationKey(ValidationType.STATIC, messageType, null);
    }

    public static ValidationKey forMethod(
            Descriptors.Descriptor messageType,
            Descriptors.MethodDescriptor method) {

        return new ValidationKey(ValidationType.STATIC, messageType, method);
    }

    public static ValidationKey forVersion(Descriptors.Descriptor messageType) {

        return new ValidationKey(ValidationType.VERSION, messageType, null);
    }

    public ValidationKey(
            ValidationType validationType,
            Descriptors.Descriptor messageType,
            Descriptors.MethodDescriptor method) {

        this.validationType = validationType;
        this.messageType = messageType;
        this.method = method;

        var contextualPart = method != null
                ? method.getFullName()
                : "";

        this.opaqueKey = String.format(OPAQUE_KEY_TEMPLATE,
                validationType.name(),
                messageType.getFullName(),
                contextualPart);

        if (method != null) {
            this.shortName = String.format(METHOD_SHORT_NAME_TEMPLATE, method.getName());
            this.displayName = String.format(METHOD_DISPLAY_NAME_TEMPLATE,
                    method.getName(),
                    messageType.getName(),
                    validationType.name());
        }
        else {
            this.shortName = messageType.getName();
            this.displayName = String.format(DISPLAY_NAME_TEMPLATE,
                    messageType.getName(),
                    validationType.name());
        }
    }

    public ValidationKey(
            ValidationType validationType,
            Descriptors.Descriptor messageType) {

        this(validationType, messageType, null);
    }

    @Override
    public int compareTo(ValidationKey other) {
        return opaqueKey.compareTo(other.opaqueKey);
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof ValidationKey))
            return false;

        var other = (ValidationKey) obj;

        return opaqueKey.equals(other.opaqueKey);
    }

    @Override
    public int hashCode() {
        return opaqueKey.hashCode();
    }

    public ValidationType validationType() {
        return validationType;
    }

    public String shortName() {
        return shortName;
    }

    public String displayName() {
        return displayName;
    }
}
