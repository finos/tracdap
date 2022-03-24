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
import com.google.protobuf.Message;
import org.finos.tracdap.common.validation.core.impl.ValidationContextImpl;
import org.finos.tracdap.common.validation.core.impl.ValidationFailure;
import org.finos.tracdap.common.validation.core.impl.ValidationKey;

import java.util.*;


public interface ValidationContext {

    static ValidationContext forMethod(Message msg, Descriptors.MethodDescriptor descriptor) {

        return ValidationContextImpl.forMethod(msg, descriptor);
    }

    static ValidationContext forMessage(Message msg) {

        return ValidationContextImpl.forMessage(msg);
    }

    static ValidationContext forVersion(Message current, Message prior) {

        return ValidationContextImpl.forVersion(current, prior);
    }

    ValidationContext push(Descriptors.FieldDescriptor fd);
    ValidationContext pushOneOf(Descriptors.OneofDescriptor oneOf);
    ValidationContext pop();

    ValidationContext error(String message);
    ValidationContext skip();

    ValidationContext apply(ValidationFunction.Basic validator);
    ValidationContext apply(ValidationFunction.Typed<String> validator);
    <T> ValidationContext apply(ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyWith(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);

    ValidationContext apply(ValidationFunction.Version<Object> validator);
    <T> ValidationContext apply(ValidationFunction.Version<T> validator, Class<T> targetClass);

    ValidationContext applyIf(ValidationFunction.Basic validator, boolean condition);
    <T> ValidationContext applyIf(ValidationFunction.Typed<T> validator, Class<T> targetClass, boolean condition);
    <T> ValidationContext applyIf(ValidationFunction.Version<T> validator, Class<T> targetClass, boolean condition);

    <TMsg extends Message> ValidationContext applyRepeated(ValidationFunction.Typed<TMsg> validator, Class<TMsg> msgClass);
    <TMsg extends Message, U> ValidationContext applyRepeatedWith(ValidationFunction.TypedArg<TMsg, U> validator, Class<TMsg> msgClass, U arg);

    ValidationType validationType();
    ValidationKey key();
    Object target();
    Message parentMsg();
    boolean isOneOf();
    Descriptors.OneofDescriptor oneOf();
    Descriptors.FieldDescriptor field();
    String fieldName();
    Descriptors.FieldDescriptor priorField();
    String priorFieldName();

    boolean failed();
    boolean skipped();
    boolean done();
    List<ValidationFailure> getErrors();
}
