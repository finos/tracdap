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

    /**
     * Push a member field of the current object onto the validation stack
     *
     * @param field The field that will be pushed onto the stack
     * @return A new validation context, pointing at the field that has been pushed onto the stack
     */
    ValidationContext push(Descriptors.FieldDescriptor field);

    /**
     * Push a repeated member field of the current object onto the validation stack
     *
     * @param repeatedField The field that will be pushed onto the stack
     * @return A new validation context, pointing at the field that has been pushed onto the stack
     */
    ValidationContext pushRepeated(Descriptors.FieldDescriptor repeatedField);

    /**
     * Push a map member field of the current object onto the validation stack
     *
     * @param mapField The field that will be pushed onto the stack
     * @return A new validation context, pointing at the field that has been pushed onto the stack
     */
    ValidationContext pushMap(Descriptors.FieldDescriptor mapField);

    /**
     * Push a "one-of" field of the current object onto the validation stack
     *
     * @param oneOfField The one-of field that will be pushed onto the stack
     * @return A new validation context, pointing at the field that has been pushed onto the stack
     */
    ValidationContext pushOneOf(Descriptors.OneofDescriptor oneOfField);

    /**
     * Pop the current object from the validation stack
     *
     * @return A new validation context, pointing at the parent of the current object
     */
    ValidationContext pop();

    /**
     * Record an error against the current location in the validation stack
     *
     * @param message The error message to record
     * @return A new validation context, which includes the recorded error
     */
    ValidationContext error(String message);

    /**
     * Skip the current location in the validation stack
     *
     * Any future calls to apply() at this location or child locations will be ignored.
     * Errors already recorded at this location or child locations are still included in the validation report.
     *
     * @return A new context with the current object in the validation stack marked as skipped.
     */
    ValidationContext skip();

    ValidationContext applyRegistered();

    ValidationContext apply(ValidationFunction.Basic validator);
    ValidationContext apply(ValidationFunction.Typed<String> validator);
    <T> ValidationContext apply(ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext apply(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);
    ValidationContext apply(ValidationFunction.Version<Object> validator);
    <T> ValidationContext apply(ValidationFunction.Version<T> validator, Class<T> targetClass);

    ValidationContext applyIf(boolean condition, ValidationFunction.Basic validator);
    <T> ValidationContext applyIf(boolean condition, ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyIf(boolean condition, ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);
    <T> ValidationContext applyIf(boolean condition, ValidationFunction.Version<T> validator, Class<T> targetClass);

    ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Basic validator);
    <T> ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);
    <T> ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Version<T> validator, Class<T> targetClass);

    <T> ValidationContext applyRepeated(ValidationFunction.Basic validator);
    <T> ValidationContext applyRepeated(ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyRepeated(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);

    ValidationContext applyMapKeys(ValidationFunction.Basic validator);
    ValidationContext applyMapKeys(ValidationFunction.Typed<String> validator);
    <U> ValidationContext applyMapKeys(ValidationFunction.TypedArg<String, U> validator, U arg);
    <T> ValidationContext applyMapValues(ValidationFunction.Basic validator);
    <T> ValidationContext applyMapValues(ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyMapValues(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);


    ValidationType validationType();
    ValidationKey key();
    Object target();
    Message parentMsg();
    boolean isOneOf();
    boolean isRepeated();
    boolean isMap();
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
