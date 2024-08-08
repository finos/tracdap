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

import org.finos.tracdap.common.validation.core.impl.ValidationContextImpl;
import org.finos.tracdap.common.validation.core.impl.ValidationFailure;
import org.finos.tracdap.common.validation.core.impl.ValidationKey;
import org.finos.tracdap.common.metadata.MetadataBundle;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.*;
import java.util.function.Function;


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

    static ValidationContext forConsistency(Message msg, MetadataBundle resources) {

        return ValidationContextImpl.forConsistency(msg, resources);
    }

    /**
     * Get the metadata bundle associated with this validation context
     *
     * @return The metadata bundle associated with this validation context
     */
    MetadataBundle getMetadataBundle();

    /**
     * Push a member field of the current object onto the validation stack
     *
     * @param field The field that will be pushed onto the stack
     * @return A validation context pointing at the field that has been pushed
     */
    ValidationContext push(Descriptors.FieldDescriptor field);

    /**
     * Push a "one-of" field of the current object onto the validation stack
     *
     * @param oneOfField The one-of field that will be pushed onto the stack
     * @return A validation context pointing at the field that has been pushed
     */
    ValidationContext pushOneOf(Descriptors.OneofDescriptor oneOfField);

    /**
     * Push a repeated member field of the current object onto the validation stack
     *
     * This method pushes a list onto the validation stack, so the target becomes a Java List object.
     * To push individual list items, use pushRepeatedItem() after calling this method.
     *
     * @param repeatedField The field that will be pushed onto the stack
     * @return A validation context pointing at the list that has been pushed
     */
    ValidationContext pushRepeated(Descriptors.FieldDescriptor repeatedField);

    /**
     * Push an individual list item onto the validation stack
     *
     * Requires that the current target is a repeated field.
     *
     * @param index The index of the list item to push onto the stack (must be within bounds of the list)
     * @return A validation context pointing at the list item that has been pushed
     */
    ValidationContext pushRepeatedItem(int index);

    /**
     * Push an individual list item onto the validation stack
     *
     * Requires that the current target is a repeated field.
     *
     * This overload allows a prior version of the object to be specified for version comparison.
     * This is helpful for version validators, which need to match current / prior entries in a list.
     *
     * @param index The index of the list item to push onto the stack (must be within bounds of the list)
     * @param priorObject The prior object to be compared against this list item in version validators
     * @return A validation context pointing at the list item that has been pushed
     */
    ValidationContext pushRepeatedItem(int index, Object priorObject);

    /**
     * Push an individual list item onto the validation stack
     *
     * Requires that the current target is a repeated field.
     *
     * This overload allows a prior version of the object to be specified for version comparison.
     * This is helpful for version validators, which need to match current / prior entries in a list.
     *
     * @param obj The current list item to push onto the stack (must be a member of the list)
     * @param priorObject The prior object to be compared against this list item in version validators
     * @return A validation context pointing at the list item that has been pushed
     */
    ValidationContext pushRepeatedItem(Object obj, Object priorObject);

    /**
     * Push a map member field of the current object onto the validation stack
     *
     * This method only pushes the map itself onto the stack, the validation target will be a Java Map object.
     * To push individual keys or values, use pushMapKey() or pushMapValue() after calling this method.
     *
     * This method needs a method reference from the parent message class to return Java map object
     * associated with this field (protobuf for Java does not yet provide a generic way of looking up map keys).
     *
     * @param mapField The field that will be pushed onto the stack
     * @param getMapFunc Method reference, on the parent message to return the field as a Java map
     * @return A validation context pointing at the map that has been pushed (this is a Map object)
     */
    <TMsg extends Message> ValidationContext
    pushMap(Descriptors.FieldDescriptor mapField, Function<TMsg, Map<?, ?>> getMapFunc);

    /**
     * Push a map member field of the current object onto the validation stack
     *
     * This method will work with applyMapKeys() and applyMapValues(). If you need to look up map items
     * by key or want to call pushMapKey() / pushMapValue() explicitly, use the overload which takes
     * getMapFunc as a parameter (protobuf for Java does not provide a generic way of looking up map keys).
     *
     * @param mapField The field that will be pushed onto the stack
     * @return A validation context pointing at the map that has been pushed (this is not a Map object)
     */
    ValidationContext pushMap(Descriptors.FieldDescriptor mapField);

    /**
     * Push an individual map key onto the validation stack
     *
     * Requires that the current target is a map field.
     *
     * @param key The map key to push onto the stack
     * @return A validation context pointing at the map key that has been pushed
     */
    ValidationContext pushMapKey(Object key);

    /**
     * Push an individual map value onto the validation stack
     *
     * Requires that the current target is a map field.
     *
     * @param key The key of the map value to push onto the stack
     * @return A validation context pointing at the map value that has been pushed
     */
    ValidationContext pushMapValue(Object key);

    /**
     * Pop the current object from the validation stack
     *
     * @return A validation context pointing at the parent of the current object
     */
    ValidationContext pop();

    /**
     * Record an error against the current location in the validation stack
     *
     * @param message The error message to record
     * @return A validation context which includes the recorded error
     */
    ValidationContext error(String message);

    /**
     * Skip the current location in the validation stack
     *
     * Any future calls to apply() at this location or child locations will be ignored.
     * Errors already recorded at this location or child locations are still included in the validation report.
     *
     * @return A validation context with the current object marked as skipped
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
    ValidationContext applyIf(boolean condition, ValidationFunction.Typed<String> validator);
    <T> ValidationContext applyIf(boolean condition, ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyIf(boolean condition, ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);
    <T> ValidationContext applyIf(boolean condition, ValidationFunction.Version<T> validator, Class<T> targetClass);

    ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Basic validator);
    <T> ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);
    <T> ValidationContext applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Version<T> validator, Class<T> targetClass);

    ValidationContext applyRepeated(ValidationFunction.Basic validator);
    ValidationContext applyRepeated(ValidationFunction.Typed<String> validator);
    <T> ValidationContext applyRepeated(ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyRepeated(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);

    ValidationContext applyMapKeys(ValidationFunction.Basic validator);
    ValidationContext applyMapKeys(ValidationFunction.Typed<String> validator);
    <U> ValidationContext applyMapKeys(ValidationFunction.TypedArg<String, U> validator, U arg);
    <T> ValidationContext applyMapValues(ValidationFunction.Basic validator);
    <T> ValidationContext applyMapValues(ValidationFunction.Typed<T> validator, Class<T> targetClass);
    <T, U> ValidationContext applyMapValues(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg);
    <T, U> ValidationContext applyMapValuesFunc(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, Function<String, U> argFunc);


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

    ValidationContext prior();

    boolean failed();
    boolean skipped();
    boolean done();
    List<ValidationFailure> getErrors();
}
