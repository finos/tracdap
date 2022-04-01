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
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationFunction;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.Function;


public class ValidationContextImpl implements ValidationContext {

    private static final Map<ValidationKey, ValidationFunction<?>> validators = ValidatorBuilder.buildValidatorMap();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ValidationType validationType;
    private final Stack<ValidationLocation> location;
    private final ValidationContextImpl priorCtx;
    private final List<ValidationFailure> failures;


    private ValidationContextImpl(ValidationType validationType, ValidationLocation root, ValidationContextImpl priorCtx) {

        this.validationType = validationType;
        this.location = new Stack<>();
        this.location.push(root);

        this.priorCtx = priorCtx;

        this.failures = new ArrayList<>();
    }

    public static ValidationContext forMethod(Message msg, Descriptors.MethodDescriptor method) {

        var root = new ValidationLocation(null, msg, method, null, null, null);
        return new ValidationContextImpl(ValidationType.STATIC, root, null);
    }

    public static ValidationContext forMessage(Message msg) {

        var root = new ValidationLocation(null, msg, null, null);
        return new ValidationContextImpl(ValidationType.STATIC, root, null);
    }

    public static ValidationContext forVersion(Message current, Message prior) {

        var currentRoot = new ValidationLocation(null, current, null, null);
        var priorRoot = new ValidationLocation(null, prior, null, null);
        var priorCtx = new ValidationContextImpl(ValidationType.VERSION, priorRoot, null);
        return new ValidationContextImpl(ValidationType.VERSION, currentRoot, priorCtx);
    }

    @Override
    public ValidationContext push(Descriptors.FieldDescriptor fd) {

        return push(fd, false, false, null);
    }

    private ValidationContext push(
            Descriptors.FieldDescriptor fd,
            boolean repeated, boolean map,
            Function<Message, Map<?, ?>> getMapFunc) {

        if (fd.isRepeated() != repeated || fd.isMapField() != map)
            throw new ETracInternal("Use push, pushRepeated and pushMap for regular, repeated and map fields respectively");

        var parentLoc = location.peek();
        var msg = (Message) parentLoc.msg();

        var obj = map && getMapFunc != null
            ? getMapFunc.apply(msg)
            : msg.getField(fd);

        var loc = new ValidationLocation(
                parentLoc, obj,
                null, fd, fd.getName());

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);

        if (priorCtx != null)
            priorCtx.push(fd, repeated, map, getMapFunc);

        return this;
    }

    @Override
    public ValidationContext pushOneOf(Descriptors.OneofDescriptor oneOf) {

        var parentLoc = location.peek();
        var msg = parentLoc.msg();

        // If the oneOf field has been set, look up the fd, field name and value for the field in use
        // In this case the location ctx be reported as that field in any error messages

        // If the oneOf is not set, fd and value cannot be set, the name is left as the oneOf name
        // Error messages for required() will refer to the oneOf name
        // Other checks must be guarded with optional(), omitted() or applyIf()

        var fd = msg.hasOneof(oneOf) ? msg.getOneofFieldDescriptor(oneOf) : null;
        var name = fd != null ? fd.getName() : oneOf.getName();
        var obj = fd != null ? msg.getField(fd) : null;
        var loc = new ValidationLocation(parentLoc, obj, oneOf, fd, name);

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);

        if (priorCtx != null)
            priorCtx.pushOneOf(oneOf);

        return this;
    }

    @Override
    public ValidationContext pushRepeated(Descriptors.FieldDescriptor fd) {

        return push(fd, true, false, null);
    }

    @Override
    public ValidationContext pushRepeatedItem(int index) {

        return pushRepeatedItem(index, null, false, null);
    }

    @Override
    public ValidationContext pushRepeatedItem(int index, Object priorObj) {

        return pushRepeatedItem(index, null, false, priorObj);
    }

    @Override
    public ValidationContext pushRepeatedItem(Object obj, Object priorObj) {

        return pushRepeatedItem(-1, obj, false, priorObj);
    }

    private ValidationContext pushRepeatedItem(int index, Object obj, boolean isPrior, Object priorObj) {

        var parentLoc = location.peek();

        if (!parentLoc.field().isRepeated() || parentLoc.field().isMapField())
            throw new ETracInternal("[pushRepeatedItem] is only for repeated fields (and not map fields)");

        var msg = parentLoc.parent().msg();

        if (!isPrior) {

            if (index < 0 && obj != null) {

                var list = (List<?>) msg.getField(parentLoc.field());
                index = list.indexOf(obj);

                if (index < 0)
                    throw new ETracInternal("Object not in list for [pushRepeatedItem]");
            }

            if (index < 0 || index >= msg.getRepeatedFieldCount(parentLoc.field()))
                throw new ETracInternal("Index out of bounds for [pushRepeatedItem]");

            if (obj == null) {
                var list = (List<?>) msg.getField(parentLoc.field());
                obj = list.get(index);
            }
        }

        var fieldName = String.format("%d", index);
        var loc = new ValidationLocation(parentLoc, obj, parentLoc.field(), fieldName);
        location.push(loc);

        if (priorCtx != null)
            priorCtx.pushRepeatedItem(-1, priorObj, true, null);

        return this;
    }

    @Override public ValidationContext
    pushMap(Descriptors.FieldDescriptor fd) {

        if (priorCtx != null)
            throw new ETracInternal("Use [pushMap] with [getMapFunc] for version validation");

        return push(fd, true, true, null);
    }

    @Override public <TMsg extends Message> ValidationContext
    pushMap(Descriptors.FieldDescriptor mapField, Function<TMsg, Map<?, ?>> getMapFunc) {

        @SuppressWarnings("unchecked")
        var getMapFunc_ = (Function<Message, Map<?, ?>>) getMapFunc;

        return push(mapField, true, true, getMapFunc_);
    }

    @Override
    public ValidationContext pushMapKey(Object key) {

        return pushMapEntry(key, true, false);
    }

    @Override
    public ValidationContext pushMapValue(Object key) {

        return pushMapEntry(key, false, false);
    }

    private ValidationContext pushMapEntry(Object key, boolean pushKey, boolean keyAlreadySeen) {

        var methodName = pushKey ? "[pushMapKey]" : "[pushMapValue]";

        var parentLoc = location.peek();

        if (!parentLoc.field().isMapField())
            throw new ETracInternal(methodName + " can only be used on map fields");

        if (!(parentLoc.target() instanceof Map))
            throw new ETracInternal(methodName + " requires [pushMap] is called with [getMapFunc]");

        var map = (Map<?, ?>) parentLoc.target();
        var keyPresent = map.containsKey(key);

        if (!keyPresent) {

            // For version validators, allow pushing keys that exist in either the current or prior map
            // For the current ctx, if the key is not present we need to check the prior ctx
            // For the prior ctx, we allow the key if it was already seen in the current ctx
            if (priorCtx == null && !keyAlreadySeen)
                throw new ETracInternal(methodName + " attempted to push a key that is not in the map");
        }

        var fieldName = key.toString();
        var obj = pushKey ? key : map.getOrDefault(key, null);
        var loc = new ValidationLocation(parentLoc, obj, parentLoc.field(), fieldName);

        location.push(loc);

        if (parentLoc.skipped())
            loc.skip();

        if (priorCtx != null)
            priorCtx.pushMapEntry(key, pushKey, keyPresent);

        return this;
    }

    private ValidationContext pushMapEntry(int index, boolean pushKey) {

        // Push map entry by index is only called from the applyMap* functions
        var methodName = pushKey ? "[applyMapKeys]" : "[applyMapValues]";

        var parentLoc = location.peek();

        if (!parentLoc.field().isMapField())
            throw new ETracInternal(methodName + " can only be used on map fields");

        if (!(parentLoc.target() instanceof List))
            throw new ETracInternal(methodName + " requires [pushMap] is called without [getMapFunc]");

        if (priorCtx != null)
            throw new ETracInternal(methodName + " by index is not allowed for version validators");

        @SuppressWarnings("unchecked")
        var list = (List<MapEntry<?, ?>>) parentLoc.target();

        if (index < 0 || index > list.size())
            throw new ETracInternal(methodName + " map entry index is out of range");

        var entry = list.get(index);
        var fieldName = entry.getKey().toString();
        var obj = pushKey ? entry.getKey() : entry.getValue();
        var loc = new ValidationLocation(parentLoc, obj, parentLoc.field(), fieldName);

        location.push(loc);

        if (parentLoc.skipped())
            loc.skip();

        return this;
    }

    public ValidationContext pop() {

        if (location.empty())
            throw new IllegalStateException();

        // Failures propagate up the stack
        var loc = location.peek();
        if (loc.failed() && loc.parent() != null)
            loc.parent().fail();

        location.pop();

        if (priorCtx != null)
            priorCtx.pop();

        return this;
    }

    public ValidationContext error(String message) {

        var failure = new ValidationFailure(location.peek(), message);
        failures.add(failure);

        var loc = location.peek();
        loc.fail();

        return this;
    }

    public ValidationContext skip() {

        var loc = location.peek();
        loc.skip();

        return this;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public ValidationContext applyRegistered() {

        var loc = location.peek();
        var msg = loc.msg();

        if (msg == null)
            throw new ETracInternal("applyRegistered() can only be applied to message types");

        var key = new ValidationKey(validationType, msg.getDescriptorForType(), loc.method());
        var validator = validators.get(key);

        if (validator == null) {
            var err = String.format("Required validator is not registered: [%s]", key.displayName());
            log.error(err);
            throw new ETracInternal(err);
        }

        if (validator.isBasic())
            return apply(validator.basic());

        if (!validator.targetClass().isInstance(msg))
            throw new EUnexpected();

        if (validator.isTyped()) {

            var typedValidator = (ValidationFunction.Typed) validator.typed();
            return apply(typedValidator, msg.getClass());
        }

        if (validator.isVersion()) {

            var versionValidator = (ValidationFunction.Version) validator.version();
            return apply(versionValidator, msg.getClass());
        }

        throw new EUnexpected();
    }

    public ValidationContext apply(ValidationFunction.Basic validator) {

        return apply((obj, arg, ctx) -> validator.apply(ctx), Object.class, null);
    }

    public ValidationContext apply(ValidationFunction.Typed<String> validator) {

        return apply((obj, arg, ctx) -> validator.apply(obj, ctx), String.class, null);
    }

    public <T>
    ValidationContext apply(ValidationFunction.Typed<T> validator, Class<T> targetClass) {

        return apply((obj, arg, ctx) -> validator.apply(obj, ctx), targetClass, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T, U>
    ValidationContext apply(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg) {

        if (done())
            return this;

        var obj = location.peek().target();

        // Simple case - obj is an instance of the expected target class

        if (obj == null || targetClass.isInstance(obj)) {
            var target = (T) obj;
            return validator.apply(target, arg, this);
        }

        // Protobuf stores enum values as EnumValueDescriptor objects
        // So, special handling is needed for enums

        if (Enum.class.isAssignableFrom(targetClass)) {

            if (obj instanceof Descriptors.EnumValueDescriptor) {

                var valueDesc = (Descriptors.EnumValueDescriptor) obj;
                var enumType = (Class<? extends Enum>) targetClass;
                var enumVal = Enum.valueOf(enumType, valueDesc.getName());

                return validator.apply((T) enumVal, arg, this);
            }
        }

        // If obj does not match the expected target type, blow up the validation process

        // Type mismatch errors should be detected during development, otherwise the validator won't run
        // In the event of a type mismatch at run time, blow up the validator!
        // I.e. report as an unexpected internal error, validation cannot be completed.

        var err = String.format(
                "Validator type mismatch (this is a bug): expected [%s], got [%s]",
                targetClass.getSimpleName(), obj.getClass().getSimpleName());

        log.error(err);
        throw new ETracInternal(err);
    }

    public ValidationContext apply(ValidationFunction.Version<Object> validator) {

        return apply(validator, Object.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> ValidationContext apply(ValidationFunction.Version<T> validator, Class<T> targetClass) {

        if (priorCtx == null)
            throw new ETracInternal("Version validator requires a version validation context (ValidationContext.forVersion())");

        if (done())
            return this;

        var current = location.peek().target();
        var prior = priorCtx.location.peek().target();

        if ((current == null || targetClass.isInstance(current)) &&
            (prior == null || targetClass.isInstance(prior))) {

            var typedCurrent = (T) current;
            var typedPrior = (T) prior;

            return validator.apply(typedCurrent, typedPrior, this);
        }

        if (Enum.class.isAssignableFrom(targetClass)) {

            if (current instanceof Descriptors.EnumValueDescriptor && prior instanceof Descriptors.EnumValueDescriptor) {

                var enumType = (Class<? extends Enum>) targetClass;

                var currentDesc = (Descriptors.EnumValueDescriptor) current;
                var currentVal = Enum.valueOf(enumType, currentDesc.getName());
                var priorDesc = (Descriptors.EnumValueDescriptor) current;
                var priorVal = Enum.valueOf(enumType, priorDesc.getName());

                return validator.apply((T) currentVal, (T) priorVal, this);
            }
        }

        var currentClass = current != null ? current.getClass().getSimpleName() : "(null)";
        var priorClass = prior != null ? prior.getClass().getSimpleName() : "(null)";
        var err = String.format(
                "Validator type mismatch (this is a bug): expected [%s], got prior = [%s], current = [%s]",
                targetClass.getSimpleName(), priorClass, currentClass);

        log.error(err);
        throw new ETracInternal(err);
    }

    public ValidationContext
    applyIf(boolean condition, ValidationFunction.Basic validator) {

        if (!condition)
            return this;

        return apply(validator);
    }

    public <T> ValidationContext
    applyIf(boolean condition, ValidationFunction.Typed<T> validator, Class<T> targetClass) {

        if (!condition)
            return this;

        return apply(validator, targetClass);
    }

    public <T, U> ValidationContext
    applyIf(boolean condition, ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg) {

        if (!condition)
            return this;

        return apply(validator, targetClass, arg);
    }

    public <T> ValidationContext
    applyIf(boolean condition, ValidationFunction.Version<T> validator, Class<T> targetClass) {

        if (!condition)
            return this;

        return apply(validator, targetClass);
    }

    public ValidationContext
    applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Basic validator) {

        var oneOfCondition = oneOfFieldMatch(field);
        return applyIf(oneOfCondition, validator);
    }

    public <T> ValidationContext
    applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Typed<T> validator, Class<T> targetClass) {

        var oneOfCondition = oneOfFieldMatch(field);
        return applyIf(oneOfCondition, validator, targetClass);
    }

    public <T, U> ValidationContext
    applyOneOf(Descriptors.FieldDescriptor field,ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg) {

        var oneOfCondition = oneOfFieldMatch(field);
        return applyIf(oneOfCondition, validator, targetClass, arg);
    }

    public <T> ValidationContext
    applyOneOf(Descriptors.FieldDescriptor field, ValidationFunction.Version<T> validator, Class<T> targetClass) {

        var oneOfCondition = oneOfFieldMatch(field);
        return applyIf(oneOfCondition, validator, targetClass);
    }

    private boolean oneOfFieldMatch(Descriptors.FieldDescriptor oneOfField) {

        var loc = location.peek();

        if (!loc.isOneOf())
            throw new ETracInternal("applyOneOf() can only be applied to one-of fields");

        if (loc.oneOf() != oneOfField.getContainingOneof())
            throw new ETracInternal("applyOneOf() field is not a member of the current one-of");

        return loc.field() == oneOfField;
    }

    public ValidationContext
    applyRepeated(ValidationFunction.Basic validator) {

        return applyRepeated((obj, arg, ctx) -> validator.apply(ctx), Object.class, null);
    }

    public <T> ValidationContext
    applyRepeated(ValidationFunction.Typed<T> validator, Class<T> targetClass) {

        return applyRepeated((obj, arg, ctx) -> validator.apply(obj, ctx), targetClass, null);
    }

    public <T, U> ValidationContext
    applyRepeated(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg) {

        var loc = location.peek();

        if (!loc.field().isRepeated() || loc.field().isMapField())
            throw new ETracInternal("applyRepeated() can only apply to repeated fields (not including map fields)");

        if (done())
            return this;

        var size = loc.parent().msg().getRepeatedFieldCount(loc.field());

        var resultCtx = this;

        for (var i = 0; i < size; i++) {

            resultCtx = (ValidationContextImpl) resultCtx
                    .pushRepeatedItem(i)
                    .apply(validator, targetClass, arg)
                    .pop();
        }

        return resultCtx;
    }

    @Override public ValidationContext
    applyMapKeys(ValidationFunction.Basic validator) {

        return applyMap((obj, arg, ctx) -> validator.apply(ctx), String.class, null, true);
    }

    @Override public ValidationContext
    applyMapKeys(ValidationFunction.Typed<String> validator) {

        return applyMap((obj, arg, ctx) -> validator.apply(obj, ctx), String.class, null, true);
    }

    @Override public <U> ValidationContext
    applyMapKeys(ValidationFunction.TypedArg<String, U> validator, U arg) {

        return applyMap(validator, String.class, arg, true);
    }

    @Override public ValidationContext
    applyMapValues(ValidationFunction.Basic validator) {

        return applyMap((obj, arg, ctx) -> validator.apply(ctx), Object.class, null, false);
    }

    @Override public <T> ValidationContext
    applyMapValues(ValidationFunction.Typed<T> validator, Class<T> targetClass) {

        return applyMap((obj, arg, ctx) -> validator.apply(obj, ctx), targetClass, null, false);
    }

    @Override public <T, U> ValidationContext
    applyMapValues(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg) {

        return applyMap(validator, targetClass, arg, false);
    }

    private <T, U> ValidationContext
    applyMap(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg, boolean pushKey) {

        var funcName = pushKey ? "[applyMapKeys]" : "[applyMapValues]";

        var loc = location.peek();

        if (!loc.field().isMapField())
            throw new ETracInternal(funcName + " can only apply to map fields");

        if (done())
            return this;

        var resultCtx = this;

        if (loc.target() instanceof Map) {

            var map = (Map<?, ?>) loc.target();

            for (var key : map.keySet())

                resultCtx = (ValidationContextImpl) resultCtx
                        .pushMapEntry(key, pushKey, false)
                        .apply(validator, targetClass, arg)
                        .pop();
        }
        else {

            var mapSize = loc.parent().msg().getRepeatedFieldCount(loc.field());

            for (var i = 0; i < mapSize; i++)

                resultCtx = (ValidationContextImpl) resultCtx
                        .pushMapEntry(i, pushKey)
                        .apply(validator, targetClass, arg)
                        .pop();
        }

        return resultCtx;
    }

    public ValidationType validationType() {
        return validationType;
    }

    public ValidationKey key() {

        var loc = location.peek();
        var msg = loc.msg();

        if (msg == null)
            throw new EUnexpected();

        return new ValidationKey(validationType, msg.getDescriptorForType(), loc.method());
    }

    public Object target() {
        return location.peek().target();
    }

    public Message parentMsg() {

        var loc = location.peek();
        var parent = loc.parent();

        if (parent == null)
            throw new ETracInternal("Attempt to access parent of root location");

        // For repeated field items, we need to go up two levels in the location hierarchy
        if (loc.isRepeated() && loc.field().equals(parent.field()))
            parent = parent.parent();

        return parent.msg();
    }

    public boolean isOneOf() {
        return location.peek().isOneOf();
    }

    public boolean isRepeated() {
        return location.peek().field().isRepeated();
    }

    public boolean isMap() {
        return location.peek().field().isMapField();
    }

    public Descriptors.OneofDescriptor oneOf() {
        return location.peek().oneOf();
    }

    public Descriptors.FieldDescriptor field() {
        return location.peek().field();
    }

    public String fieldName() {
        return location.peek().fieldName();
    }

    public ValidationContext prior() {
        return priorCtx;
    }

    public boolean failed() {
        return location.peek().failed();
    }

    public boolean skipped() {
        return location.peek().skipped();
    }

    public boolean done() {
        return location.peek().done();
    }

    public List<ValidationFailure> getErrors() {
        return failures;
    }
}
