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
import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationFunction;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;


public class ValidationContextImpl implements ValidationContext {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Stack<ValidationLocation> location;
    private final List<ValidationFailure> failures;

    private ValidationContextImpl(ValidationLocation root) {

        location = new Stack<>();
        location.push(root);

        failures = new ArrayList<>();
    }

    public static ValidationContext forMethod(Message msg, Descriptors.MethodDescriptor descriptor) {

        var key = ValidationKey.forMethod(msg.getDescriptorForType(), descriptor);
        var root = new ValidationLocation(null, key, msg, null, null);
        return new ValidationContextImpl(root);
    }

    public static ValidationContext forMessage(Message msg) {

        var key = ValidationKey.forMethod(msg.getDescriptorForType(), null);
        var root = new ValidationLocation(null, key, msg, null, null);
        return new ValidationContextImpl(root);
    }

    public static ValidationContext forVersion(Message current, Message prior) {

        var key = ValidationKey.forVersion(prior.getDescriptorForType());
        var root = new ValidationLocation(null, key, current, prior, null, null, null);
        return new ValidationContextImpl(root);
    }

    public ValidationContext push(Descriptors.FieldDescriptor fd) {

        var parentLoc = location.peek();
        var msg = parentLoc.msg();
        var priorMsg = parentLoc.priorMsg();

        var obj = msg.getField(fd);
        var priorObj = priorMsg != null ? priorMsg.getField(fd) : null;

        var field = fd.getName();
        var loc = new ValidationLocation(parentLoc, null, obj, priorObj, null, fd, field);

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);

        return this;
    }

    public ValidationContext pushOneOf(Descriptors.OneofDescriptor oneOf) {

        var parentLoc = location.peek();
        var msg = parentLoc.msg();
        var priorMsg = parentLoc.priorMsg();

        // If the oneOf field has been set, look up the fd, field name and value for the field in use
        // In this case the location ctx be reported as that field in any error messages

        // If the oneOf is not set, fd and value cannot be set, the name is left as the oneOf name
        // Error messages for required() will refer to the oneOf name
        // Other checks must be guarded with optional(), omitted() or applyIf()

        var fd = msg.hasOneof(oneOf) ? msg.getOneofFieldDescriptor(oneOf) : null;
        var name = fd != null ? fd.getName() : oneOf.getName();
        var obj = fd != null ? msg.getField(fd) : null;

        var priorFd = priorMsg != null && priorMsg.hasOneof(oneOf) ? priorMsg.getOneofFieldDescriptor(oneOf) : null;
        var priorName = priorFd != null ? priorFd.getName() : oneOf.getName();
        var priorObj = priorFd != null ? priorMsg.getField(priorFd) : null;

        var loc = new ValidationLocation(parentLoc, null, obj, priorObj, oneOf, fd, name, priorFd, priorName);

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);

        return this;
    }

    private ValidationContext pushRepeated(Integer index) {

        var parentLoc = location.peek();

        if (!parentLoc.field().isRepeated() || parentLoc.field().isMapField())
            throw new EUnexpected();

        var list = (List<?>) parentLoc.target();
        var priorList = (List<?>) parentLoc.prior();

        var obj = list.get(index);
        var priorObj = priorList != null ? priorList.get(index) : null;

        var fieldName = String.format("[%d]", index);

        var loc = new ValidationLocation(parentLoc, null, obj, priorObj, null, parentLoc.field(), fieldName);

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);

        return this;
    }

    public ValidationContext pop() {

        if (location.empty())
            throw new IllegalStateException();

        location.pop();

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

    public ValidationContext apply(ValidationFunction.Basic validator) {

        if (done())
            return this;

        return validator.apply(this);
    }

    public ValidationContext apply(ValidationFunction.Typed<String> validator) {
        return apply(validator, String.class);
    }

    @SuppressWarnings({"unchecked", "rawTypes"})
    public <T>
    ValidationContext apply(ValidationFunction.Typed<T> validator, Class<T> targetClass) {

        if (done())
            return this;

        var obj = location.peek().target();

        // Simple case - obj is an instance of the expected target class

        if (targetClass.isInstance(obj)) {
            var target = (T) obj;
            return validator.apply(target, this);
        }

        // Protobuf stores enum values as EnumValueDescriptor objects
        // So, special handling is needed for enums

        if (Enum.class.isAssignableFrom(targetClass)) {

            if (obj instanceof Descriptors.EnumValueDescriptor) {

                var valueDesc = (Descriptors.EnumValueDescriptor) obj;
                var enumType = (Class<? extends Enum>) targetClass;
                var enumVal = Enum.valueOf(enumType, valueDesc.getName());

                return validator.apply((T) enumVal, this);
            }
        }

        // If obj does not match the expected target type, blow up the validation process

        // Type mis-match errors should be detected during development, otherwise the validator won't run
        // In the event of a type mis-match at run time, blow up the validator!
        // I.e. report as an unexpected internal error, validation cannot be completed.

        log.error("Validator type mismatch (this is a bug): expected [{}], got [{}]", targetClass, obj.getClass());
        log.error("(Expected target class is specified in ctx.apply())");

        throw new EUnexpected();
    }

    @SuppressWarnings({"unchecked", "rawTypes"})
    public <T, U>
    ValidationContext applyWith(ValidationFunction.TypedArg<T, U> validator, Class<T> targetClass, U arg) {

        if (done())
            return this;

        var obj = location.peek().target();

        // Simple case - obj is an instance of the expected target class

        if (targetClass.isInstance(obj)) {
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

        // Type mis-match errors should be detected during development, otherwise the validator won't run
        // In the event of a type mis-match at run time, blow up the validator!
        // I.e. report as an unexpected internal error, validation cannot be completed.

        log.error("Validator type mismatch (this is a bug): expected [{}], got [{}]", targetClass, obj.getClass());
        log.error("(Expected target class is specified in ctx.apply())");

        throw new EUnexpected();
    }

    public ValidationContext apply(ValidationFunction.Version<Object> validator) {

        if (done())
            return this;

        var current = location.peek().target();
        var prior = location.peek().prior();

        return validator.apply(current, prior, this);
    }

    @SuppressWarnings("unchecked")
    public <T> ValidationContext apply(ValidationFunction.Version<T> validator, Class<T> targetClass) {

        if (done())
            return this;

        var current = location.peek().target();
        var prior = location.peek().prior();

        if (targetClass.isInstance(prior) && targetClass.isInstance(current)) {

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

        log.error("Validator type mismatch (this is a bug): expected [{}], got prior = [{}], current = [{}]",
                targetClass, prior.getClass(), current.getClass());

        log.error("(Expected target class is specified in ctx.apply())");

        throw new EUnexpected();
    }

    public ValidationContext applyIf(ValidationFunction.Basic validator, boolean condition) {

        if (!condition)
            return this;

        return apply(validator);
    }

    public <T>
    ValidationContext applyIf(ValidationFunction.Typed<T> validator, Class<T> targetClass, boolean condition) {

        if (!condition)
            return this;

        return apply(validator, targetClass);
    }

    public <T>
    ValidationContext applyIf(ValidationFunction.Version<T> validator, Class<T> targetClass, boolean condition) {

        if (!condition)
            return this;

        return apply(validator, targetClass);
    }

    public <TMsg extends Message>
    ValidationContext applyRepeated(ValidationFunction.Typed<TMsg> validator, Class<TMsg> msgClass) {

        if (done())
            return this;

        var loc = location.peek();

        if (!loc.field().isRepeated() || loc.field().isMapField())
            throw new EUnexpected();

        var list = (List<?>) loc.target();

        if (list == null)
            throw new EUnexpected();

        var resultCtx = this;

        for (var i = 0; i < list.size(); i++) {

            resultCtx = (ValidationContextImpl) resultCtx
                    .pushRepeated(i)
                    .apply(validator, msgClass)
                    .pop();
        }

        return resultCtx;
    }

    public <TMsg extends Message, U>
    ValidationContext applyRepeatedWith(ValidationFunction.TypedArg<TMsg, U> validator, Class<TMsg> msgClass, U arg) {

        if (done())
            return this;

        var loc = location.peek();

        if (!loc.field().isRepeated() || loc.field().isMapField())
            throw new EUnexpected();

        var list = (List<?>) loc.target();

        if (list == null)
            throw new EUnexpected();

        var resultCtx = this;

        for (var i = 0; i < list.size(); i++) {

            resultCtx = (ValidationContextImpl) resultCtx
                    .pushRepeated(i)
                    .applyWith(validator, msgClass, arg)
                    .pop();
        }

        return resultCtx;
    }

    public ValidationType validationType() {
        return location.peek().key().validationType();
    }

    public ValidationKey key() {
        return location.peek().key();
    }

    public Object target() {
        return location.peek().target();
    }

    public Message parentMsg() {
        return location.peek().parent().msg();
    }

    public boolean isOneOf() {
        return location.peek().isOneOf();
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

    public Descriptors.FieldDescriptor priorField() {
        return location.peek().priorField();
    }

    public String priorFieldName() {
        return location.peek().priorFieldName();
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
