/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.validation.core;

import com.accenture.trac.common.exception.EUnexpected;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.*;


public class ValidationContext {

    private final Stack<ValidationLocation> location;
    private final List<ValidationFailure> failures;

    private ValidationContext(ValidationLocation root) {

        location = new Stack<>();
        location.push(root);

        failures = new ArrayList<>();
    }

    public static ValidationContext forMessage(Descriptors.Descriptor message, Message msg) {

        var root = new ValidationLocation(null, null, null, msg);
        return new ValidationContext(root);
    }

    public static ValidationContext forApiCall(Descriptors.MethodDescriptor method, Message msg) {

        var root = new ValidationLocation(null, null, null, msg);
        return new ValidationContext(root);
    }

    public ValidationContext push(String field) {

        var parentLoc = location.peek();
        var parentMsg = parentLoc.msg();

        var fd = parentMsg.getDescriptorForType().findFieldByName(field);
        var obj = parentMsg.getField(fd);

        var loc = new ValidationLocation(parentLoc, fd, field, obj);

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);

        return this;
    }

    public ValidationContext pushOneOf(String field) {

        var parentLoc = location.peek();
        var parentMsg = parentLoc.msg();

        var oneOfs = parentMsg.getDescriptorForType().getOneofs();
        var oneOfMaybe = oneOfs.stream().filter(oneOf -> oneOf.getName().equals(field)).findFirst();

        if (oneOfMaybe.isEmpty())
            throw new EUnexpected();

        var oneOf = oneOfMaybe.get();

        // If the oneOf field has been set, look up the fd, field name and value for the field in use
        // In this case the location ctx be reported as that field in any error messages

        // If the oneOf is not set, fd and value cannot be set, the name is left as the oneOf name
        // Error messages for required() will refer to the oneOf name
        // Other checks must be guarded with optional(), omitted() or applyIf()

        var fd = parentMsg.hasOneof(oneOf) ? parentMsg.getOneofFieldDescriptor(oneOf) : null;
        var name = fd != null ? fd.getName() : field;
        var obj = fd != null ? parentMsg.getField(fd) : null;

        var loc = new ValidationLocation(parentLoc, oneOf, fd, name, obj);

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);

        return this;
    }

    private ValidationContext pushList(Integer index) {

        var parentLoc = location.peek();

        if (!parentLoc.field().isRepeated() || parentLoc.field().isMapField())
            throw new EUnexpected();

        var list = (List<?>) parentLoc.obj();
        var obj = list.get(index);
        var fieldName = String.format("[%d]", index);

        var loc = new ValidationLocation(parentLoc, parentLoc.field(), fieldName, obj);

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

    public ValidationContext apply(ValidationFunction.Basic validation) {

        if (done())
            return this;

        var obj = location.peek().obj();

        return validation.validate(obj, this);
    }

    public <TMsg extends Message>
    ValidationContext apply(ValidationFunction.Typed<TMsg> validation, Class<TMsg> msgClass) {

        if (done())
            return this;

        var msg = location.peek().msg();

        if (!msgClass.isInstance(msg))
            throw new EUnexpected();

        @SuppressWarnings("unchecked")
        var typedMsg = (TMsg) msg;

        return validation.validate(typedMsg, this);
    }

    public ValidationContext applyIf(ValidationFunction.Basic validation, boolean condition) {

        if (done() || !condition)
            return this;

        var obj = location.peek().obj();

        return validation.validate(obj, this);
    }

    public <TMsg extends Message>
    ValidationContext applyIf(ValidationFunction.Typed<TMsg> validation, Class<TMsg> msgClass, boolean condition) {

        if (done() || !condition)
            return this;

        var msg = location.peek().msg();

        if (!msgClass.isInstance(msg))
            throw new EUnexpected();

        @SuppressWarnings("unchecked")
        var typedMsg = (TMsg) msg;

        return validation.validate(typedMsg, this);
    }

    public <TMsg extends Message>
    ValidationContext applyTypedList(ValidationFunction.Typed<TMsg> validation, Class<TMsg> msgClass) {

        if (done())
            return this;

        var loc = location.peek();

        if (!loc.field().isRepeated() || loc.field().isMapField())
            throw new EUnexpected();

        var list = (List<?>) loc.obj();

        if (list == null)
            throw new EUnexpected();

        var resultCtx = this;

        for (var i = 0; i < list.size(); i++) {

            resultCtx = resultCtx
                    .pushList(i)
                    .apply(validation, msgClass)
                    .pop();
        }

        return resultCtx;
    }

    public ValidationContext skip() {

        var loc = location.peek();
        loc.skip();

        return this;
    }

    public ValidationContext error(String message) {

        var failure = new ValidationFailure(location.peek(), message);
        failures.add(failure);

        var loc = location.peek();
        loc.fail();

        return this;
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
