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

package com.accenture.trac.svc.data.validation;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.*;


public class ValidationContext {

    private final Stack<ValidationLocation> location;
    private final List<ValidationFailure> failures;
    private Message msg;

    private ValidationContext(ValidationLocation root) {

        location = new Stack<>();
        location.push(root);

        failures = new ArrayList<>();
    }

    public static ValidationContext forMessage(Descriptors.Descriptor message) {

        var root = new ValidationLocation(null, null, null);
        return new ValidationContext(root);
    }

    public static ValidationContext forApiCall(Descriptors.MethodDescriptor method) {

        var root = new ValidationLocation(null, null, null);
        return new ValidationContext(root);
    }

    public ValidationContext push(Message msg, String field) {

        var fd = msg.getDescriptorForType().findFieldByName(field);
        var parentLoc = location.peek();
        var loc = new ValidationLocation(parentLoc, fd, field);

        if (parentLoc.skipped())
            loc.skip();

        location.push(loc);
        this.msg = msg;

        return this;
    }

    public ValidationContext pop() {

        if (location.empty())
            throw new IllegalStateException();

        location.pop();
        this.msg = null;

        return this;
    }

    public ValidationContext apply(ValidationFunction validation) {

        if (done())
            return this;

        return validation.validate(msg, this);  // todo: msg
    }

    public ValidationContext skip() {

        var loc = location.peek();
        loc.skip();

        return this;
    }

    public ValidationContext error(String message) {

        var failure = new ValidationFailure(null, message);  // todo: loc in error
        failures.add(failure);

        var loc = location.peek();
        loc.fail();

        return this;
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
