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

import com.accenture.trac.common.exception.EUnexpected;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

class ValidationLocation {

    private final ValidationLocation parent;

    private final Descriptors.MethodDescriptor method = null;
    private final Descriptors.OneofDescriptor oneOf;
    private final Descriptors.FieldDescriptor field;
    private final String fieldName;

    private final Object msg;

    private boolean failed;
    private boolean skipped;

    public ValidationLocation(
            ValidationLocation parent,
            Descriptors.OneofDescriptor oneOf,
            Descriptors.FieldDescriptor field,
            String fieldName,
            Object msg) {

        this.parent = parent;
        this.oneOf = oneOf;
        this.field = field;
        this.fieldName = fieldName;

        this.msg = msg;

        this.failed = false;
        this.skipped = false;
    }

    public ValidationLocation(
            ValidationLocation parent,
            Descriptors.FieldDescriptor field,
            String fieldName,
            Object msg) {

        this(parent, null, field, fieldName, msg);
    }

    public void fail() {
        failed = true;
    }

    public void skip() {
        skipped = true;
    }

    public boolean isOneOf() {
        return oneOf != null;
    }

    public Descriptors.OneofDescriptor oneOf() {
        return oneOf;
    }

    public Descriptors.FieldDescriptor field() {
        return field;
    }

    public String fieldName() {
        return fieldName;
    }

    public Object obj() {
        return msg;
    }

    public Message msg() {

        if (!(msg instanceof Message))
            throw new EUnexpected();

        return (Message) msg;
    }

    public boolean failed() {
        return failed;
    }

    public boolean skipped() {
        return skipped;
    }

    public boolean done() {
        return failed || skipped;
    }


    public ValidationLocation parent() {
        return parent;
    }

    public List<ValidationLocation> stack() {
        return stack(0);
    }

    private List<ValidationLocation> stack(int depth) {

        var stack = (parent == null)
                ? new ArrayList<ValidationLocation>(depth + 1)
                : parent.stack(depth + 1);

        stack.set(depth, this);

        return stack;
    }
}
