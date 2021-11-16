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

package com.accenture.trac.common.validation.core;

import com.accenture.trac.common.exception.EUnexpected;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

class ValidationLocation {

    private final ValidationLocation parent;
    private final ValidationKey key;
    private final Object target;
    private final Object prior;

    private final Descriptors.MethodDescriptor method = null;
    private final Descriptors.OneofDescriptor oneOf;
    private final Descriptors.FieldDescriptor field;
    private final String fieldName;

    // For one-of locations, prior field may be different from current
    private final Descriptors.FieldDescriptor priorField;
    private final String priorFieldName;


    private boolean failed;
    private boolean skipped;

    public ValidationLocation(
            ValidationLocation parent,
            ValidationKey key,
            Object target,
            Object prior,

            Descriptors.OneofDescriptor oneOf,
            Descriptors.FieldDescriptor field,
            String fieldName,

            Descriptors.FieldDescriptor priorField,
            String priorFieldName) {

        this.parent = parent;
        this.key = key;
        this.target = target;
        this.prior = prior;

        this.oneOf = oneOf;
        this.field = field;
        this.fieldName = fieldName;

        this.priorField = priorField;
        this.priorFieldName = priorFieldName;

        this.failed = false;
        this.skipped = false;
    }

    public ValidationLocation(
            ValidationLocation parent,
            ValidationKey key,
            Object target,
            Object prior,

            Descriptors.OneofDescriptor oneOf,
            Descriptors.FieldDescriptor field,
            String fieldName) {

        this(parent, key, target, prior, oneOf, field, fieldName, field, fieldName);
    }

    public ValidationLocation(
            ValidationLocation parent,
            ValidationKey key,
            Object target,

            Descriptors.OneofDescriptor oneOf,
            Descriptors.FieldDescriptor field,
            String fieldName) {

        this(parent, key, target, null, oneOf, field, fieldName);
    }

    public ValidationLocation(
            ValidationLocation parent,
            ValidationKey key,
            Object target,
            Descriptors.FieldDescriptor field,
            String fieldName) {

        this(parent, key, target, null, null, field, fieldName);
    }

    public ValidationLocation parent() {
        return parent;
    }

    public ValidationKey key() {
        return key;
    }

    public String fieldName() {
        return fieldName;
    }

    public Object target() {
        return target;
    }

    public Object prior() {
        return prior;
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

    public Descriptors.FieldDescriptor priorField() {
        return priorField;
    }

    public String priorFieldName() {
        return priorFieldName;
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

    void fail() {
        failed = true;
    }

    void skip() {
        skipped = true;
    }

    Message msg() {

        if (!(target instanceof Message))
            throw new EUnexpected();

        return (Message) target;
    }

    Message priorMsg() {

        if (prior == null)
            return null;

        if (!(prior instanceof Message))
            throw new EUnexpected();

        return (Message) prior;
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
