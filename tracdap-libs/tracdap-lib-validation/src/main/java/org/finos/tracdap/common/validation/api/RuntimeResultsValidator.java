/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.validation.api;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.*;
import org.finos.tracdap.config.JobResult;
import org.finos.tracdap.config.JobResultAttrs;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;

@Validator(type = ValidationType.STATIC)
public class RuntimeResultsValidator {

    private static final Descriptors.Descriptor RUNTIME_JOB_RESULT;
    private static final Descriptors.FieldDescriptor JR_JOB_ID;
    private static final Descriptors.FieldDescriptor JR_RESULT_ID;
    private static final Descriptors.FieldDescriptor JR_RESULT;
    private static final Descriptors.FieldDescriptor JR_OBJECT_IDS;
    private static final Descriptors.FieldDescriptor JR_OBJECTS;
    private static final Descriptors.FieldDescriptor JR_ATTRS;

    private static final Descriptors.Descriptor RUNTIME_JOB_ATTRS;
    private static final Descriptors.FieldDescriptor JRA_ATTRS;

    static {

        RUNTIME_JOB_RESULT = JobResult.getDescriptor();
        JR_JOB_ID = field(RUNTIME_JOB_RESULT, JobResult.JOBID_FIELD_NUMBER);
        JR_RESULT_ID = field(RUNTIME_JOB_RESULT, JobResult.RESULTID_FIELD_NUMBER);
        JR_RESULT = field(RUNTIME_JOB_RESULT, JobResult.RESULT_FIELD_NUMBER);
        JR_OBJECT_IDS = field(RUNTIME_JOB_RESULT, JobResult.OBJECTIDS_FIELD_NUMBER);
        JR_OBJECTS = field(RUNTIME_JOB_RESULT, JobResult.OBJECTS_FIELD_NUMBER);
        JR_ATTRS = field(RUNTIME_JOB_RESULT, JobResult.ATTRS_FIELD_NUMBER);

        RUNTIME_JOB_ATTRS = JobResultAttrs.getDescriptor();
        JRA_ATTRS = field(RUNTIME_JOB_ATTRS, JobResultAttrs.ATTRS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext job(JobResult msg, ValidationContext ctx) {

        ctx = ctx.push(JR_JOB_ID)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagHeader, TagHeader.class)
                .apply(ObjectIdValidator::headerType, TagHeader.class, ObjectType.JOB)
                .pop();

        ctx = ctx.push(JR_RESULT_ID)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagHeader, TagHeader.class)
                .apply(ObjectIdValidator::headerType, TagHeader.class, ObjectType.RESULT)
                .pop();

        ctx.push(JR_RESULT)
                .apply(CommonValidators::required)
                .applyRegistered()
                .pop();

        ctx = ctx.pushRepeated(JR_OBJECT_IDS)
                .applyRepeated(ObjectIdValidator::tagHeader, TagHeader.class)
                .pop();

        ctx = ctx.pushMap(JR_OBJECTS)
                .applyMapValues(ObjectValidator::objectDefinition, ObjectDefinition.class)
                .pop();

        ctx = ctx.pushMap(JR_ATTRS)
                .applyMapValues(RuntimeResultsValidator::runtimeJobAttrs, JobResultAttrs.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext runtimeJobAttrs(JobResultAttrs msg, ValidationContext ctx) {

        return ctx.pushRepeated(JRA_ATTRS)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .pop();
    }
}
