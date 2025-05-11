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

import org.finos.tracdap.api.internal.RuntimeJobResult;
import org.finos.tracdap.api.internal.RuntimeJobResultAttrs;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.*;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;

@Validator(type = ValidationType.STATIC)
public class RuntimeResultsValidator {

    private static final Descriptors.Descriptor RUNTIME_JOB_RESULT;
    private static final Descriptors.FieldDescriptor RJR_JOB_ID;
    private static final Descriptors.FieldDescriptor RJR_RESULT_ID;
    private static final Descriptors.FieldDescriptor RJR_RESULT;
    private static final Descriptors.FieldDescriptor RJR_OBJECT_IDS;
    private static final Descriptors.FieldDescriptor RJR_OBJECTS;
    private static final Descriptors.FieldDescriptor RJR_ATTRS;

    private static final Descriptors.Descriptor RUNTIME_JOB_ATTRS;
    private static final Descriptors.FieldDescriptor RJA_ATTRS;

    static {

        RUNTIME_JOB_RESULT = RuntimeJobResult.getDescriptor();
        RJR_JOB_ID = field(RUNTIME_JOB_RESULT, RuntimeJobResult.JOBID_FIELD_NUMBER);
        RJR_RESULT_ID = field(RUNTIME_JOB_RESULT, RuntimeJobResult.RESULTID_FIELD_NUMBER);
        RJR_RESULT = field(RUNTIME_JOB_RESULT, RuntimeJobResult.RESULT_FIELD_NUMBER);
        RJR_OBJECT_IDS = field(RUNTIME_JOB_RESULT, RuntimeJobResult.OBJECTIDS_FIELD_NUMBER);
        RJR_OBJECTS = field(RUNTIME_JOB_RESULT, RuntimeJobResult.OBJECTS_FIELD_NUMBER);
        RJR_ATTRS = field(RUNTIME_JOB_RESULT, RuntimeJobResult.ATTRS_FIELD_NUMBER);

        RUNTIME_JOB_ATTRS = RuntimeJobResultAttrs.getDescriptor();
        RJA_ATTRS = field(RUNTIME_JOB_ATTRS, RuntimeJobResultAttrs.ATTRS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext job(RuntimeJobResult msg, ValidationContext ctx) {

        ctx = ctx.push(RJR_JOB_ID)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagHeader, TagHeader.class)
                .apply(ObjectIdValidator::headerType, TagHeader.class, ObjectType.JOB)
                .pop();

        ctx = ctx.push(RJR_RESULT_ID)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagHeader, TagHeader.class)
                .apply(ObjectIdValidator::headerType, TagHeader.class, ObjectType.RESULT)
                .pop();

        ctx.push(RJR_RESULT)
                .apply(CommonValidators::required)
                .applyRegistered()
                .pop();

        ctx = ctx.pushRepeated(RJR_OBJECT_IDS)
                .applyRepeated(ObjectIdValidator::tagHeader, TagHeader.class)
                .pop();

        ctx = ctx.pushMap(RJR_OBJECTS)
                .applyMapValues(ObjectValidator::objectDefinition, ObjectDefinition.class)
                .pop();

        ctx = ctx.pushMap(RJR_ATTRS)
                .applyMapValues(RuntimeResultsValidator::runtimeJobAttrs, RuntimeJobResultAttrs.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext runtimeJobAttrs(RuntimeJobResultAttrs msg, ValidationContext ctx) {

        return ctx.pushRepeated(RJA_ATTRS)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .pop();
    }
}
