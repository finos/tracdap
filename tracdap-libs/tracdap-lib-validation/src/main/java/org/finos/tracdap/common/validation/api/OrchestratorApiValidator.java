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

package org.finos.tracdap.common.validation.api;

import org.finos.tracdap.api.*;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.*;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC, serviceFile = Orchestrator.class, serviceName = TracOrchestratorApiGrpc.SERVICE_NAME)
public class OrchestratorApiValidator {

    private static final Descriptors.Descriptor JOB_REQUEST;
    private static final Descriptors.FieldDescriptor JR_TENANT;
    private static final Descriptors.FieldDescriptor JR_JOB;
    private static final Descriptors.FieldDescriptor JR_JOB_ATTRS;

    private static final Descriptors.Descriptor JOB_STATUS_REQUEST;
    private static final Descriptors.FieldDescriptor JSR_TENANT;
    private static final Descriptors.FieldDescriptor JSR_SELECTOR;

    static {

        JOB_REQUEST = JobRequest.getDescriptor();
        JR_TENANT = field(JOB_REQUEST, JobRequest.TENANT_FIELD_NUMBER);
        JR_JOB = field(JOB_REQUEST, JobRequest.JOB_FIELD_NUMBER);
        JR_JOB_ATTRS = field(JOB_REQUEST, JobRequest.JOBATTRS_FIELD_NUMBER);

        JOB_STATUS_REQUEST = JobStatusRequest.getDescriptor();
        JSR_TENANT = field(JOB_STATUS_REQUEST, JobStatusRequest.TENANT_FIELD_NUMBER);
        JSR_SELECTOR = field(JOB_STATUS_REQUEST, JobStatusRequest.SELECTOR_FIELD_NUMBER);
    }

    @Validator(method = "validateJob")
    public static ValidationContext validateJob(JobRequest msg, ValidationContext ctx) {

        return validateOrSubmit(msg, ctx);
    }

    @Validator(method = "submitJob")
    public static ValidationContext submitJob(JobRequest msg, ValidationContext ctx) {

        return validateOrSubmit(msg, ctx);
    }

    private static ValidationContext validateOrSubmit(JobRequest msg, ValidationContext ctx) {

        ctx = ctx.push(JR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(JR_JOB)
                .apply(CommonValidators::required)
                .applyRegistered()
                .apply(JobValidator::outputsMustBeEmpty, JobDefinition.class)
                .pop();

        ctx = ctx.pushRepeated(JR_JOB_ATTRS)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .applyRepeated(TagUpdateValidator::reservedAttrs, TagUpdate.class, false)
                .pop();

        return ctx;
    }

    @Validator(method = "checkJob")
    public static ValidationContext checkJob(JobStatusRequest msg, ValidationContext ctx) {

        ctx = ctx.push(JSR_TENANT)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(JSR_SELECTOR)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.JOB)
                .pop();

        return ctx;
    }
}
