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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.Map;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class JobValidator {

    private static final Map<JobDefinition.JobDetailsCase, JobType> JOB_DETAILS_CASE_MAPPING = Map.ofEntries(
            Map.entry(JobDefinition.JobDetailsCase.RUNMODEL, JobType.RUN_MODEL),
            Map.entry(JobDefinition.JobDetailsCase.RUNFLOW, JobType.RUN_FLOW),
            Map.entry(JobDefinition.JobDetailsCase.IMPORTMODEL, JobType.IMPORT_MODEL));

    private static final Descriptors.Descriptor JOB_DEFINITION;
    private static final Descriptors.FieldDescriptor JD_JOB_TYPE;
    private static final Descriptors.OneofDescriptor JD_JOB_DETAILS;

    private static final Descriptors.Descriptor IMPORT_MODEL_JOB;
    private static final Descriptors.FieldDescriptor IMJ_LANGUAGE;
    private static final Descriptors.FieldDescriptor IMJ_REPOSITORY;
    private static final Descriptors.FieldDescriptor IMJ_PATH;
    private static final Descriptors.FieldDescriptor IMJ_ENTRY_POINT;
    private static final Descriptors.FieldDescriptor IMJ_VERSION;

    private static final Descriptors.Descriptor RUN_MODEL_JOB;
    private static final Descriptors.FieldDescriptor RMJ_MODEL;
    private static final Descriptors.FieldDescriptor RMJ_PARAMETERS;
    private static final Descriptors.FieldDescriptor RMJ_INPUTS;
    private static final Descriptors.FieldDescriptor RMJ_OUTPUTS;
    private static final Descriptors.FieldDescriptor RMJ_PRIOR_OUTPUTS;

    static {

        JOB_DEFINITION = JobDefinition.getDescriptor();
        JD_JOB_TYPE = field(JOB_DEFINITION, JobDefinition.JOBTYPE_FIELD_NUMBER);
        JD_JOB_DETAILS = field(JOB_DEFINITION, JobDefinition.RUNMODEL_FIELD_NUMBER).getContainingOneof();

        IMPORT_MODEL_JOB = ImportModelJob.getDescriptor();
        IMJ_LANGUAGE = field(IMPORT_MODEL_JOB, ImportModelJob.LANGUAGE_FIELD_NUMBER);
        IMJ_REPOSITORY = field(IMPORT_MODEL_JOB, ImportModelJob.REPOSITORY_FIELD_NUMBER);
        IMJ_PATH = field(IMPORT_MODEL_JOB, ImportModelJob.PATH_FIELD_NUMBER);
        IMJ_ENTRY_POINT = field(IMPORT_MODEL_JOB, ImportModelJob.ENTRYPOINT_FIELD_NUMBER);
        IMJ_VERSION = field(IMPORT_MODEL_JOB, ImportModelJob.VERSION_FIELD_NUMBER);

        RUN_MODEL_JOB = RunModelJob.getDescriptor();
        RMJ_MODEL = field(RUN_MODEL_JOB, RunModelJob.MODEL_FIELD_NUMBER);
        RMJ_PARAMETERS = field(RUN_MODEL_JOB, RunModelJob.PARAMETERS_FIELD_NUMBER);
        RMJ_INPUTS = field(RUN_MODEL_JOB, RunModelJob.INPUTS_FIELD_NUMBER);
        RMJ_OUTPUTS = field(RUN_MODEL_JOB, RunModelJob.OUTPUTS_FIELD_NUMBER);
        RMJ_PRIOR_OUTPUTS = field(RUN_MODEL_JOB, RunModelJob.PRIOROUTPUTS_FIELD_NUMBER);
    }


    @Validator
    public static ValidationContext job(JobDefinition msg, ValidationContext ctx) {

        ctx = ctx.push(JD_JOB_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, JobType.class)
                .pop();

        ctx = ctx.pushOneOf(JD_JOB_DETAILS)
                .apply(CommonValidators::required)
                .apply(JobValidator::jobMatchesType)
                .applyRegistered()
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext importModelJob(ImportModelJob msg, ValidationContext ctx) {

        return ModelValidator.modelDetails(IMJ_LANGUAGE, IMJ_REPOSITORY, IMJ_PATH, IMJ_ENTRY_POINT, IMJ_VERSION, ctx);
    }

    @Validator
    public static ValidationContext runModelJob(RunModelJob msg, ValidationContext ctx) {

        ctx = ctx.push(RMJ_MODEL)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.MODEL)
                .pop();

        ctx = ctx.pushMap(RMJ_PARAMETERS)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(TypeSystemValidator::value, Value.class)
                .pop();

        ctx = ctx.pushMap(RMJ_INPUTS)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.DATA)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(RMJ_OUTPUTS)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.DATA)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(RMJ_PRIOR_OUTPUTS)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.DATA)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext runFlowJob(RunFlowJob msg, ValidationContext ctx) {

        return ctx.error("Run flow not implemented yet");
    }

    private static ValidationContext jobMatchesType(ValidationContext ctx) {

        var job = (JobDefinition) ctx.parentMsg();
        var detailsCase = job.getJobDetailsCase();

        var jobType = job.getJobType();
        var detailsType = JOB_DETAILS_CASE_MAPPING.getOrDefault(detailsCase, JobType.UNRECOGNIZED);

        if (jobType != detailsType) {

            var err = String.format("Job has type [%s] but contains details of type [%s]",
                    jobType, detailsType);

            return ctx.error(err);

        }
        return ctx;
    }

    public static ValidationContext outputsMustBeEmpty(JobDefinition msg, ValidationContext ctx) {

        ctx = ctx.pushOneOf(JD_JOB_DETAILS);

        if (msg.getJobType() == JobType.RUN_MODEL)
            ctx = ctx.apply(JobValidator::outputsMustBeEmpty, RunModelJob.class);

        return ctx.pop();
    }

    private static ValidationContext outputsMustBeEmpty(RunModelJob msg, ValidationContext ctx) {

        if (msg.getOutputsCount() > 0) {

            ctx = ctx.push(RMJ_OUTPUTS)
                    .error("Outputs must be empty, they cannot be specified explicitly when submitting a job")
                    .pop();
        }

        return ctx;
    }
}
