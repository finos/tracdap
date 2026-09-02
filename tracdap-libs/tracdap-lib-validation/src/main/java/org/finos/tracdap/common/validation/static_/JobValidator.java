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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.Map;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class JobValidator {

    private static final Map<JobDefinition.JobDetailsCase, JobType> JOB_DETAILS_CASE_MAPPING = Map.ofEntries(
            Map.entry(JobDefinition.JobDetailsCase.RUNMODEL, JobType.RUN_MODEL),
            Map.entry(JobDefinition.JobDetailsCase.RUNFLOW, JobType.RUN_FLOW),
            Map.entry(JobDefinition.JobDetailsCase.IMPORTMODEL, JobType.IMPORT_MODEL),
            Map.entry(JobDefinition.JobDetailsCase.IMPORTDATA, JobType.IMPORT_DATA),
            Map.entry(JobDefinition.JobDetailsCase.EXPORTDATA, JobType.EXPORT_DATA));

    private static final List<ObjectType> ALLOWED_IO_TYPES = List.of(ObjectType.DATA, ObjectType.FILE);

    private static final Descriptors.Descriptor JOB_DEFINITION;
    private static final Descriptors.FieldDescriptor JD_JOB_TYPE;
    private static final Descriptors.OneofDescriptor JD_JOB_DETAILS;
    private static final Descriptors.FieldDescriptor JD_RESULT_ID;

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
    private static final Descriptors.FieldDescriptor RMJ_RESOURCES;
    private static final Descriptors.FieldDescriptor RMJ_OUTPUT_ATTRS;

    private static final Descriptors.Descriptor RUN_FLOW_JOB;
    private static final Descriptors.FieldDescriptor RFJ_FLOW;
    private static final Descriptors.FieldDescriptor RFJ_MODELS;
    private static final Descriptors.FieldDescriptor RFJ_PARAMETERS;
    private static final Descriptors.FieldDescriptor RFJ_INPUTS;
    private static final Descriptors.FieldDescriptor RFJ_OUTPUTS;
    private static final Descriptors.FieldDescriptor RFJ_PRIOR_OUTPUTS;
    private static final Descriptors.FieldDescriptor RFJ_RESOURCES;
    private static final Descriptors.FieldDescriptor RFJ_OUTPUT_ATTRS;

    private static final Descriptors.Descriptor IMPORT_DATA_JOB;
    private static final Descriptors.FieldDescriptor IDJ_MODEL;
    private static final Descriptors.FieldDescriptor IDJ_PARAMETERS;
    private static final Descriptors.FieldDescriptor IDJ_INPUTS;
    private static final Descriptors.FieldDescriptor IDJ_OUTPUTS;
    private static final Descriptors.FieldDescriptor IDJ_PRIOR_OUTPUTS;
    private static final Descriptors.FieldDescriptor IDJ_STORAGE_ACCESS;
    private static final Descriptors.FieldDescriptor IDJ_IMPORTS;
    private static final Descriptors.FieldDescriptor IDJ_OUTPUT_ATTRS;
    private static final Descriptors.FieldDescriptor IDJ_IMPORT_ATTRS;

    private static final Descriptors.Descriptor EXPORT_DATA_JOB;
    private static final Descriptors.FieldDescriptor EDJ_MODEL;
    private static final Descriptors.FieldDescriptor EDJ_PARAMETERS;
    private static final Descriptors.FieldDescriptor EDJ_INPUTS;
    private static final Descriptors.FieldDescriptor EDJ_OUTPUTS;
    private static final Descriptors.FieldDescriptor EDJ_PRIOR_OUTPUTS;
    private static final Descriptors.FieldDescriptor EDJ_STORAGE_ACCESS;
    private static final Descriptors.FieldDescriptor EDJ_EXPORTS;
    private static final Descriptors.FieldDescriptor EDJ_OUTPUT_ATTRS;

    static {

        JOB_DEFINITION = JobDefinition.getDescriptor();
        JD_JOB_TYPE = field(JOB_DEFINITION, JobDefinition.JOBTYPE_FIELD_NUMBER);
        JD_JOB_DETAILS = field(JOB_DEFINITION, JobDefinition.RUNMODEL_FIELD_NUMBER).getContainingOneof();
        JD_RESULT_ID = field(JOB_DEFINITION, JobDefinition.RESULTID_FIELD_NUMBER);

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
        RMJ_RESOURCES = field(RUN_MODEL_JOB, RunModelJob.RESOURCES_FIELD_NUMBER);
        RMJ_OUTPUT_ATTRS = field(RUN_MODEL_JOB, RunModelJob.OUTPUTATTRS_FIELD_NUMBER);

        RUN_FLOW_JOB = RunFlowJob.getDescriptor();
        RFJ_FLOW = field(RUN_FLOW_JOB, RunFlowJob.FLOW_FIELD_NUMBER);
        RFJ_MODELS = field(RUN_FLOW_JOB, RunFlowJob.MODELS_FIELD_NUMBER);
        RFJ_PARAMETERS = field(RUN_FLOW_JOB, RunFlowJob.PARAMETERS_FIELD_NUMBER);
        RFJ_INPUTS = field(RUN_FLOW_JOB, RunFlowJob.INPUTS_FIELD_NUMBER);
        RFJ_OUTPUTS = field(RUN_FLOW_JOB, RunFlowJob.OUTPUTS_FIELD_NUMBER);
        RFJ_PRIOR_OUTPUTS = field(RUN_FLOW_JOB, RunFlowJob.PRIOROUTPUTS_FIELD_NUMBER);
        RFJ_RESOURCES = field(RUN_FLOW_JOB, RunFlowJob.RESOURCES_FIELD_NUMBER);
        RFJ_OUTPUT_ATTRS = field(RUN_FLOW_JOB, RunFlowJob.OUTPUTATTRS_FIELD_NUMBER);

        IMPORT_DATA_JOB = ImportDataJob.getDescriptor();
        IDJ_MODEL = field(IMPORT_DATA_JOB, ImportDataJob.MODEL_FIELD_NUMBER);
        IDJ_PARAMETERS = field(IMPORT_DATA_JOB, ImportDataJob.PARAMETERS_FIELD_NUMBER);
        IDJ_INPUTS = field(IMPORT_DATA_JOB, ImportDataJob.INPUTS_FIELD_NUMBER);
        IDJ_OUTPUTS = field(IMPORT_DATA_JOB, ImportDataJob.OUTPUTS_FIELD_NUMBER);
        IDJ_PRIOR_OUTPUTS = field(IMPORT_DATA_JOB, ImportDataJob.PRIOROUTPUTS_FIELD_NUMBER);
        IDJ_STORAGE_ACCESS = field(IMPORT_DATA_JOB, ImportDataJob.STORAGEACCESS_FIELD_NUMBER);
        IDJ_IMPORTS = field(IMPORT_DATA_JOB, ImportDataJob.IMPORTS_FIELD_NUMBER);
        IDJ_OUTPUT_ATTRS = field(IMPORT_DATA_JOB, ImportDataJob.OUTPUTATTRS_FIELD_NUMBER);
        IDJ_IMPORT_ATTRS = field(IMPORT_DATA_JOB, ImportDataJob.IMPORTATTRS_FIELD_NUMBER);

        EXPORT_DATA_JOB = ExportDataJob.getDescriptor();
        EDJ_MODEL = field(EXPORT_DATA_JOB, ExportDataJob.MODEL_FIELD_NUMBER);
        EDJ_PARAMETERS = field(EXPORT_DATA_JOB, ExportDataJob.PARAMETERS_FIELD_NUMBER);
        EDJ_INPUTS = field(EXPORT_DATA_JOB, ExportDataJob.INPUTS_FIELD_NUMBER);
        EDJ_OUTPUTS = field(EXPORT_DATA_JOB, ExportDataJob.OUTPUTS_FIELD_NUMBER);
        EDJ_PRIOR_OUTPUTS = field(EXPORT_DATA_JOB, ExportDataJob.PRIOROUTPUTS_FIELD_NUMBER);
        EDJ_STORAGE_ACCESS = field(EXPORT_DATA_JOB, ExportDataJob.STORAGEACCESS_FIELD_NUMBER);
        EDJ_EXPORTS = field(EXPORT_DATA_JOB, ExportDataJob.EXPORTS_FIELD_NUMBER);
        EDJ_OUTPUT_ATTRS = field(EXPORT_DATA_JOB, ExportDataJob.OUTPUTATTRS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext job(JobDefinition msg, ValidationContext ctx) {

        return ctx.apply(JobValidator::job, JobDefinition.class, /* isClientRequest = */ false);
    }

    // Do not register two validators for the same object type
    // This method is called directly from the orch API validator
    public static ValidationContext jobRequest(JobDefinition msg, ValidationContext ctx) {

        return ctx
                .apply(JobValidator::job, JobDefinition.class, /* isClientRequest = */ true)
                .apply(JobValidator::outputsMustBeEmpty, JobDefinition.class);
    }

    public static ValidationContext job(JobDefinition msg, boolean isClientRequest, ValidationContext ctx) {

        ctx = ctx.push(JD_JOB_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, JobType.class)
                .pop();

        ctx = ctx.pushOneOf(JD_JOB_DETAILS)
                .apply(CommonValidators::required)
                .apply(JobValidator::jobMatchesType)
                .applyRegistered()
                .pop();

        // Jobs submitted through the API must not contain a result ID (it is added later by the orchestrator)

        var clientRequestQualifier = "a job is submitted from the client";

        ctx = ctx.push(JD_RESULT_ID)
                .apply(CommonValidators.ifAndOnlyIf(!isClientRequest, clientRequestQualifier, true))
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.RESULT)
                .apply(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
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

        ctx = runModelOrFlow(ctx, RMJ_PARAMETERS, RMJ_INPUTS, RMJ_OUTPUTS, RMJ_PRIOR_OUTPUTS, RMJ_RESOURCES);

        return outputAttrs(ctx, RMJ_OUTPUT_ATTRS);
    }

    @Validator
    public static ValidationContext runFlowJob(RunFlowJob msg, ValidationContext ctx) {

        ctx = ctx.push(RFJ_FLOW)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.FLOW)
                .pop();

        ctx = ctx.pushMap(RFJ_MODELS)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.MODEL)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = runModelOrFlow(ctx, RFJ_PARAMETERS, RFJ_INPUTS, RFJ_OUTPUTS, RFJ_PRIOR_OUTPUTS, RFJ_RESOURCES);

        return outputAttrs(ctx, RFJ_OUTPUT_ATTRS);
    }

    public static ValidationContext runModelOrFlow(
            ValidationContext ctx,
            Descriptors.FieldDescriptor parameters,
            Descriptors.FieldDescriptor inputs,
            Descriptors.FieldDescriptor outputs,
            Descriptors.FieldDescriptor priorOutputs,
            Descriptors.FieldDescriptor resources) {

        ctx = ctx.pushMap(parameters)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(TypeSystemValidator::value, Value.class)
                .pop();

        ctx = ctx.pushMap(inputs)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ALLOWED_IO_TYPES)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(outputs)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ALLOWED_IO_TYPES)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(priorOutputs)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ALLOWED_IO_TYPES)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(resources)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(CommonValidators::identifier, String.class)
                .applyMapValues(CommonValidators::notTracReserved, String.class)
                .pop();

        return ctx;
    }

    private static ValidationContext outputAttrs(ValidationContext ctx, Descriptors.FieldDescriptor outputAttrs) {

        return ctx.pushRepeated(outputAttrs)
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .applyRepeated(TagUpdateValidator::reservedAttrs, TagUpdate.class, false)
                .pop();
    }

    @Validator
    public static ValidationContext importDataJob(ImportDataJob msg, ValidationContext ctx) {

        ctx = ctx.push(IDJ_MODEL)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.MODEL)
                .pop();

        ctx = importOrExportJob(ctx, IDJ_PARAMETERS, IDJ_INPUTS, IDJ_OUTPUTS, IDJ_PRIOR_OUTPUTS, IDJ_STORAGE_ACCESS);

        ctx = outputAttrs(ctx, IDJ_OUTPUT_ATTRS);

        if (msg.getImportsCount() > 0) {
            ctx = ctx.pushMap(IDJ_IMPORTS)
                    .error("The imports field is not currently supported and must be empty")
                    .pop();
        }

        if (msg.getImportAttrsCount() > 0) {
            ctx = ctx.pushRepeated(IDJ_IMPORT_ATTRS)
                    .error("The importAttrs field is not currently supported and must be empty")
                    .pop();
        }

        return ctx;
    }

    @Validator
    public static ValidationContext exportDataJob(ExportDataJob msg, ValidationContext ctx) {

        ctx = ctx.push(EDJ_MODEL)
                .apply(CommonValidators::required)
                .apply(ObjectIdValidator::tagSelector, TagSelector.class)
                .apply(ObjectIdValidator::selectorType, TagSelector.class, ObjectType.MODEL)
                .pop();

        ctx = importOrExportJob(ctx, EDJ_PARAMETERS, EDJ_INPUTS, EDJ_OUTPUTS, EDJ_PRIOR_OUTPUTS, EDJ_STORAGE_ACCESS);

        ctx = outputAttrs(ctx, EDJ_OUTPUT_ATTRS);

        if (msg.getExportsCount() > 0) {
            ctx = ctx.pushMap(EDJ_EXPORTS)
                    .error("The exports field is not currently supported and must be empty")
                    .pop();
        }

        return ctx;
    }

    // Duplicates runModelOrFlow's parameters/inputs/outputs/priorOutputs validation blocks
    // (not shared with RunModelJob/RunFlowJob)
    private static ValidationContext importOrExportJob(
            ValidationContext ctx,
            Descriptors.FieldDescriptor parameters,
            Descriptors.FieldDescriptor inputs,
            Descriptors.FieldDescriptor outputs,
            Descriptors.FieldDescriptor priorOutputs,
            Descriptors.FieldDescriptor storageAccess) {

        ctx = ctx.pushMap(parameters)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(TypeSystemValidator::value, Value.class)
                .pop();

        ctx = ctx.pushMap(inputs)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ALLOWED_IO_TYPES)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(outputs)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ALLOWED_IO_TYPES)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushMap(priorOutputs)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(ObjectIdValidator::tagSelector, TagSelector.class)
                .applyMapValues(ObjectIdValidator::selectorType, TagSelector.class, ALLOWED_IO_TYPES)
                .applyMapValues(ObjectIdValidator::fixedObjectVersion, TagSelector.class)
                .pop();

        ctx = ctx.pushRepeated(storageAccess)
                .applyRepeated(CommonValidators::identifier)
                .applyRepeated(CommonValidators::notTracReserved)
                .pop();

        return ctx;
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

        else if (msg.getJobType() == JobType.IMPORT_DATA)
            ctx = ctx.apply(JobValidator::outputsMustBeEmpty, ImportDataJob.class);

        else if (msg.getJobType() == JobType.EXPORT_DATA)
            ctx = ctx.apply(JobValidator::outputsMustBeEmpty, ExportDataJob.class);

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

    private static ValidationContext outputsMustBeEmpty(ImportDataJob msg, ValidationContext ctx) {

        if (msg.getOutputsCount() > 0) {

            ctx = ctx.pushMap(IDJ_OUTPUTS)
                    .error("Outputs must be empty, they cannot be specified explicitly when submitting a job")
                    .pop();
        }

        return ctx;
    }

    private static ValidationContext outputsMustBeEmpty(ExportDataJob msg, ValidationContext ctx) {

        if (msg.getOutputsCount() > 0) {

            ctx = ctx.pushMap(EDJ_OUTPUTS)
                    .error("Outputs must be empty, they cannot be specified explicitly when submitting a job")
                    .pop();
        }

        return ctx;
    }
}
