/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.validation.consistency;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.TypeSystemValidator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Map;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.CONSISTENCY)
public class JobConsistencyValidator {

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

    private static final Descriptors.Descriptor RUN_FLOW_JOB;
    private static final Descriptors.FieldDescriptor RFJ_FLOW;
    private static final Descriptors.FieldDescriptor RFJ_MODELS;
    private static final Descriptors.FieldDescriptor RFJ_PARAMETERS;
    private static final Descriptors.FieldDescriptor RFJ_INPUTS;
    private static final Descriptors.FieldDescriptor RFJ_OUTPUTS;
    private static final Descriptors.FieldDescriptor RFJ_PRIOR_OUTPUTS;

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

        RUN_FLOW_JOB = RunFlowJob.getDescriptor();
        RFJ_FLOW = field(RUN_FLOW_JOB, RunFlowJob.FLOW_FIELD_NUMBER);
        RFJ_MODELS = field(RUN_FLOW_JOB, RunFlowJob.MODELS_FIELD_NUMBER);
        RFJ_PARAMETERS = field(RUN_FLOW_JOB, RunFlowJob.PARAMETERS_FIELD_NUMBER);
        RFJ_INPUTS = field(RUN_FLOW_JOB, RunFlowJob.INPUTS_FIELD_NUMBER);
        RFJ_OUTPUTS = field(RUN_FLOW_JOB, RunFlowJob.OUTPUTS_FIELD_NUMBER);
        RFJ_PRIOR_OUTPUTS = field(RUN_FLOW_JOB, RunFlowJob.PRIOROUTPUTS_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext job(JobDefinition job, ValidationContext ctx) {

        return ctx.pushOneOf(JD_JOB_DETAILS)
                .applyRegistered()
                .pop();
    }

    @Validator
    public static ValidationContext runModelJob(RunModelJob job, ValidationContext ctx) {

        var resources = ctx.getMetadataBundle();
        var modelObj = resources.getResource(job.getModel());

        if (modelObj == null) {
            var message = "Required metadata is not available for [" + MetadataUtil.objectKey(job.getModel()) + "]";
            return ctx.push(RMJ_MODEL).error(message).pop();
        }

        var modelDef = modelObj.getModel();

        ctx.pushMap(RMJ_PARAMETERS, RunModelJob::getParametersMap)
                .apply(JobConsistencyValidator::missingParams, Map.class, modelDef.getParametersMap())
                .applyMapKeys(JobConsistencyValidator::unknownParams, modelDef.getParametersMap())
                .applyMapValuesFunc(JobConsistencyValidator::paramMatchesSchema, Value.class, modelDef::getParametersOrThrow)
                .pop();

        ctx.pushMap(RMJ_INPUTS, RunModelJob::getInputsMap)
                .apply(JobConsistencyValidator::missingInputs, Map.class, modelDef.getInputsMap())
                .applyMapKeys(JobConsistencyValidator::unknownInputs, modelDef.getInputsMap())
                .applyMapValuesFunc(JobConsistencyValidator::inputMatchesSchema, TagSelector.class, modelDef::getInputsOrThrow)
                .pop();

        // Prior outputs are optional, however any provided must be valid
        ctx.pushMap(RMJ_PRIOR_OUTPUTS)
                // No check for missing prior outputs
                .applyMapKeys(JobConsistencyValidator::unknownOutputs, modelDef.getOutputsMap())
                .applyMapValuesFunc(JobConsistencyValidator::priorOutputMatchesSchema, TagSelector.class, modelDef::getOutputsOrThrow)
                .pop();

        // Do not validate final outputs at all
        // Consistency check is applied before the job runs
        ctx.pushMap(RMJ_OUTPUTS)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext runFlowJob(RunFlowJob job, ValidationContext ctx) {

        // TODO: Flow validation
        return ctx;
    }


    @Validator
    public static ValidationContext importModelJob(ImportModelJob job, ValidationContext ctx) {

        // TODO: Model import validation
        return ctx;
    }

    private static ValidationContext missingParams(Map<String, ?> jobParams, Map<String, ModelParameter> requiredParams, ValidationContext ctx) {

        for (var param : requiredParams.entrySet()) {
            if (!jobParams.containsKey(param.getKey()))
                return ctx.error("Missing required parameter [" + param.getKey() + "]");
        }

        return ctx;
    }

    private static ValidationContext unknownParams(String key, Map<String, ?> expectedItems, ValidationContext ctx) {

        if (!expectedItems.containsKey(key))
            return ctx.error("Unknown parameter: [" + key + "]");

        return ctx;
    }

    private static ValidationContext paramMatchesSchema(Value paramValue, ModelParameter requiredParam, ValidationContext ctx) {

        return TypeSystemValidator.valueWithType(paramValue, requiredParam.getParamType(), ctx);
    }

    private static ValidationContext missingInputs(Map<String, ?> jobInputs, Map<String, ModelInputSchema> requiredInputs, ValidationContext ctx) {

        for (var input : requiredInputs.entrySet()) {
            if (!jobInputs.containsKey(input.getKey()))
                return ctx.error("Missing required input [" + input.getKey() + "]");
        }

        return ctx;
    }

    private static ValidationContext unknownInputs(String key, Map<String, ?> expectedItems, ValidationContext ctx) {

        if (!expectedItems.containsKey(key))
            return ctx.error("Unknown input: [" + key + "]");

        return ctx;
    }

    private static ValidationContext inputMatchesSchema(TagSelector inputSelector, ModelInputSchema requiredSchema, ValidationContext ctx) {

        var inputObject = ctx.getMetadataBundle().getResource(inputSelector);

        // Metadata should be loaded before the validator runs (partial validation not available at present)
        if (inputObject == null)
            throw new ETracInternal("Metadata not available for validation");

        if (inputObject.getObjectType() != ObjectType.DATA) {
            return ctx.error(String.format(
                    "Input is not a dataset (expected %s, got %s)",
                    ObjectType.DATA, inputObject.getObjectType()));
        }

        return checkDataSchema(inputObject.getData(), requiredSchema.getSchema(), ctx);
    }

    private static ValidationContext unknownOutputs(String key, Map<String, ?> expectedItems, ValidationContext ctx) {

        if (!expectedItems.containsKey(key))
            return ctx.error("Unknown output: [" + key + "]");

        return ctx;
    }

    private static ValidationContext priorOutputMatchesSchema(TagSelector outputSelector, ModelOutputSchema requiredSchema, ValidationContext ctx) {

        var outputObject = ctx.getMetadataBundle().getResource(outputSelector);

        // Metadata should be loaded before the validator runs (partial validation not available at present)
        if (outputObject == null)
            throw new ETracInternal("Metadata not available for validation");

        if (outputObject.getObjectType() != ObjectType.DATA) {
            return ctx.error(String.format(
                    "Prior output is not a dataset (expected %s, got %s)",
                    ObjectType.DATA, outputObject.getObjectType()));
        }

        return checkDataSchema(outputObject.getData(), requiredSchema.getSchema(), ctx);
    }

    private static ValidationContext checkDataSchema(DataDefinition suppliedData, SchemaDefinition requiredSchema, ValidationContext ctx) {

        var suppliedSchema = findSchema(suppliedData, ctx.getMetadataBundle());

        if (suppliedSchema.getSchemaType() != requiredSchema.getSchemaType()) {
            return ctx.error(String.format(
                    "The dataset supplied has the wrong schema type (expected [%s], got [%s])",
                    requiredSchema.getSchemaType(),
                    suppliedSchema.getSchemaType()));
        }

        if (requiredSchema.getSchemaType() != SchemaType.TABLE)
            throw new ETracInternal("Schema type " + requiredSchema.getSchemaType() + " is not supported");

        return checkTableSchema(suppliedSchema.getTable(), requiredSchema.getTable(), ctx);
    }

    private static ValidationContext checkTableSchema(TableSchema suppliedSchema, TableSchema requiredSchema, ValidationContext ctx) {

        // Mapping of supplied fields by lower case name
        var suppliedMapping = new HashMap<String, FieldSchema>(suppliedSchema.getFieldsCount());

        for (var fieldIndex = 0; fieldIndex < suppliedSchema.getFieldsCount(); fieldIndex++) {
            var field = suppliedSchema.getFields(fieldIndex);
            suppliedMapping.put(field.getFieldName().toLowerCase(), field);
        }

        for (var requiredField : requiredSchema.getFieldsList()) {

            var fieldName = requiredField.getFieldName();
            var suppliedField = suppliedMapping.get(fieldName.toLowerCase());

            if (suppliedField == null)
                ctx = ctx.error(String.format("Field [%s] is not available in the supplied dataset", fieldName));
            else
                ctx = checkFieldSchema(suppliedField, requiredField, ctx);
        }

        return ctx;
    }

    private static ValidationContext checkFieldSchema(FieldSchema suppliedField, FieldSchema requiredField, ValidationContext ctx) {

        if (requiredField.getFieldType() != suppliedField.getFieldType()) {
            return ctx.error(String.format(
                    "Field [%s] has the wrong type in the supplied dataset (expected %s, got %s)",
                    requiredField.getFieldName(), requiredField.getFieldType(), suppliedField.getFieldType()));
        }

        if (requiredField.getBusinessKey() && !suppliedField.getBusinessKey()) {
            return ctx.error(String.format(
                    "Field [%s] should be a business key, but is not a business key in the supplied dataset",
                    requiredField.getFieldName()));
        }

        // Business keys are implicitly not null if the flag is not specified (currently notNull is optional)
        var suppliedNNotNull = suppliedField.hasNotNull() ? suppliedField.getNotNull() : suppliedField.getBusinessKey();

        if (requiredField.getNotNull() && !suppliedNNotNull) {
            return ctx.error(String.format(
                    "Field [%s] should not be nullable, but is nullable in the supplied dataset",
                    requiredField.getFieldName()));
        }

        if (requiredField.getCategorical() && !suppliedField.getCategorical()) {
            return ctx.error(String.format(
                    "Field [%s] should not be categorical, but is not categorical in the supplied dataset",
                    requiredField.getFieldName()));
        }

        return ctx;
    }

    private static SchemaDefinition findSchema(DataDefinition dataset, MetadataBundle resources) {

        if (dataset.hasSchema())
            return dataset.getSchema();

        if (dataset.hasSchemaId()) {

            var schema = resources.getResource(dataset.getSchemaId());

            // Metadata should be loaded before the validator runs (partial validation not available at present)
            if (schema == null)
                throw new ETracInternal("Metadata not available for validation");

            if (schema.getObjectType() == ObjectType.SCHEMA)
                return schema.getSchema();
        }

        // Should never happen - something has gone very wrong with object consistency!
        throw new EUnexpected();
    }
}
