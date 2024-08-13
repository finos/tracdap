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
import org.finos.tracdap.common.graph.*;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.MetadataUtil;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.common.validation.static_.CommonValidators;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.*;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.CONSISTENCY)
public class JobConsistencyValidator {

    private static final Descriptors.Descriptor JOB_DEFINITION;
    private static final Descriptors.OneofDescriptor JD_JOB_DETAILS;

    private static final Descriptors.Descriptor IMPORT_MODEL_JOB;
    private static final Descriptors.FieldDescriptor IMJ_LANGUAGE;
    private static final Descriptors.FieldDescriptor IMJ_REPOSITORY;

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
        JD_JOB_DETAILS = field(JOB_DEFINITION, JobDefinition.RUNMODEL_FIELD_NUMBER).getContainingOneof();

        IMPORT_MODEL_JOB = ImportModelJob.getDescriptor();
        IMJ_LANGUAGE = field(IMPORT_MODEL_JOB, ImportModelJob.LANGUAGE_FIELD_NUMBER);
        IMJ_REPOSITORY = field(IMPORT_MODEL_JOB, ImportModelJob.REPOSITORY_FIELD_NUMBER);

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
    public static ValidationContext importModelJob(ImportModelJob job, ValidationContext ctx) {

        ctx.push(IMJ_LANGUAGE)
                .apply(ModelConsistencyValidator::isSupportedLanguage)
                .pop();

        ctx.push(IMJ_REPOSITORY)
                .apply(ModelConsistencyValidator::isKnownModelRepo)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext runModelJob(RunModelJob job, ValidationContext ctx) {

        var metadata = ctx.getMetadataBundle();
        var modelObj = metadata.getResource(job.getModel());

        if (modelObj == null) {
            var message = "Required metadata is not available for [" + MetadataUtil.objectKey(job.getModel()) + "]";
            return ctx.push(RMJ_MODEL).error(message).pop();
        }

        var modelDef = modelObj.getModel();

        ctx.pushMap(RMJ_PARAMETERS, RunModelJob::getParametersMap)
                .apply(JobConsistencyValidator::runModelParameters, Map.class, modelDef.getParametersMap())
                .pop();

        ctx.pushMap(RMJ_INPUTS, RunModelJob::getInputsMap)
                .apply(JobConsistencyValidator::runModelInputs, Map.class, modelDef.getInputsMap())
                .pop();

        // Prior outputs are optional, however any provided must be valid
        ctx.pushMap(RMJ_PRIOR_OUTPUTS, RunModelJob::getPriorOutputsMap)
                .apply(JobConsistencyValidator::runModelPriorOutputs, Map.class, modelDef.getOutputsMap())
                .pop();

        // Do not validate final outputs at all
        // Consistency check is applied before outputs are populated
        ctx.pushMap(RMJ_OUTPUTS, RunModelJob::getOutputsMap)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext runFlowJob(RunFlowJob job, ValidationContext ctx) {

        ctx.push(RFJ_FLOW)
                .apply(CommonValidators::required)
                .apply(JobConsistencyValidator::flowAvailable, TagSelector.class)
                .pop();

        // If the flow is not available, do not attempt consistency validation
        if (ctx.failed())
            return ctx;

        // TODO: Build a meaningful namespace from the request context
        var namespace = NodeNamespace.ROOT;
        var builder = new GraphBuilder(namespace, ctx.getMetadataBundle(), graphErrorHandler(ctx));
        var graph = builder.buildRunFlowJob(job);

        ctx.pushMap(RFJ_MODELS, RunFlowJob::getModelsMap)
                .apply(JobConsistencyValidator::runFlowModels, Map.class, graph)
                .pop();

        ctx.pushMap(RFJ_PARAMETERS, RunFlowJob::getParametersMap)
                .apply(JobConsistencyValidator::runFlowParameters, Map.class, graph)
                .pop();

        ctx.pushMap(RFJ_INPUTS, RunFlowJob::getInputsMap)
                .apply(JobConsistencyValidator::runFlowInputs, Map.class, graph)
                .pop();

        ctx.pushMap(RFJ_PRIOR_OUTPUTS, RunFlowJob::getPriorOutputsMap)
                .apply(JobConsistencyValidator::runFlowPriorOutputs, Map.class, graph)
                .pop();

        // Do not validate final outputs at all
        // Consistency check is applied before outputs are populated
        ctx.pushMap(RFJ_OUTPUTS, RunFlowJob::getOutputsMap)
                .pop();

        return ctx;
    }

    private static ValidationContext flowAvailable(TagSelector flowSelector, ValidationContext ctx) {

        var flowObj = ctx.getMetadataBundle().getResource(flowSelector);

        if (flowObj == null)
            return ctx.error(String.format("Flow definition is not available for [%s]", MetadataUtil.objectKey(flowSelector)));

        if (flowObj.getObjectType() != ObjectType.FLOW)
            return ctx.error(String.format(
                    "Flow definition is the wrong object type (expected %s, got %s)",
                    ObjectType.FLOW.name(), flowObj.getObjectType().name()));

        return ctx;
    }


    // -----------------------------------------------------------------------------------------------------------------
    //   Validators for job-level collections
    // -----------------------------------------------------------------------------------------------------------------


    private static ValidationContext runFlowModels(Map<String, TagSelector> modelSelectors, GraphSection<NodeMetadata> graph, ValidationContext ctx) {

        var modelNodes = graph.nodes().values().stream()
                .filter(node -> node.payload().flowNode().getNodeType() == FlowNodeType.MODEL_NODE)
                .collect(Collectors.toMap(n -> n.nodeId().name(), n -> n));

        return alignedMapValidation(
                "model", (key, selector, node, ctx_) -> modelNode(key, selector, node, graph, ctx_), false,
                modelSelectors, modelNodes, ctx);
    }

    private static ValidationContext runFlowParameters(Map<String, Value> paramValues, GraphSection<NodeMetadata> graph, ValidationContext ctx) {

        var paramNodes = graph.nodes().values().stream()
                .filter(node -> node.payload().flowNode().getNodeType() == FlowNodeType.PARAMETER_NODE)
                .collect(Collectors.toMap(n -> n.nodeId().name(), n -> n));

        return alignedMapValidation(
                "parameter", JobConsistencyValidator::paramMatchesSchema, false,
                paramValues, paramNodes, ctx);
    }

    private static ValidationContext runModelParameters(Map<String, Value> paramValues, Map<String, ModelParameter> requiredParams, ValidationContext ctx) {

        return alignedMapValidation(
                "parameter", JobConsistencyValidator::paramMatchesSchema, false,
                paramValues, requiredParams, ctx);
    }

    private static ValidationContext runFlowInputs(Map<String, TagSelector> inputSelectors, GraphSection<NodeMetadata> graph, ValidationContext ctx) {

        var inputNodes = graph.nodes().values().stream()
                .filter(node -> node.payload().flowNode().getNodeType() == FlowNodeType.INPUT_NODE)
                .collect(Collectors.toMap(n -> n.nodeId().name(), n -> n));

        return alignedMapValidation(
                "input", JobConsistencyValidator::inputMatchesSchema, false,
                inputSelectors, inputNodes, ctx);
    }

    private static ValidationContext runModelInputs(Map<String, TagSelector> inputSelectors, Map<String, ModelInputSchema> requiredInputs, ValidationContext ctx) {

        return alignedMapValidation(
                "input", JobConsistencyValidator::inputMatchesSchema, false,
                inputSelectors, requiredInputs, ctx);
    }

    private static ValidationContext runFlowPriorOutputs(Map<String, TagSelector> priorOutputSelectors, GraphSection<NodeMetadata> graph, ValidationContext ctx) {

        var outputNodes = graph.nodes().values().stream()
                .filter(node -> node.payload().flowNode().getNodeType() == FlowNodeType.OUTPUT_NODE)
                .collect(Collectors.toMap(n -> n.nodeId().name(), n -> n));

        return alignedMapValidation(
                "output", JobConsistencyValidator::outputMatchesSchema, true,
                priorOutputSelectors, outputNodes, ctx);
    }

    private static ValidationContext runModelPriorOutputs(Map<String, TagSelector> priorOutputSelectors, Map<String, ModelOutputSchema> requiredOutputs, ValidationContext ctx) {

        return alignedMapValidation(
                "output", JobConsistencyValidator::outputMatchesSchema, true,
                priorOutputSelectors, requiredOutputs, ctx);
    }


    // -----------------------------------------------------------------------------------------------------------------
    //   Individual params, inputs and outputs
    // -----------------------------------------------------------------------------------------------------------------


    private static ValidationContext paramMatchesSchema(String paramName, Value paramValue, Node<NodeMetadata> paramNode, ValidationContext ctx) {

        // Do not attempt to check param type if type inference failed
        if (paramNode.payload().modelParameter() == null)
            return ctx.error("Type inference failed for parameter [" + paramName + "]");

        return paramMatchesSchema(paramName, paramValue, paramNode.payload().modelParameter(), ctx);
    }

    private static ValidationContext paramMatchesSchema(String paramName, Value paramValue, ModelParameter requiredParam, ValidationContext ctx) {

        var paramType = TypeSystem.descriptor(paramValue);
        var requiredType = requiredParam.getParamType();

        return paramMatchesType(paramName, paramType, requiredType, ctx);
    }

    private static ValidationContext paramMatchesSchema(String paramName, ModelParameter suppliedParam, ModelParameter requiredParam, ValidationContext ctx) {

        var paramType = suppliedParam.getParamType();
        var requiredType = requiredParam.getParamType();

        return paramMatchesType(paramName, paramType, requiredType, ctx);
    }

    private static ValidationContext paramMatchesType(String paramName, TypeDescriptor paramType, TypeDescriptor requiredType, ValidationContext ctx) {

        if (!paramType.equals(requiredType)) {
            if (paramType.getBasicType() != requiredType.getBasicType()) {
                return ctx.error(String.format(
                        "Parameter [%s] has the wrong type (expected %s, got %s)",
                        paramName, requiredType.getBasicType(), paramType.getBasicType()));
            }
            else {
                return ctx.error(String.format(
                        "Parameter [%s] has the wrong type (%s types, contents differ)",
                        paramName, requiredType.getBasicType()));
            }
        }

        return ctx;
    }

    private static ValidationContext inputMatchesSchema(String inputName, TagSelector inputSelector, Node<NodeMetadata> inputNode, ValidationContext ctx) {

        if (inputNode.payload().modelInputSchema() == null)
            ctx.error("Type inference failed for input [" + inputName + "]");

        // Inference failed, we can still check that the metadata is available
        return inputMatchesSchema(inputName, inputSelector, inputNode.payload().modelInputSchema(), ctx);
    }

    private static ValidationContext inputMatchesSchema(String inputName, TagSelector inputSelector, ModelInputSchema requiredSchema, ValidationContext ctx) {

        var inputObject = ctx.getMetadataBundle().getResource(inputSelector);

        if (inputObject == null) {
            return ctx.error(String.format(
                    "Metadata is not available for input [%s] (%s)",
                    inputName, MetadataUtil.objectKey(inputSelector)));
        }

        if (inputObject.getObjectType() != ObjectType.DATA) {
            return ctx.error(String.format(
                    "Input is not a dataset (expected %s, got %s)",
                    ObjectType.DATA, inputObject.getObjectType()));
        }

        // In case inference failed, requiredSchema == null so stop here
        if (ctx.failed())
            return ctx;

        return checkDataSchema(inputObject.getData(), requiredSchema.getSchema(), ctx);
    }

    private static ValidationContext inputMatchesSchema(String inputName, ModelInputSchema inputSchema, ModelInputSchema requiredSchema, ValidationContext ctx) {

        return checkDataSchema(inputSchema.getSchema(), requiredSchema.getSchema(), ctx);
    }

    private static ValidationContext inputMatchesSchema(String inputName, ModelOutputSchema outputSchema, ModelInputSchema requiredSchema, ValidationContext ctx) {

        return checkDataSchema(outputSchema.getSchema(), requiredSchema.getSchema(), ctx);
    }

    private static ValidationContext outputMatchesSchema(String outputName, TagSelector outputSelector, Node<NodeMetadata> outputNode, ValidationContext ctx) {

        if (outputNode.payload().modelOutputSchema() == null)
            ctx.error("Type inference failed for output [" + outputName + "]");

        return outputMatchesSchema(outputName, outputSelector, outputNode.payload().modelOutputSchema(), ctx);
    }

    private static ValidationContext outputMatchesSchema(String outputName, TagSelector outputSelector, ModelOutputSchema requiredSchema, ValidationContext ctx) {

        var outputObject = ctx.getMetadataBundle().getResource(outputSelector);

        if (outputObject == null) {
            return ctx.error(String.format(
                    "Metadata is not available for output [%s] (%s)",
                    outputName, MetadataUtil.objectKey(outputSelector)));
        }

        if (outputObject.getObjectType() != ObjectType.DATA) {
            return ctx.error(String.format(
                    "Output is not a dataset (expected %s, got %s)",
                    ObjectType.DATA, outputObject.getObjectType()));
        }

        // In case inference failed, requiredSchema == null so stop here
        if (ctx.failed())
            return ctx;

        return checkDataSchema(outputObject.getData(), requiredSchema.getSchema(), ctx);
    }

    private static ValidationContext checkDataSchema(DataDefinition suppliedData, SchemaDefinition requiredSchema, ValidationContext ctx) {

        var suppliedSchema = findSchema(suppliedData, ctx.getMetadataBundle());

        return checkDataSchema(suppliedSchema, requiredSchema, ctx);
    }

    private static ValidationContext checkDataSchema(SchemaDefinition suppliedSchema, SchemaDefinition requiredSchema, ValidationContext ctx) {

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


    // -----------------------------------------------------------------------------------------------------------------
    //   Individual flow model nodes
    // -----------------------------------------------------------------------------------------------------------------


    private static ValidationContext modelNode(String modelKey, TagSelector modelSelector, Node<NodeMetadata> node, GraphSection<NodeMetadata> graph, ValidationContext ctx) {

        var modelObj = ctx.getMetadataBundle().getResource(modelSelector);

        // Metadata should be loaded before the validator runs (partial validation not available at present)
        if (modelObj == null)
            return ctx.error("No model provided for [" + modelKey + "]");

        if (modelObj.getObjectType() != ObjectType.MODEL)
            return ctx.error("Object provided for [" + modelKey + "] is not a model");

        var modelDef = modelObj.getModel();
        var nodeMetadata = node.payload();

        // Check the model keys for params / inputs / outputs match the structure of the flow node
        // Parameters should have been autowired if they were not declared explicitly

        var paramsCheck = compareKeys(nodeMetadata.flowNode().getParametersList(), modelDef.getParametersMap().keySet());
        var inputsCheck = compareKeys(nodeMetadata.flowNode().getInputsList(), modelDef.getInputsMap().keySet());
        var outputsCheck = compareKeys(nodeMetadata.flowNode().getOutputsList(), modelDef.getOutputsMap().keySet());

        if (paramsCheck.anyErrors() || inputsCheck.anyErrors() || outputsCheck.anyErrors()) {

            // TODO: Allow details to be recorded separately for more readable error messages

            var details = new ArrayList<String>();
            modelNodeKeyErrors("missing parameters: ", paramsCheck.missingKeys, details);
            modelNodeKeyErrors("extra parameters: ", paramsCheck.extraKeys, details);
            modelNodeKeyErrors("missing inputs: ", inputsCheck.missingKeys, details);
            modelNodeKeyErrors("extra inputs: ", inputsCheck.extraKeys, details);
            modelNodeKeyErrors("missing outputs: ", outputsCheck.missingKeys, details);
            modelNodeKeyErrors("extra outputs: ", outputsCheck.extraKeys, details);

            var message = "Model is not compatible with the flow (" + String.join(", ", details) + ")";
            return ctx.error(message);
        }

        // If the structure matches, check params / inputs match what is supplied in this job
        // No need to check outputs - these will be checked by the consuming nodes (if they are used)

        for (var param : modelDef.getParametersMap().entrySet())
            ctx = JobConsistencyValidator.modelParameter(node, graph, param.getKey(), param.getValue(), ctx);

        for (var input : modelDef.getInputsMap().entrySet())
            ctx = JobConsistencyValidator.modelInput(node, graph, input.getKey(), input.getValue(), ctx);

        return ctx;
    }

    private static void modelNodeKeyErrors(String prefix, List<String> keys, List<String> details) {

        if (!keys.isEmpty()) {
            var detail = prefix + "[" + String.join(", ", keys) + "]";
            details.add(detail);
        }
    }

    private static ValidationContext modelParameter(
            Node<NodeMetadata> node, GraphSection<NodeMetadata> graph,
            String paramName, ModelParameter modelParameter,
            ValidationContext ctx) {

        var sourceSocket = node.dependencies().get(paramName);

        if (sourceSocket == null)
            return ctx.error(String.format("Parameter [%s] is not connected in the flow", paramName));

        var sourceNode = graph.nodes().get(sourceSocket.nodeId());
        var sourceMetadata = sourceNode.payload();
        var sourceNodeName = sourceSocket.nodeId().name();
        var sourceNodeType = sourceMetadata.flowNode().getNodeType();

        if (sourceNodeType == FlowNodeType.PARAMETER_NODE) {

            if (sourceMetadata.modelParameter() == null)
                return ctx.error(String.format("No type information available for connected parameter [%s]", sourceNodeName));

            return paramMatchesSchema(paramName, sourceMetadata.modelParameter(), modelParameter, ctx);
        }
        else {
            return ctx.error(String.format(
                    "Parameter [%s] cannot be supplied from [%s] (%s)",
                    paramName, sourceNodeName, sourceNodeType));
        }
    }

    private static ValidationContext modelInput(
            Node<NodeMetadata> node, GraphSection<NodeMetadata> graph,
            String inputName, ModelInputSchema modelInput,
            ValidationContext ctx) {

        var sourceSocket = node.dependencies().get(inputName);

        if (sourceSocket == null)
            return ctx.error(String.format("Input [%s] is not connected in the flow", inputName));

        var sourceNode = graph.nodes().get(sourceSocket.nodeId());
        var sourceMetadata = sourceNode.payload();
        var sourceNodeName = sourceSocket.nodeId().name();
        var sourceNodeType = sourceMetadata.flowNode().getNodeType();

        if (sourceNodeType == FlowNodeType.INPUT_NODE) {

            if (sourceMetadata.modelInputSchema() == null)
                return ctx.error(String.format("No schema available for connected input [%s]", sourceNodeName));

            return inputMatchesSchema(inputName, sourceMetadata.modelInputSchema(), modelInput, ctx);
        }
        else if (sourceNodeType == FlowNodeType.MODEL_NODE) {

            if (sourceMetadata.runtimeObjectType() != ObjectType.MODEL)
                return ctx.error(String.format("No metadata available for connected model [%s]", sourceNodeName));

            var sourceModel = sourceMetadata.runtimeObject().getModel();

            if (!sourceModel.containsOutputs(sourceSocket.socket()))
                return ctx.error(String.format("Connected model [%s] has no output named [%s]", sourceNodeName, sourceSocket.socket()));

            return inputMatchesSchema(inputName, sourceModel.getOutputsOrThrow(sourceSocket.socket()), modelInput, ctx);
        }
        else {
            return ctx.error(String.format(
                    "Input [%s] cannot be supplied from [%s] (%s)",
                    inputName, sourceNodeName, sourceNodeType));
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    //   Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private static <T, U> ValidationContext alignedMapValidation(
            String itemType, AlignedMapValidator<T, U> validatorFunc, boolean allowMissing,
            Map<String, T> providedValues, Map<String, U> requiredValues,
            ValidationContext ctx) {

        for (var provided : providedValues.entrySet()) {

            var itemKey = provided.getKey();
            var providedValue = provided.getValue();
            var requiredValue = requiredValues.get(provided.getKey());

            ctx = ctx.pushMapKey(itemKey);

            if (requiredValue == null) {
                if (providedValue != null)
                    ctx = ctx.error(String.format("Unexpected %s [%s]", itemType, itemKey));
            }
            else if (providedValue == null) {
                ctx = ctx.error(String.format("Missing required %s [%s]", itemType, itemKey));
            }
            else {
                ctx = validatorFunc.validate(itemKey, providedValue, requiredValue, ctx);
            }

            ctx = ctx.pop();
        }

        if (!allowMissing) {
            for (var requiredKey : requiredValues.keySet()) {
                if (!providedValues.containsKey(requiredKey))
                    ctx = ctx.error(String.format("Missing required %s [%s]", itemType, requiredKey));
            }
        }

        return ctx;
    }

    @FunctionalInterface
    private interface AlignedMapValidator<T, U> {
        ValidationContext validate(String key, T providedValue, U requiredValue, ValidationContext ctx);
    }

    private static KeyCheckResult compareKeys(Collection<String> expectedKeys, Collection<String> actualKeys) {

        var missingKeys = new ArrayList<String>();
        var extraKeys = new ArrayList<String>();

        for (var key : expectedKeys)
            if (!actualKeys.contains(key))
                missingKeys.add(key);

        for (var key : actualKeys)
            if (!expectedKeys.contains(key))
                extraKeys.add(key);

        return new KeyCheckResult(missingKeys, extraKeys);
    }

    private static class KeyCheckResult {

        final List<String> missingKeys;
        final List<String> extraKeys;

        public KeyCheckResult(List<String> missingKeys, List<String> extraKeys) {
            this.missingKeys = missingKeys;
            this.extraKeys = extraKeys;
        }

        public boolean anyErrors() {
            return !missingKeys.isEmpty() || !extraKeys.isEmpty();
        }
    }

    private static GraphBuilder.ErrorHandler graphErrorHandler(ValidationContext ctx) {

        return (nodeId, detail) -> graphErrorHandler(detail, ctx);
    }

    private static void graphErrorHandler(String detail, ValidationContext ctx) {

        ctx.error(detail);
    }
}
