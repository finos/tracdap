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

package org.finos.tracdap.common.graph;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.common.metadata.ResourceBundle;
import org.finos.tracdap.metadata.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class GraphBuilder {

    @FunctionalInterface
    public interface ErrorHandler {
        void error(NodeId nodeId, String detail);
    }

    private static final ErrorHandler DEFAULT_ERROR_HANDLER = new DefaultErrorHandler();
    private static final Logger log = LoggerFactory.getLogger(GraphBuilder.class);

    private static final String MODEL_NODE_NAME = "trac_model";
    private static final Map<String, SocketId> NO_DEPENDENCIES = Map.of();
    private static final List<String> NO_OUTPUTS = List.of();
    private static final List<String> SINGLE_OUTPUT = List.of(SocketId.SINGLE_OUTPUT);
    private static final String SINGLE_INPUT = SocketId.SINGLE_INPUT;

    private final NodeNamespace namespace;
    private final MetadataBundle metadataBundle;
    private final ResourceBundle resourceBundle;
    private final ErrorHandler errorHandler;

    public GraphBuilder(
            NodeNamespace namespace,
            MetadataBundle metadataBundle,
            ResourceBundle resourceBundle,
            ErrorHandler errorHandler) {

        this.namespace = namespace;
        this.metadataBundle = metadataBundle;
        this.resourceBundle = resourceBundle;
        this.errorHandler = errorHandler;
    }

    public GraphBuilder(NodeNamespace namespace, MetadataBundle metadataBundle, ResourceBundle resourceBundle) {
        this(namespace, metadataBundle, resourceBundle, DEFAULT_ERROR_HANDLER);
    }

    public GraphSection<NodeMetadata> buildJob(JobDefinition job) {

        if (job.getJobType() != JobType.RUN_FLOW)
            throw new ETracInternal("Graph building is only supported for RUN_FLOW jobs");

        return buildRunFlowJob(job.getRunFlow());
    }

    public GraphSection<NodeMetadata> buildRunFlowJob(RunFlowJob job) {

        if (metadataBundle == null || resourceBundle == null)
            throw new ETracInternal("Metadata and resource bundles are needed to build a job graph");

        var flowObj = metadataBundle.getObject(job.getFlow());

        if (flowObj == null || flowObj.getObjectType() != ObjectType.FLOW)
            throw new ETracInternal("Metadata bundle does not contain the flow object");

        var flow = flowObj.getFlow();

        var flowGraph = buildFlow(flow);
        var jobGraph = addJobMetadata(flowGraph, job);
        var paramsGraph = autowireFlowParameters(jobGraph, flow, job);

        return applyTypeInference(paramsGraph);
    }

    // This is assuming the flow has already been through the FlowDefinition static validator
    // Ideally that logic should move here and be used in JobConsistencyValidator and FlowConsistencyValidator
    // More work would be needed to report errors with the correct location in ValidationContext

    public GraphSection<NodeMetadata> buildFlow(FlowDefinition flow) {

        // https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

        var nodeIds = flow.getNodesMap().keySet().stream()
                .map(node -> new NodeId(node, namespace))
                .collect(Collectors.toMap(NodeId::name, Function.identity()));

        var edgesBySource = new HashMap<String, List<FlowEdge>>();
        var edgesByTarget = new HashMap<String, List<FlowEdge>>();

        for (var edge : flow.getEdgesList()) {

            // Do not add edges with invalid connections
            if (!checkEdgeConnection(edge, flow.getNodesMap()))
                continue;

            var sourceNode = edge.getSource().getNode();
            var targetNode = edge.getTarget().getNode();

            if (!edgesBySource.containsKey(sourceNode))
                edgesBySource.put(sourceNode, new ArrayList<>());

            if (!edgesByTarget.containsKey(targetNode))
                edgesByTarget.put(targetNode, new ArrayList<>());

            edgesBySource.get(sourceNode).add(edge);
            edgesByTarget.get(targetNode).add(edge);
        }

        // Initial set of reachable flow nodes is just the input nodes
        var remainingNodes = new HashMap<>(flow.getNodesMap());
        var reachableNodes = new HashMap<String, FlowNode>();

        for (var node : flow.getNodesMap().entrySet()) {
            if (node.getValue().getNodeType() == FlowNodeType.INPUT_NODE ||
                node.getValue().getNodeType() == FlowNodeType.PARAMETER_NODE ||
                node.getValue().getNodeType() == FlowNodeType.RESOURCE_NODE) {

                reachableNodes.put(node.getKey(), node.getValue());
                remainingNodes.remove(node.getKey());
            }
        }

        // Graph nodes to build during traversal
        var graphNodes = new HashMap<NodeId, Node<NodeMetadata>>();
        var graphEdges = new HashMap<NodeId, Map<String, SocketId>>();

        while (!reachableNodes.isEmpty()) {

            var nextNode = reachableNodes.keySet().stream().findAny();
            var nodeName = nextNode.get();
            var flowNode = reachableNodes.remove(nodeName);

            var nodeId = nodeIds.get(nodeName);
            var dependencies = graphEdges.getOrDefault(nodeId, NO_DEPENDENCIES);
            var graphNode = buildFlowNode(nodeId, dependencies, flowNode, flow);

            graphNodes.put(graphNode.nodeId(), graphNode);

            var sourceEdges = edgesBySource.get(nodeName);

            // Some nodes do not feed any other nodes, e.g. output nodes, or if there are errors in the flow
            if (sourceEdges == null)
                continue;

            for (var edge : sourceEdges) {

                addGraphEdge(graphEdges, edge, nodeIds);

                var targetNodeName = edge.getTarget().getNode();
                var targetEdges = edgesByTarget.get(targetNodeName);

                // Remove just this one inbound edge from the list of inbound edges
                targetEdges.remove(edge);

                // Target nodes is reachable when there are no inbound edges left
                if (targetEdges.isEmpty()) {
                    var targetNode = remainingNodes.remove(targetNodeName);
                    reachableNodes.put(targetNodeName, targetNode);
                }
            }
        }

        // Once the traversal is complete, add validation errors for any nodes that could not be reached

        for (var node : remainingNodes.keySet()) {
            var nodeId = new NodeId(node, namespace);
            var msg = String.format("Flow node [%s] is not reachable (this may indicate a cyclic dependency)", node);
            errorHandler.error(nodeId, msg);
        }

        var inputs = graphNodes.values().stream()
                .filter(n -> n.dependencies().isEmpty())
                .map(Node::nodeId)
                .collect(Collectors.toUnmodifiableList());

        var outputs = graphNodes.values().stream()
                .filter(n -> n.outputs().isEmpty())
                .map(Node::nodeId)
                .collect(Collectors.toUnmodifiableList());

        return new GraphSection<>(graphNodes, inputs, outputs);
    }

    private Node<NodeMetadata> buildFlowNode(NodeId nodeId, Map<String, SocketId> dependencies, FlowNode flowNode, FlowDefinition flow) {

        // Look up strict metadata if it is present in the flow
        var modelParam = flowNode.getNodeType() == FlowNodeType.PARAMETER_NODE ? flow.getParametersOrDefault(nodeId.name(), null) : null;
        var modelInput = flowNode.getNodeType() == FlowNodeType.INPUT_NODE ? flow.getInputsOrDefault(nodeId.name(), null) : null;
        var modelOutput = flowNode.getNodeType() == FlowNodeType.OUTPUT_NODE ? flow.getOutputsOrDefault(nodeId.name(), null) : null;
        var modelResource = flowNode.getNodeType() == FlowNodeType.RESOURCE_NODE ? flow.getResourcesOrDefault(nodeId.name(), null) : null;

        // Runtime object / value is not part of the flow, these will always be null
        var nodeMetadata = new NodeMetadata(flowNode, modelParam, modelInput, modelOutput, modelResource, null, null);

        // Check and report any unsatisfied dependencies
        // The dependencies that are present are already validated
        var missing = missingDependencies(dependencies, flowNode);
        for (var dependency : missing) {
            var socketName = dependency.equals(SINGLE_INPUT) ? nodeId.name() : nodeId.name() + "." + dependency;
            errorHandler.error(nodeId, String.format("Socket [%s] is not supplied by any edge", socketName));
        }

        switch (flowNode.getNodeType()) {

            case PARAMETER_NODE:
            case INPUT_NODE:
            case RESOURCE_NODE:
                return new Node<>(nodeId, dependencies, SINGLE_OUTPUT, nodeMetadata);

            case OUTPUT_NODE:
                return new Node<>(nodeId, dependencies,  NO_OUTPUTS, nodeMetadata);

            case MODEL_NODE:
                return new Node<>(nodeId, dependencies, flowNode.getOutputsList(), nodeMetadata);

            default:
                errorHandler.error(nodeId, String.format("Missing or invalid node type [%s]", flowNode.getNodeType()));
                return new Node<>(nodeId, NO_DEPENDENCIES, NO_OUTPUTS, nodeMetadata);
        }
    }

    private void addGraphEdge(Map<NodeId, Map<String, SocketId>> graphEdges, FlowEdge flowEdge, Map<String, NodeId> nodeIds) {

        var targetNode = nodeIds.get(flowEdge.getTarget().getNode());
        var targetSocket = flowEdge.getTarget().getSocket();

        // Add a dependency map for the target if it doesn't already exist
        var targetEdges = graphEdges.computeIfAbsent(targetNode, x -> new HashMap<>());

        if (targetEdges.containsKey(targetSocket)) {
            var socketName = targetSocket.equals(SINGLE_INPUT) ? targetNode.name() : targetNode.name() + "." + targetSocket;
            errorHandler.error(targetNode, String.format("Target [%s] is supplied by multiple edges", socketName));
            return;
        }

        var sourceNode = nodeIds.get(flowEdge.getSource().getNode());
        var sourceSocket = new SocketId(sourceNode, flowEdge.getSource().getSocket());

        targetEdges.put(targetSocket, sourceSocket);
    }

    private List<String> missingDependencies(Map<String, SocketId> dependencies, FlowNode flowNode) {

        if (dependencies.size() == flowNode.getParametersCount() + flowNode.getInputsCount() +  flowNode.getResourcesCount())
            return NO_OUTPUTS;

        var missing = new ArrayList<String>();

        for (var param : flowNode.getParametersList())
            if (!dependencies.containsKey(param))
                missing.add(param);

        for (var input : flowNode.getInputsList())
            if (!dependencies.containsKey(input))
                missing.add(input);

        for (var resource : flowNode.getResourcesList())
            if (!dependencies.containsKey(resource))
                missing.add(resource);

        return missing;
    }

    private boolean checkEdgeConnection(FlowEdge edge, Map<String, FlowNode> nodes) {

        // This check is already handled in FlowValidator
        // Ideally that logic should move here

        return true;
    }

    public GraphSection<NodeMetadata> addJobMetadata(GraphSection<NodeMetadata> graph, RunModelJob job) {

        var params = job.getParametersMap();
        var inputs = job.getInputsMap();
        var outputs = job.getPriorOutputsMap();
        var models = Map.of(MODEL_NODE_NAME, job.getModel());
        var resources = job.getResourcesMap();

        return addJobMetadata(graph, params, inputs, outputs, models, resources);
    }

    public GraphSection<NodeMetadata> addJobMetadata(GraphSection<NodeMetadata> graph, RunFlowJob job) {

        var params = job.getParametersMap();
        var inputs = job.getInputsMap();
        var outputs = job.getPriorOutputsMap();
        var models = job.getModelsMap();
        var resources = job.getResourcesMap();

        return addJobMetadata(graph, params, inputs, outputs, models, resources);
    }

    public GraphSection<NodeMetadata> addJobMetadata(
            GraphSection<NodeMetadata> graph,
            Map<String, Value> params,
            Map<String, TagSelector> inputs,
            Map<String, TagSelector> outputs,
            Map<String, TagSelector> models,
            Map<String, String> resources) {

        if (metadataBundle == null)
            throw new ETracInternal("No metadata bundle supplied, job metadata cannot be added to the graph");

        var updatedNodes= new HashMap<>(graph.nodes());

        // Iterate original graph, update the
        for (var node : graph.nodes().values()) {

            var flowNode = node.payload().flowNode();

            switch (flowNode.getNodeType()) {

                case PARAMETER_NODE:
                    var paramNode = addNRuntimeValue(node, params);
                    updatedNodes.put(node.nodeId(), paramNode);
                    break;

                case INPUT_NODE:
                    var inputNode = addRuntimeObject(node, inputs);
                    updatedNodes.put(node.nodeId(), inputNode);
                    break;

                case OUTPUT_NODE:
                    var outputNode = addRuntimeObject(node, outputs);
                    updatedNodes.put(node.nodeId(), outputNode);
                    break;

                case MODEL_NODE:
                    var modelNode = addRuntimeObject(node, models);
                    updatedNodes.put(node.nodeId(), modelNode);
                    break;

                case RESOURCE_NODE:
                    var resourceNode = addRuntimeResource(node, resources);
                    updatedNodes.put(node.nodeId(), resourceNode);
                    break;

                default:
                    throw new EUnexpected();
            }
        }

        return new GraphSection<>(updatedNodes, graph.inputs(), graph.outputs());
    }

    private Node<NodeMetadata> addNRuntimeValue(Node<NodeMetadata> node, Map<String, Value> runtimeValues) {

        var runtimeValue = runtimeValues.get(node.nodeId().name());

        if (runtimeValue == null)
            return node;

        var metadata = node.payload().withRuntimeValue(runtimeValue);
        return node.withPayload(metadata);
    }

    private Node<NodeMetadata> addRuntimeObject(Node<NodeMetadata> node, Map<String, TagSelector> runtimeObjects) {

        var runtimeSelector = runtimeObjects.get(node.nodeId().name());

        if (runtimeSelector == null)
            return node;

        var runtimeObject = metadataBundle.getObject(runtimeSelector);

        if (runtimeObject == null)
            return node;

        var metadata = node.payload().withRuntimeObject(runtimeObject);
        return node.withPayload(metadata);
    }

    private Node<NodeMetadata> addRuntimeResource(Node<NodeMetadata> node, Map<String, String> runtimeResources) {

        var resourceName = runtimeResources.get(node.nodeId().name());

        if (resourceName == null)
            return node;

        var runtimeResource = resourceBundle.getResource(resourceName);

        if (runtimeResource == null)
            return node;

        var runtimeObject = ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.RESOURCE)
                .setResource(runtimeResource)
                .build();

        var metadata = node.payload().withRuntimeObject(runtimeObject);
        return node.withPayload(metadata);
    }

    public GraphSection<NodeMetadata> autowireFlowParameters(GraphSection<NodeMetadata> graph, FlowDefinition flow, RunFlowJob job) {

        // Check whether this flow is using explicit parameters
        var flowIsExplicit = flow.getParametersCount() > 0;

        var nodes = new HashMap<>(graph.nodes());
        var paramIds = new HashMap<String, NodeId>();

        for (var node : graph.nodes().values()) {

            var nodeMetadata = node.payload();

            if (nodeMetadata.runtimeObjectType() != ObjectType.MODEL)
                continue;

            var flowNode = nodeMetadata.flowNode().toBuilder();
            var dependencies = new HashMap<>(node.dependencies());
            var modelDef = nodeMetadata.runtimeObject().getModel();

            // Look at parameters in the model def
            // If they are missing, add them to the flow node with dependencies
            // Also add the flow-level parameter node if that is missing

            for (var paramName : modelDef.getParametersMap().keySet()) {

                // Do not auto-wire parameters that are declared explicitly in the node
                if (flowNode.getParametersList().contains(paramName))
                    continue;

                flowNode.addParameters(paramName);

                // For explicit flows, do not try to auto-wire parameters that are not declared
                if (flowIsExplicit && ! flow.containsParameters(paramName)) {
                    errorHandler.error(node.nodeId(), String.format("Parameter [%s] is not declared in the flow", paramName));
                    continue;
                }

                var paramId = paramIds.computeIfAbsent(paramName, pn -> new NodeId(pn, namespace));

                if (!nodes.containsKey(paramId)) {
                    var paramNode = buildParameterNode(paramId, flow, job);
                    nodes.put(paramId, paramNode);
                }

                if (!dependencies.containsKey(paramName)) {

                    var socketId = new SocketId(paramId, SINGLE_INPUT);
                    dependencies.put(paramName, socketId);
                }
            }

            var updatedMetadata = nodeMetadata
                    .withFlowNode(flowNode.build());

            var updatedNode = node
                    .withDependencies(dependencies)
                    .withPayload(updatedMetadata);

            nodes.put(node.nodeId(), updatedNode);
        }

        return new GraphSection<>(nodes, graph.inputs(), graph.outputs());
    }

    private Node<NodeMetadata> buildParameterNode(NodeId paramId, FlowDefinition flow, RunFlowJob job) {

        var flowNode = FlowNode.newBuilder()
                .setNodeType(FlowNodeType.PARAMETER_NODE)
                .build();

        var paramName = paramId.name();
        var modelParameter = flow.getParametersOrDefault(paramName, null);
        var runtimeValue = job.getParametersOrDefault(paramName, null);
        var nodeMetadata = new NodeMetadata(flowNode, modelParameter, null, null, null, null, runtimeValue);

        return new Node<>(paramId, NO_DEPENDENCIES, SINGLE_OUTPUT, nodeMetadata);
    }

    public GraphSection<NodeMetadata> applyTypeInference(GraphSection<NodeMetadata> graph) {

        var dependents = new HashMap<NodeId, List<SocketId>>();

        for (var node : graph.nodes().entrySet()) {
            for (var dep : node.getValue().dependencies().entrySet()) {

                var sourceId = dep.getValue().nodeId();
                var targetId = new SocketId(node.getKey(), dep.getKey());

                if (!dependents.containsKey(sourceId))
                    dependents.put(sourceId, new ArrayList<>());

                dependents.get(sourceId).add(targetId);
            }
        }

        var nodes = new HashMap<>(graph.nodes());

        for (var node : nodes.values()) {

            var nodeMetadata = node.payload();

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.PARAMETER_NODE && nodeMetadata.modelParameter() == null) {

                var targets = dependents.getOrDefault(node.nodeId(), List.of());
                var parameter = inferParameter(node.nodeId(), targets, graph);
                var inferredMetadata = nodeMetadata.withModelParameter(parameter);
                var inferredNode = node.withPayload(inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.INPUT_NODE && nodeMetadata.modelInputSchema() == null) {

                var targets = dependents.getOrDefault(node.nodeId(), List.of());
                var inputSchema = inferInputSchema(node.nodeId(), targets, graph);
                var inferredMetadata = nodeMetadata.withModelInputSchema(inputSchema);
                var inferredNode = node.withPayload(inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.OUTPUT_NODE && nodeMetadata.modelOutputSchema() == null) {

                if (node.dependencies().size() != 1)
                    continue;

                var source = node.dependencies().values().iterator().next();
                var outputSchema = inferOutputSchema(source, graph);
                var inferredMetadata = nodeMetadata.withModelOutputSchema(outputSchema);
                var inferredNode = node.withPayload(inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.RESOURCE_NODE && nodeMetadata.modelResource() == null) {

                var targets = dependents.getOrDefault(node.nodeId(), List.of());
                var resource = inferResource(node.nodeId(), targets, graph);
                var inferredMetadata = nodeMetadata.withModelResource(resource);
                var inferredNode = node.withPayload(inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }
        }

        return new GraphSection<>(nodes, graph.inputs(), graph.outputs());
    }


    private ModelParameter inferParameter(NodeId paramId, List<SocketId> targets, GraphSection<NodeMetadata> graph) {

        var targetParams = new ArrayList<Map.Entry<SocketId, ModelParameter>>(targets.size());

        for (var target : targets) {

            var targetNode = graph.nodes().get(target.nodeId());

            if (targetNode == null || targetNode.payload().runtimeObjectType() != ObjectType.MODEL)
                continue;

            var targetModel = targetNode.payload().runtimeObject().getModel();

            if (targetModel.containsParameters(target.socket())) {
                var modelParam = targetModel.getParametersOrThrow(target.socket());
                targetParams.add(Map.entry(target, modelParam));
            }
        }

        // Parameter not used, type cannot be inferred (but is probably not needed)
        if(targetParams.isEmpty())
            return null;

        // Parameter only used once, infer an exact match to the target
        if (targetParams.size() == 1)
            return targetParams.get(0).getValue();

        var param = targetParams.get(0).getValue().toBuilder();
        var paramTarget = targetParams.get(0).getKey();

        for (int i = 1; i < targetParams.size(); i++) {

            var nextParam = targetParams.get(i).getValue();
            var nextParamTarget = targetParams.get(i).getKey();

            // Type differs between targets, then type cannot be inferred
            if (!nextParam.getParamType().equals(param.getParamType())) {

                var message = String.format("Parameter is ambiguous for [%s]: Types are different for [%s.%s] and [%s.%s]",
                        paramId.name(),
                        paramTarget.nodeId().name(), paramTarget.socket(),
                        nextParamTarget.nodeId().name(), nextParamTarget.socket());

                errorHandler.error(paramId, message);

                return null;
            }

            // Default value differs between targets, type can still be inferred but default value cannot
            if (!nextParam.hasDefaultValue() || !nextParam.getDefaultValue().equals(param.getDefaultValue()))
                param.clearDefaultValue();
        }

        return param.build();
    }

    private ModelInputSchema inferInputSchema(NodeId inputId, List<SocketId> targets, GraphSection<NodeMetadata> graph) {

        var targetInputs = new ArrayList<Map.Entry<SocketId, ModelInputSchema>>(targets.size());

        for (var target : targets) {

            var targetNode = graph.nodes().get(target.nodeId());

            if (targetNode == null || targetNode.payload().runtimeObjectType() != ObjectType.MODEL)
                continue;

            var targetModel = targetNode.payload().runtimeObject().getModel();

            if (targetModel.containsInputs(target.socket())) {
                var modelInput = targetModel.getInputsOrThrow(target.socket());
                targetInputs.add(Map.entry(target, modelInput));
            }
        }

        // Input not used, schema cannot be inferred (not needed, although there may be other consistency errors)
        if(targetInputs.isEmpty())
            return null;

        // Input used only once, infer schema is an exact match
        if (targetInputs.size() == 1)
            return targetInputs.get(0).getValue();

        var schema = targetInputs.get(0).getValue().toBuilder();
        var schemaTarget = targetInputs.get(0).getKey();

        for (int i = 1; i < targetInputs.size(); i++) {

            var nextSchema = targetInputs.get(i).getValue();
            var nextSchemaTarget = targetInputs.get(i).getKey();

            schema = combineInputSchema(inputId, schema, nextSchema);

            // If combination is not possible, schema cannot be inferred
            if (schema == null) {

                var message = String.format("Input is ambiguous for [%s]: Schemas are not compatible for [%s.%s] and [%s.%s]",
                        inputId.name(),
                        schemaTarget.nodeId().name(), schemaTarget.socket(),
                        nextSchemaTarget.nodeId().name(), nextSchemaTarget.socket());

                errorHandler.error(inputId, message);

                return null;
            }
        }

        return schema.build();
    }

    private ModelInputSchema.Builder combineInputSchema(NodeId nodeId, ModelInputSchema.Builder modelInput, ModelInputSchema nextModelInput) {

        // Combining optional and non-optional inputs is allowed, the result is non-optional
        if (!nextModelInput.getOptional())
            modelInput.setOptional(false);

        var schema = modelInput.getSchema();
        var nextSchema = nextModelInput.getSchema();

        if (schema.getSchemaType() != nextSchema.getSchemaType()) {
            errorHandler.error(nodeId, String.format(
                    "Cannot combine schema types %s and %s",
                    schema.getSchemaType().name(),
                    nextSchema.getSchemaType().name()));
            return null;
        }

        // If next schema is dynamic do not try to merge fields (there will be none)
        if (nextModelInput.getDynamic())
            return modelInput;

        // If the current schema is dynamic and the next is static, take the static requirements
        if (modelInput.getDynamic()) {
            modelInput.setDynamic(false);
            modelInput.setSchema(nextSchema);
            return modelInput;
        }

        var combinedSchema = combineSchema(nodeId, schema, nextSchema);
        return combinedSchema != null ? modelInput.setSchema(combinedSchema) : null;
    }

    private SchemaDefinition combineSchema(NodeId nodeId, SchemaDefinition schema, SchemaDefinition nextSchema) {

        if (schema.getSchemaType() != nextSchema.getSchemaType()) {
            errorHandler.error(nodeId, String.format(
                    "Cannot combine schema types %s and %s",
                    schema.getSchemaType().name(),
                    nextSchema.getSchemaType().name()));
            return null;
        }

        var combinedSchema = schema.toBuilder();

        for (var enumEntry : nextSchema.getNamedEnumsMap().entrySet()) {

            var namedEnumKey = enumEntry.getKey();
            var namedEnum = enumEntry.getValue();

            if (!combinedSchema.containsNamedEnums(namedEnumKey)) {
                combinedSchema.putNamedEnums(namedEnumKey, namedEnum);
            }
            else {
                var originalNamedEnum = schema.getNamedEnumsOrThrow(namedEnumKey);
                var combinedNamedEnum = combineEnumValues(nodeId, namedEnumKey, originalNamedEnum, namedEnum);
                if (combinedNamedEnum != null)
                    combinedSchema.putNamedEnums(namedEnumKey, combinedNamedEnum);
            }
        }

        for (var typeEntry : nextSchema.getNamedTypesMap().entrySet()) {

            var namedTypeKey = typeEntry.getKey();
            var namedType = typeEntry.getValue();

            if (!combinedSchema.containsNamedTypes(namedTypeKey)) {
                combinedSchema.putNamedTypes(namedTypeKey, namedType);
            }
            else {
                var originalNamedType = schema.getNamedTypesOrThrow(namedTypeKey);
                var combinedNamedType = combineSchema(nodeId,originalNamedType, namedType);
                if (combinedNamedType != null)
                    combinedSchema.putNamedTypes(namedTypeKey, combinedNamedType);
            }
        }

        if (schema.getSchemaType() == SchemaType.TABLE_SCHEMA) {

            var fieldList = schema.getTable().getFieldsList();
            var nextFieldList = nextSchema.getTable().getFieldsList();
            var combinedFieldList = combineFieldList(fieldList, nextFieldList);
            if (combinedFieldList == null)
                return null;
            var combinedTable = schema.getTable().toBuilder().clearFields().addAllFields(combinedFieldList);
            return schema.toBuilder().setTable(combinedTable).build();
        }
        else if (schema.getSchemaType() == SchemaType.STRUCT_SCHEMA) {

            var fieldList = schema.getStruct().getFieldsList();
            var nextFieldList = nextSchema.getStruct().getFieldsList();
            var combinedFieldList = combineFieldList(fieldList, nextFieldList);
            if (combinedFieldList == null)
                return null;
            var combinedStruct = schema.getStruct().toBuilder().clearFields().addAllFields(combinedFieldList);
            return schema.toBuilder().setStruct(combinedStruct).build();
        }
        else {

            // Unknown schema type
            throw new EUnexpected();
        }
    }

    private EnumValues combineEnumValues(NodeId nodeId, String namedEnumKey, EnumValues enumValues, EnumValues nextEnumValues) {

        // Combining two named enums is restrictive
        // Values must be valid in both enums to be included in the combined enum

        var combinedValues = enumValues.getValuesList();
        combinedValues.removeIf(value -> !nextEnumValues.getValuesList().contains(value));

        if (combinedValues.isEmpty()) {
            errorHandler.error(nodeId, String.format("Cannot combine named enums for %s (no common values)", namedEnumKey));
            return null;
        }

        return EnumValues.newBuilder().addAllValues(combinedValues).build();
    }

    private List<FieldSchema> combineFieldList(List<FieldSchema> fieldList, List<FieldSchema> nextFieldList) {

        var combinedFieldList = new ArrayList<>(fieldList);

        var fieldsMap = fieldList.stream()
                .collect(Collectors.toMap(f -> f.getFieldName().toLowerCase(), f -> f));

        for (var nextField : nextFieldList) {

            var field = fieldsMap.get(nextField.getFieldName().toLowerCase());

            if (field == null) {
                var nextFieldOrder = fieldsMap.size();
                combinedFieldList.add(nextField.toBuilder().setFieldOrder(nextFieldOrder).build());
                fieldsMap.put(
                        nextField.getFieldName().toLowerCase(),
                        combinedFieldList.get(nextFieldOrder));
            }
            else {
                var combinedField = combineFieldSchema(field, nextField);
                if (combinedField == null)
                    return null;
                combinedFieldList.set(combinedField.getFieldOrder(), combinedField);
                fieldsMap.put(nextField.getFieldName().toLowerCase(), combinedField);
            }
        }

        return combinedFieldList;
    }

    private FieldSchema combineFieldSchema(FieldSchema field, FieldSchema nextField) {

        // Field types must always match, otherwise they cannot be combined
        if (field.getFieldType() != nextField.getFieldType())
            return null;

        // Require categorical flag match - this could be relaxed so we take the stricter condition
        if (field.getCategorical() != nextField.getCategorical())
            return null;

        // Require business key flag match - this could be relaxed so we take the stricter condition
        if (field.getBusinessKey() != nextField.getBusinessKey())
            return null;

        // For the not null flag, take the stricter condition
        if (nextField.getNotNull() && ! field.getNotNull())
            return field.toBuilder().setNotNull(true).build();

        return field;
    }


    private ModelOutputSchema inferOutputSchema(SocketId source, GraphSection<NodeMetadata> graph) {

        var sourceNode = graph.nodes().get(source.nodeId());

        // Output not produced, schema cannot be inferred (there will be consistency errors anyway)
        if (sourceNode == null)
            return null;

        if (sourceNode.payload().runtimeObjectType() == ObjectType.MODEL) {

            var sourceModel = sourceNode.payload().runtimeObject().getModel();

            if (sourceModel.containsOutputs(source.socket()))
                return sourceModel.getOutputsOrThrow(source.socket());
            else
                return null;
        }

        if (sourceNode.payload().modelInputSchema() != null) {

            var inputSchema = sourceNode.payload().modelInputSchema();

            return ModelOutputSchema.newBuilder()
                    .setSchema(inputSchema.getSchema())
                    .setLabel(inputSchema.getLabel())
                    .build();
        }

        return null;
    }

    private ModelResource inferResource(NodeId resourceId, List<SocketId> targets, GraphSection<NodeMetadata> graph) {

        var targetResources = new ArrayList<Map.Entry<SocketId, ModelResource>>(targets.size());

        for (var target : targets) {

            var targetNode = graph.nodes().get(target.nodeId());

            if (targetNode == null || targetNode.payload().runtimeObjectType() != ObjectType.MODEL)
                continue;

            var targetModel = targetNode.payload().runtimeObject().getModel();

            if (targetModel.containsResources(target.socket())) {
                var modelResource = targetModel.getResourcesOrThrow(target.socket());
                targetResources.add(Map.entry(target, modelResource));
            }
        }

        // Resource not used (there may be consistency errors)
        if(targetResources.isEmpty())
            return null;

        // Resource only used once
        if (targetResources.size() == 1)
            return targetResources.get(0).getValue();

        var resource = targetResources.get(0).getValue().toBuilder();
        var resourceTarget = targetResources.get(0).getKey();

        for (int i = 1; i < targetResources.size(); i++) {

            var nextResource = targetResources.get(i).getValue();
            var nextResourceTarget = targetResources.get(i).getKey();

            resource = combineResource(resource, nextResource);

            if (resource == null) {

                var message = String.format("Resource is ambiguous for [%s]: Resources are not compatible for [%s.%s] and [%s.%s]",
                        resourceId.name(),
                        resourceTarget.nodeId().name(), resourceTarget.socket(),
                        nextResourceTarget.nodeId().name(), nextResourceTarget.socket());

                errorHandler.error(resourceId, message);

                return null;
            }
        }

        return resource.build();
    }

    private ModelResource.Builder combineResource(ModelResource.Builder resource, ModelResource nextResource) {

        if (resource.getResourceType() != nextResource.getResourceType()) {
            return null;  // Resources must be the same type
        }

        if (resource.hasProtocol() && nextResource.hasProtocol()) {
            if (!resource.getProtocol().equals(nextResource.getProtocol()))
                return null;
        }
        else if (nextResource.hasProtocol()) {
            resource.setProtocol(nextResource.getProtocol());
        }

        if (resource.hasSubProtocol() && nextResource.hasSubProtocol()) {
            if (!resource.getSubProtocol().equals(nextResource.getSubProtocol()))
                return null;
        }
        else if (nextResource.hasSubProtocol()) {
            resource.setSubProtocol(nextResource.getSubProtocol());
        }

        // If system details do not agree, just remove them from the top level model resource
        // So long as protocol / sub-protocol match, any supported client should be usable
        if (resource.hasSystem() && nextResource.hasSystem()) {

            var system =  resource.getSystem();
            var nextSystem = nextResource.getSystem();

            if (!system.equals(nextSystem)){
                resource.setSystem(ModelSystemDetails.newBuilder());
            }
        }
        else if (nextResource.hasSystem()) {
            resource.setSystem(nextResource.getSystem());
        }

        return resource;
    }

    public FlowDefinition exportFlow(GraphSection<NodeMetadata> graph) {

        var flow = FlowDefinition.newBuilder();

        for (var node : graph.nodes().values()) {

            var nodeName = node.nodeId().name();
            var metadata = node.payload();
            var flowNode = metadata.flowNode();

            flow.putNodes(nodeName, flowNode);

            for (var dependency : node.dependencies().entrySet()) {

                var sourceId = dependency.getValue();
                var source = FlowSocket.newBuilder()
                        .setNode(sourceId.nodeId().name())
                        .setSocket(sourceId.socket());

                var target = FlowSocket.newBuilder()
                        .setNode(nodeName)
                        .setSocket(dependency.getKey());

                var edge = FlowEdge.newBuilder()
                        .setSource(source)
                        .setTarget(target);

                flow.addEdges(edge);
            }

            if (flowNode.getNodeType() == FlowNodeType.PARAMETER_NODE && metadata.modelParameter() != null)
                flow.putParameters(nodeName, metadata.modelParameter());

            if (flowNode.getNodeType() == FlowNodeType.INPUT_NODE && metadata.modelInputSchema() != null)
                flow.putInputs(nodeName, metadata.modelInputSchema());

            if (flowNode.getNodeType() == FlowNodeType.OUTPUT_NODE && metadata.modelOutputSchema() != null)
                flow.putOutputs(nodeName, metadata.modelOutputSchema());

            if (flowNode.getNodeType() == FlowNodeType.RESOURCE_NODE && metadata.modelResource() != null)
                flow.putResources(nodeName, metadata.modelResource());
        }

        return flow.build();
    }

    private static class DefaultErrorHandler implements ErrorHandler {

        @Override
        public void error(NodeId nodeId, String detail) {

            var message = String.format("Inconsistent metadata: %s (%s)", detail, nodeId);
            GraphBuilder.log.error(message);

            throw new ETracInternal(message);
        }
    }
}
