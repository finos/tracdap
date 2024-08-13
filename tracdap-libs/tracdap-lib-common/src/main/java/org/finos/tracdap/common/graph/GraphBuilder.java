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

package org.finos.tracdap.common.graph;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.metadata.MetadataBundle;
import org.finos.tracdap.metadata.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static final List<String> SINGLE_OUTPUT = List.of("");
    private static final String SINGLE_INPUT = "";

    private final NodeNamespace namespace;
    private final MetadataBundle metadataBundle;
    private final ErrorHandler errorHandler;

    public GraphBuilder(NodeNamespace namespace, MetadataBundle metadataBundle, ErrorHandler errorHandler) {
        this.namespace = namespace;
        this.metadataBundle = metadataBundle;
        this.errorHandler = errorHandler;
    }

    public GraphBuilder(NodeNamespace namespace, MetadataBundle metadataBundle) {
        this(namespace, metadataBundle, DEFAULT_ERROR_HANDLER);
    }

    public GraphBuilder(NodeNamespace namespace) {
        this(namespace, null, DEFAULT_ERROR_HANDLER);
    }

    public GraphSection<NodeMetadata> buildJob(JobDefinition job) {

        if (job.getJobType() != JobType.RUN_FLOW)
            throw new ETracInternal("Graph building is only supported for RUN_FLOW jobs");

        return buildRunFlowJob(job.getRunFlow());
    }

    public GraphSection<NodeMetadata> buildRunFlowJob(RunFlowJob job) {

        if (metadataBundle == null)
            throw new ETracInternal("Metadata bundle is needed to build a job graph");

        var flowObj = metadataBundle.getResource(job.getFlow());

        if (flowObj == null || flowObj.getObjectType() != ObjectType.FLOW)
            throw new ETracInternal("Metadata bundle does not contain the flow object");

        var flow = flowObj.getFlow();

        var flowGraph = buildFlow(flow);
        var jobGraph = addJobMetadata(flowGraph, job);
        var paramsGraph = autowireFlowParameters(jobGraph, flow, job);

        return applyTypeInference(paramsGraph);
    }

    public GraphSection<NodeMetadata> buildFlow(FlowDefinition flow) {

        // https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

        var edgesBySource = new HashMap<String, List<FlowEdge>>();
        var edgesByTarget = new HashMap<String, List<FlowEdge>>();
        var edges = new HashMap<SocketId, SocketId>();

        for (var edge : flow.getEdgesList()) {

            var sourceNode = edge.getSource().getNode();
            var targetNode = edge.getTarget().getNode();

            if (!edgesBySource.containsKey(sourceNode))
                edgesBySource.put(sourceNode, new ArrayList<>());

            if (!edgesByTarget.containsKey(targetNode))
                edgesByTarget.put(targetNode, new ArrayList<>());

            edgesBySource.get(sourceNode).add(edge);
            edgesByTarget.get(targetNode).add(edge);

            var sourceSocket = new SocketId(new NodeId(sourceNode, namespace), edge.getSource().getSocket());
            var targetSocket = new SocketId(new NodeId(targetNode, namespace), edge.getTarget().getSocket());
            edges.put(targetSocket, sourceSocket);
        }

        // Initial set of reachable flow nodes is just the input nodes
        var remainingNodes = new HashMap<>(flow.getNodesMap());
        var reachableNodes = new HashMap<String, FlowNode>();

        for (var node : flow.getNodesMap().entrySet()) {
            if (node.getValue().getNodeType() == FlowNodeType.INPUT_NODE || node.getValue().getNodeType() == FlowNodeType.PARAMETER_NODE) {
                reachableNodes.put(node.getKey(), node.getValue());
                remainingNodes.remove(node.getKey());
            }
        }

        // Graph nodes to build during traversal
        var graphNodes = new HashMap<NodeId, Node<NodeMetadata>>();

        while (!reachableNodes.isEmpty()) {

            var nodeKey = reachableNodes.keySet().stream().findAny();
            var nodeName = nodeKey.get();
            var flowNode = reachableNodes.remove(nodeName);

            var graphNode = buildFlowNode(flow, edges, nodeName, flowNode);
            graphNodes.put(graphNode.nodeId(), graphNode);

            var sourceEdges = edgesBySource.remove(nodeName);

            // Some nodes do not feed any other nodes, e.g. output nodes, or if there are errors in the flow
            if (sourceEdges == null)
                continue;

            for (var edge : sourceEdges) {

                var targetNodeName = edge.getTarget().getNode();
                var targetEdges = edgesByTarget.get(targetNodeName);
                targetEdges.remove(edge);

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

    private Node<NodeMetadata> buildFlowNode(FlowDefinition flow, Map<SocketId, SocketId> edges, String nodeName, FlowNode flowNode) {

        // Create nodeId in the building namespace
        var nodeId = new NodeId(nodeName, namespace);

        // Look up strict metadata if it is present in the flow
        var modelParam = flow.getParametersOrDefault(nodeName, null);
        var modelInput = flow.getInputsOrDefault(nodeName, null);
        var modelOutput = flow.getOutputsOrDefault(nodeName, null);

        // Runtime object / value is not part of the flow, these will always be null
        var nodeMetadata = new NodeMetadata(flowNode, modelParam, modelInput, modelOutput, null, null);

        switch (flowNode.getNodeType()) {

            case PARAMETER_NODE:
            case INPUT_NODE:
                return new Node<>(nodeId, NO_DEPENDENCIES, SINGLE_OUTPUT, nodeMetadata);

            case OUTPUT_NODE:
                var outputSocket = new SocketId(nodeId, SINGLE_INPUT);
                var outputSource = edges.get(outputSocket);  // TODO null check
                var outputDeps = Map.of(SINGLE_INPUT, outputSource);
                return new Node<>(nodeId, outputDeps, NO_OUTPUTS, nodeMetadata);

            case MODEL_NODE:
                var modelDeps = flowNode.getInputsList().stream()
                        .map(input -> new SocketId(nodeId, input))
                        .map(input -> Map.entry(input.socket(), edges.get(input)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return new Node<>(nodeId, modelDeps, flowNode.getOutputsList(), nodeMetadata);

            default:
                errorHandler.error(nodeId, String.format("Missing or invalid node type [%s]", flowNode.getNodeType()));
                return new Node<>(nodeId, NO_DEPENDENCIES, NO_OUTPUTS, nodeMetadata);
        }
    }

    private Map<String, SocketId> flowNodeDependencies(FlowNode flowNode, List<FlowEdge> edges, Map<String, NodeId> nodeIds) {

        return Map.of();  // TODO
    }

    public GraphSection<NodeMetadata> addJobMetadata(GraphSection<NodeMetadata> graph, RunModelJob job) {

        var params = job.getParametersMap();
        var inputs = job.getInputsMap();
        var outputs = job.getPriorOutputsMap();
        var models = Map.of(MODEL_NODE_NAME, job.getModel());

        return addJobMetadata(graph, params, inputs, outputs, models);
    }

    public GraphSection<NodeMetadata> addJobMetadata(GraphSection<NodeMetadata> graph, RunFlowJob job) {

        var params = job.getParametersMap();
        var inputs = job.getInputsMap();
        var outputs = job.getPriorOutputsMap();
        var models = job.getModelsMap();

        return addJobMetadata(graph, params, inputs, outputs, models);
    }

    public GraphSection<NodeMetadata> addJobMetadata(
            GraphSection<NodeMetadata> graph,
            Map<String, Value> params,
            Map<String, TagSelector> inputs,
            Map<String, TagSelector> outputs,
            Map<String, TagSelector> models) {

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
        return new Node<>(node.nodeId(), node.dependencies(), node.outputs(), metadata);
    }

    private Node<NodeMetadata> addRuntimeObject(Node<NodeMetadata> node, Map<String, TagSelector> runtimeObjects) {

        var runtimeSelector = runtimeObjects.get(node.nodeId().name());

        if (runtimeSelector == null)
            return node;

        var runtimeObject = metadataBundle.getResource(runtimeSelector);

        if (runtimeObject == null)
            return node;

        var metadata = node.payload().withRuntimeObject(runtimeObject);
        return new Node<>(node.nodeId(), node.dependencies(), node.outputs(), metadata);
    }

    public GraphSection<NodeMetadata> autowireFlowParameters(GraphSection<NodeMetadata> graph, FlowDefinition flow, RunFlowJob job) {

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

            var updatedMetadata = nodeMetadata.withFlowNode(flowNode.build());
            var updatedNode = new Node<>(node.nodeId(), dependencies, node.outputs(), updatedMetadata);
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
        var nodeMetadata = new NodeMetadata(flowNode, modelParameter, null, null, null, runtimeValue);

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
                var inferredNode = new Node<>(node.nodeId(), node.dependencies(), node.outputs(), inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.INPUT_NODE && nodeMetadata.modelInputSchema() == null) {

                var targets = dependents.getOrDefault(node.nodeId(), List.of());
                var inputSchema = inferInputSchema(node.nodeId(), targets, graph);
                var inferredMetadata = nodeMetadata.withModelInputSchema(inputSchema);
                var inferredNode = new Node<>(node.nodeId(), node.dependencies(), node.outputs(), inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.OUTPUT_NODE && nodeMetadata.modelOutputSchema() == null) {

                if (node.dependencies().size() != 1)
                    continue;

                var source = node.dependencies().values().iterator().next();
                var outputSchema = inferOutputSchema(source, graph);
                var inferredMetadata = nodeMetadata.withModelOutputSchema(outputSchema);
                var inferredNode = new Node<>(node.nodeId(), node.dependencies(), node.outputs(), inferredMetadata);

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

        var schema = modelInput.getSchema();
        var nextSchema = nextModelInput.getSchema();

        if (schema.getSchemaType() != SchemaType.TABLE || nextSchema.getSchemaType() != SchemaType.TABLE) {
            errorHandler.error(nodeId, "Only TABLE schema types are supported");
            return null;
        }

        var table = schema.getTable().toBuilder();
        var nextTable = nextSchema.getTable();

        var fieldsMap = table.getFieldsList().stream()
                .collect(Collectors.toMap(f -> f.getFieldName().toLowerCase(), f -> f));

        for (var nextField : nextTable.getFieldsList()) {

            var field = fieldsMap.get(nextField.getFieldName().toLowerCase());

            if (field == null) {
                var nextFieldOrder = table.getFieldsCount();
                table.addFields(nextField.toBuilder().setFieldOrder(nextFieldOrder));
            }
            else {
                var combinedField = combineFieldSchema(field, nextField);
                if (combinedField == null)
                    return null;
                table.setFields(field.getFieldOrder(), combinedField);
                fieldsMap.put(nextField.getFieldName().toLowerCase(), combinedField);
            }
        }

        var combinedSchema = schema.toBuilder().setTable(table);
        return modelInput.setSchema(combinedSchema);
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

    private static class DefaultErrorHandler implements ErrorHandler {

        @Override
        public void error(NodeId nodeId, String detail) {

            var message = String.format("Inconsistent metadata: %s (%s)", detail, nodeId);
            GraphBuilder.log.error(message);

            throw new ETracInternal(message);
        }
    }
}
