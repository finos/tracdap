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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class GraphBuilder {

    private static final String MODEL_NODE_NAME = "trac_model";
    private static final String ROOT_NODE_NAME = "trac_root";
    private static final Map<String, SocketId> NO_DEPENDENCIES = Map.of();
    private static final List<String> NO_OUTPUTS = List.of();
    private static final List<String> SINGLE_OUTPUT = List.of("");
    private static final String SINGLE_INPUT = "";

    private final NodeNamespace namespace;
    private final MetadataBundle metadataBundle;

    public GraphBuilder(NodeNamespace namespace, MetadataBundle metadataBundle) {
        this.namespace = namespace;
        this.metadataBundle = metadataBundle;
    }

    public GraphBuilder(NodeNamespace namespace) {
        this(namespace, null);
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

        // TODO: Refer to flow validator and bring logic into one place

        // 1-2-1 mapping of flow nodes to node IDs, all in the same namespace
        var nodeIds = flow.getNodesMap().keySet().stream()
                .map(n -> new NodeId(n, namespace))
                .collect(Collectors.toMap(NodeId::name, Function.identity()));

        var edgesBySource = new HashMap<NodeId, List<FlowEdge>>();
        var edgesByTarget = new HashMap<NodeId, List<FlowEdge>>();

        for (var edge : flow.getEdgesList()) {

            var sourceId = nodeIds.get(edge.getSource().getNode());
            var targetId = nodeIds.get(edge.getTarget().getNode());

            if (!edgesBySource.containsKey(sourceId))
                edgesBySource.put(sourceId, new ArrayList<>());

            if (!edgesByTarget.containsKey(targetId))
                edgesByTarget.put(targetId, new ArrayList<>());

            edgesBySource.get(sourceId).add(edge);
            edgesByTarget.get(targetId).add(edge);
        }

        var inputs = nodeIds.values().stream()
                .filter(n -> !edgesByTarget.containsKey(n))
                .collect(Collectors.toUnmodifiableList());

        var outputs = nodeIds.values().stream()
                .filter(n -> !edgesBySource.containsKey(n))
                .collect(Collectors.toUnmodifiableList());

        // Initial set of reachable nodes - no edge has edge.target == node
        var reachableNodes = new ArrayList<>(inputs);
        var nodes = new HashMap<NodeId, Node<NodeMetadata>>();

        while (!reachableNodes.isEmpty()) {

            var nodeId = reachableNodes.remove(reachableNodes.size() - 1);
            var flowNode = flow.getNodesOrThrow(nodeId.name());

            // Look up strict metadata if it is present in the flow
            var modelParam = flow.getParametersOrDefault(nodeId.name(), null);
            var modelInput = flow.getInputsOrDefault(nodeId.name(), null);
            var modelOutput = flow.getOutputsOrDefault(nodeId.name(), null);

            // Runtime object / value is not part of the flow, these will always be null
            var nodeMetadata = new NodeMetadata(flowNode, modelParam, modelInput, modelOutput, null, null);


            var edges = edgesByTarget.get(nodeId);
            var dependencies = flowNodeDependencies(flowNode, edges, nodeIds);
            var results = flowNodeResults(flowNode);

            var node = new Node<>(nodeId, dependencies, results, nodeMetadata);
            nodes.put(nodeId, node);

            var outboundEdges = edgesBySource.get(nodeId);
            if (outboundEdges == null)
                continue;

            for (var edge : outboundEdges) {

                var target = nodeIds.get(edge.getTarget().getNode());
                var inboundEdges = edgesByTarget.get(target);

                var reachable = inboundEdges.stream()
                        .map(e -> nodeIds.get(e.getSource().getNode()))
                        .allMatch(edgesBySource::containsKey);

                if (reachable)
                    reachableNodes.add(target);
            }
        }

        return new GraphSection<>(nodes, inputs, outputs);
    }

    private Map<String, SocketId> flowNodeDependencies(FlowNode flowNode, List<FlowEdge> edges, Map<String, NodeId> nodeIds) {

        return Map.of();  // TODO
    }

    private List<String> flowNodeResults(FlowNode flowNode) {

        switch (flowNode.getNodeType()) {

            case PARAMETER_NODE:
            case INPUT_NODE:
                return SINGLE_OUTPUT;

            case OUTPUT_NODE:
                return NO_OUTPUTS;

            case MODEL_NODE:
                return flowNode.getOutputsList();

            default:
                throw new EUnexpected();  // TODO
        }

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

            var model = nodeMetadata.runtimeObject().getModel();
            var params = new ArrayList<String>(model.getParametersCount());
            params.addAll(model.getParametersMap().keySet());
            params.addAll(nodeMetadata.flowNode().getParametersList());

            var dependencies = new HashMap<>(node.dependencies());

            for (var paramName : params) {

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

            var updatedNode = new Node<>(node.nodeId(), dependencies, node.outputs(), nodeMetadata);
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
                var parameter = inferParameter(graph, targets);
                var inferredMetadata = nodeMetadata.withModelParameter(parameter);
                var inferredNode = new Node<>(node.nodeId(), node.dependencies(), node.outputs(), inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.INPUT_NODE && nodeMetadata.modelInputSchema() == null) {

                var targets = dependents.getOrDefault(node.nodeId(), List.of());
                var inputSchema = inferInputSchema(graph, targets);
                var inferredMetadata = nodeMetadata.withModelInputSchema(inputSchema);
                var inferredNode = new Node<>(node.nodeId(), node.dependencies(), node.outputs(), inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }

            if (nodeMetadata.flowNode().getNodeType() == FlowNodeType.OUTPUT_NODE && nodeMetadata.modelOutputSchema() == null) {

                if (node.dependencies().size() != 1)
                    continue;

                var source = node.dependencies().values().iterator().next();
                var outputSchema = inferOutputSchema(graph, source);
                var inferredMetadata = nodeMetadata.withModelOutputSchema(outputSchema);
                var inferredNode = new Node<>(node.nodeId(), node.dependencies(), node.outputs(), inferredMetadata);

                nodes.put(node.nodeId(), inferredNode);
            }
        }

        return new GraphSection<>(nodes, graph.inputs(), graph.outputs());
    }


    public ModelParameter inferParameter(GraphSection<NodeMetadata> graph, List<SocketId> targets) {

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

        if(targetParams.isEmpty())
            return null;

        if (targetParams.size() == 1)
            return targetParams.get(0).getValue();

        // TODO
        return null;
    }

    public ModelInputSchema inferInputSchema(GraphSection<NodeMetadata> graph, List<SocketId> targets) {

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

        if(targetInputs.isEmpty())
            return null;

        if (targetInputs.size() == 1)
            return targetInputs.get(0).getValue();

        // TODO
        return null;
    }

    public ModelOutputSchema inferOutputSchema(GraphSection<NodeMetadata> graph, SocketId source) {

        var sourceNode = graph.nodes().get(source.nodeId());

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

}
