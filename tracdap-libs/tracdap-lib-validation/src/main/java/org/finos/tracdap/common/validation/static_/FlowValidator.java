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

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class FlowValidator {

    private static final Descriptors.Descriptor FLOW_DEFINITION;
    private static final Descriptors.FieldDescriptor FD_NODES;
    private static final Descriptors.FieldDescriptor FD_EDGES;
    private static final Descriptors.FieldDescriptor FD_PARAMETERS;
    private static final Descriptors.FieldDescriptor FD_INPUTS;
    private static final Descriptors.FieldDescriptor FD_OUTPUTS;

    private static final Descriptors.Descriptor FLOW_NODE;
    private static final Descriptors.FieldDescriptor FN_NODE_TYPE;
    private static final Descriptors.FieldDescriptor FN_PARAMETERS;
    private static final Descriptors.FieldDescriptor FN_INPUTS;
    private static final Descriptors.FieldDescriptor FN_OUTPUTS;
    private static final Descriptors.FieldDescriptor FN_NODE_SEARCH;
    private static final Descriptors.FieldDescriptor FN_NODE_ATTRS;
    private static final Descriptors.FieldDescriptor FN_NODE_PROPS;
    private static final Descriptors.FieldDescriptor FN_LABEL;

    private static final Descriptors.Descriptor FLOW_EDGE;
    private static final Descriptors.FieldDescriptor FE_SOURCE;
    private static final Descriptors.FieldDescriptor FE_TARGET;

    private static final Descriptors.Descriptor FLOW_SOCKET;
    private static final Descriptors.FieldDescriptor FS_NODE;
    private static final Descriptors.FieldDescriptor FS_SOCKET;

    static {

        FLOW_DEFINITION = FlowDefinition.getDescriptor();
        FD_NODES = field(FLOW_DEFINITION, FlowDefinition.NODES_FIELD_NUMBER);
        FD_EDGES = field(FLOW_DEFINITION, FlowDefinition.EDGES_FIELD_NUMBER);
        FD_PARAMETERS = field(FLOW_DEFINITION, FlowDefinition.PARAMETERS_FIELD_NUMBER);
        FD_INPUTS = field(FLOW_DEFINITION, FlowDefinition.INPUTS_FIELD_NUMBER);
        FD_OUTPUTS = field(FLOW_DEFINITION, FlowDefinition.OUTPUTS_FIELD_NUMBER);

        FLOW_NODE = FlowNode.getDescriptor();
        FN_NODE_TYPE = field(FLOW_NODE, FlowNode.NODETYPE_FIELD_NUMBER);
        FN_PARAMETERS = field(FLOW_NODE, FlowNode.PARAMETERS_FIELD_NUMBER);
        FN_INPUTS = field(FLOW_NODE, FlowNode.INPUTS_FIELD_NUMBER);
        FN_OUTPUTS = field(FLOW_NODE, FlowNode.OUTPUTS_FIELD_NUMBER);
        FN_NODE_SEARCH = field(FLOW_NODE, FlowNode.NODESEARCH_FIELD_NUMBER);
        FN_NODE_ATTRS = field(FLOW_NODE, FlowNode.NODEATTRS_FIELD_NUMBER);
        FN_NODE_PROPS = field(FLOW_NODE, FlowNode.NODEPROPS_FIELD_NUMBER);
        FN_LABEL = field(FLOW_NODE, FlowNode.LABEL_FIELD_NUMBER);

        FLOW_EDGE = FlowEdge.getDescriptor();
        FE_SOURCE = field(FLOW_EDGE, FlowEdge.SOURCE_FIELD_NUMBER);
        FE_TARGET = field(FLOW_EDGE, FlowEdge.TARGET_FIELD_NUMBER);

        FLOW_SOCKET = FlowSocket.getDescriptor();
        FS_NODE = field(FLOW_SOCKET, FlowSocket.NODE_FIELD_NUMBER);
        FS_SOCKET = field(FLOW_SOCKET, FlowSocket.SOCKET_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext flow(FlowDefinition flow, ValidationContext ctx) {

        // Semantic checks look at each element in isolation

        ctx = ctx.pushMap(FD_NODES)
                .apply(CommonValidators::mapNotEmpty)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapValues(FlowValidator::flowNode, FlowNode.class)
                .pop();

        ctx = ctx.pushRepeated(FD_EDGES)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(FlowValidator::flowEdge, FlowEdge.class)
                .pop();

        // Only apply consistency checks if all the individual items in the flow are semantically valid

        // TODO: Use Graph and GraphBuilder for flow consistency validation
        // This would be the same as JobConsistencyValidator and avoid duplication

        if (!ctx.failed())
            ctx = ctx.apply(FlowValidator::flowConsistency, FlowDefinition.class);

        // If the flow declares an explicit schema this must be validated as well
        // Parameters, inputs & outputs on a flow have the same structure as a model
        // Inputs and outputs must match what is declared in the nodes

        if (flow.getInputsCount() > 0 || flow.getOutputsCount() > 0 || flow.getParametersCount() > 0) {

            ctx = ModelValidator.modelSchema(FD_PARAMETERS, FD_INPUTS, FD_OUTPUTS, ctx);
            ctx = ctx.apply(FlowValidator::flowSchemaMatch, FlowDefinition.class);
        }

        return ctx;
    }

    @Validator
    public static ValidationContext flowNode(FlowNode msg, ValidationContext ctx) {

        ctx = ctx.push(FN_NODE_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, FlowNodeType.class)
                .pop();

        // Parameters, inputs and outputs are only allowed on model nodes
        var isModelNode = msg.getNodeType() == FlowNodeType.MODEL_NODE;
        var isModelNodeQualifier = String.format("%s == %s", FN_NODE_TYPE.getName(), FlowNodeType.MODEL_NODE.name());

        var isOutputNode = msg.getNodeType() == FlowNodeType.OUTPUT_NODE;
        var isOutputNodeQualifier = String.format("%s == %s", FN_NODE_TYPE.getName(), FlowNodeType.OUTPUT_NODE.name());

        // Search expressions are not allowed for parameter nodes
        var notParamNode = msg.getNodeType() == FlowNodeType.PARAMETER_NODE;
        var notParamNodeQualifier = String.format("%s == %s", FN_NODE_TYPE.getName(), FlowNodeType.PARAMETER_NODE.name());

        var knownSockets = new HashMap<String, String>();

        ctx = ctx.pushRepeated(FN_PARAMETERS)
                .apply(CommonValidators.onlyIf(isModelNode, isModelNodeQualifier))
                .applyRepeated(CommonValidators::identifier, String.class)
                .applyRepeated(CommonValidators::notTracReserved, String.class)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyRepeated(CommonValidators.uniqueContextCheck(knownSockets, FN_PARAMETERS.getName()))
                .pop();

        ctx = ctx.pushRepeated(FN_INPUTS)
                .apply(CommonValidators.ifAndOnlyIf(isModelNode, isModelNodeQualifier))
                .applyRepeated(CommonValidators::identifier, String.class)
                .applyRepeated(CommonValidators::notTracReserved, String.class)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyRepeated(CommonValidators.uniqueContextCheck(knownSockets, FN_INPUTS.getName()))
                .pop();

        ctx = ctx.pushRepeated(FN_OUTPUTS)
                .apply(CommonValidators.ifAndOnlyIf(isModelNode, isModelNodeQualifier))
                .applyRepeated(CommonValidators::identifier, String.class)
                .applyRepeated(CommonValidators::notTracReserved, String.class)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyRepeated(CommonValidators.uniqueContextCheck(knownSockets, FN_OUTPUTS.getName()))
                .pop();

        ctx = ctx.push(FN_NODE_SEARCH)
                .apply(CommonValidators.onlyIf(notParamNode, notParamNodeQualifier, true))
                .apply(SearchValidator::searchExpression, SearchExpression.class)
                .pop();

        ctx = ctx.pushRepeated(FN_NODE_ATTRS)
                .apply(CommonValidators.onlyIf(isOutputNode, isOutputNodeQualifier))
                .applyRepeated(TagUpdateValidator::tagUpdate, TagUpdate.class)
                .pop();

        ctx = ctx.pushMap(FN_NODE_PROPS)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .applyMapValues(TypeSystemValidator::value, Value.class)
                .pop();

        ctx = ctx.push(FN_LABEL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::labelLengthLimit)
                .pop();

        return ctx;
    }

    @Validator
    private static ValidationContext flowEdge(FlowEdge msg, ValidationContext ctx) {

        ctx = ctx.push(FE_SOURCE)
                .apply(CommonValidators::required)
                .apply(FlowValidator::flowSocket, FlowSocket.class)
                .pop();

        ctx = ctx.push(FE_TARGET)
                .apply(CommonValidators::required)
                .apply(FlowValidator::flowSocket, FlowSocket.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext flowSocket(FlowSocket msg, ValidationContext ctx) {

        ctx = ctx.push(FS_NODE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(FS_SOCKET)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::identifier)
                .pop();

        return ctx;
    }

    private static ValidationContext flowSchemaMatch(FlowDefinition flow, ValidationContext ctx) {

        // This implementation of the schema match check is case-sensitive
        // It should be easy for users to ensure case matches within a single flow
        // We can allow case-insensitive match when loading items into the flow for a job

        var schemaInputs = new HashSet<>(flow.getInputsMap().keySet());
        var schemaOutputs = new HashSet<>(flow.getOutputsMap().keySet());

        // First check that every input and output node is declared explicitly

        for (var nodeEntry : flow.getNodesMap().entrySet()) {

            var nodeName = nodeEntry.getKey();
            var node = nodeEntry.getValue();

            if (node.getNodeType() == FlowNodeType.INPUT_NODE) {
                var matched = schemaInputs.remove(nodeName);
                if (!matched)
                    ctx = ctx.error(String.format("Input node [%s] is missing from flow explicit inputs", nodeName));

            }

            if (node.getNodeType() == FlowNodeType.OUTPUT_NODE) {
                var matched = schemaOutputs.remove(nodeName);
                if (!matched)
                    ctx = ctx.error(String.format("Output node [%s] is missing from flow explicit outputs", nodeName));

            }
        }

        // Resport any additional inputs / outputs that do not correspond to nodes

        for (var inputName : schemaInputs)
            ctx = ctx.error(String.format("Flow explicit input [%s] does not correspond to an input node", inputName));

        for (var outputName : schemaOutputs)
            ctx = ctx.error(String.format("Flow explicit output [%s] does not correspond to an output node", outputName));

        return ctx;
    }

    private static ValidationContext flowConsistency(FlowDefinition msg, ValidationContext ctx) {

        var nodes = msg.getNodesMap();

        ctx = ctx.pushRepeated(FD_EDGES)
                .applyRepeated(FlowValidator::edgeConnection, FlowEdge.class, nodes)
                .pop();

        ctx.apply(FlowValidator::oneEdgePerTarget, FlowDefinition.class);
        ctx.apply(FlowValidator::noUnusedNodes, FlowDefinition.class);
        ctx.apply(FlowValidator::cyclicRedundancyCheck, FlowDefinition.class);

        return ctx;
    }

    private static ValidationContext edgeConnection(FlowEdge edge, Map<String, FlowNode> nodes, ValidationContext ctx) {

        // Check both source and target connect to valid sockets

        ctx.push(FE_SOURCE);
        ctx.apply(FlowValidator::socketConnection, FlowSocket.class, nodes);
        ctx.pop();

        ctx.push(FE_TARGET);
        ctx.apply(FlowValidator::socketConnection, FlowSocket.class, nodes);
        ctx.pop();

        // Do not allow an edge to connect a node to itself

        if (edge.getSource().getNode().equals(edge.getTarget().getNode()))
            ctx.error(String.format("Source and target both point to the same node [%s]", edge.getSource().getNode()));

        // Do not allow an input to be wired directly to an output

        var sourceNode = nodes.getOrDefault(edge.getSource().getNode(), null);
        var targetNode = nodes.getOrDefault(edge.getTarget().getNode(), null);

        if (sourceNode != null && sourceNode.getNodeType() == FlowNodeType.INPUT_NODE &&
            targetNode != null && targetNode.getNodeType() == FlowNodeType.OUTPUT_NODE) {

            ctx.error(String.format(
                    "Input node [%s] is connected directly to output node [%s]",
                    edge.getSource().getNode(), edge.getTarget().getNode()));
        }

        return ctx;
    }

    private static ValidationContext socketConnection(FlowSocket socket, Map<String, FlowNode> nodes, ValidationContext ctx) {

        var socketType = ctx.field().equals(FE_SOURCE) ? "Source" : "Target";
        var node = nodes.getOrDefault(socket.getNode(), null);

        if (node == null) {

            ctx.error(String.format("%s node [%s] does not exist", socketType, socket.getNode()));
        }
        else if (node.getNodeType() == FlowNodeType.OUTPUT_NODE) {

            if (ctx.field().equals(FE_SOURCE))
                ctx.error(String.format("Output node [%s] cannot be used as a source", socket.getNode()));

            else if (socket.hasField(FS_SOCKET))
                ctx.error(String.format("Target node [%s] is an output node, do not specify a [socket]", socket.getNode()));
        }
        else if (node.getNodeType() == FlowNodeType.INPUT_NODE) {

            if (ctx.field().equals(FE_TARGET))
                ctx.error(String.format("Input node [%s] cannot be used as a target", socket.getNode()));

            else if (socket.hasField(FS_SOCKET))
                ctx.error(String.format("Source node [%s] is an input node, do not specify a [socket]", socket.getNode()));
        }
        else {

            var inputOrOutput = ctx.field().equals(FE_SOURCE) ? "output" : "input";
            var modelSockets = ctx.field().equals(FE_SOURCE)
                    ? node.getOutputsList()
                    : node.getInputsList();

            if (!socket.hasField(FS_SOCKET)) {

                ctx.error(String.format(
                        "%s node [%s] is a model node, specify a [socket] to connect to a model %s",
                        socketType, socket.getNode(), inputOrOutput));
            }
            else if (!modelSockets.contains(socket.getSocket())) {

                ctx.error(String.format(
                        "Socket [%s] is not an %s of node [%s]",
                        socket.getSocket(), inputOrOutput, socket.getNode()));
            }
        }

        return ctx;
    }

    private static ValidationContext oneEdgePerTarget(FlowDefinition flow, ValidationContext ctx) {

        var edgesByTarget = new HashMap<String, Integer>();

        for (var edge : flow.getEdgesList()) {

            var socketKey = socketKey(edge.getTarget());

            if (!edgesByTarget.containsKey(socketKey))
                edgesByTarget.put(socketKey, 1);
            else
                edgesByTarget.put(socketKey, edgesByTarget.get(socketKey) + 1);
        }

        for (var nodeEntry : flow.getNodesMap().entrySet()) {

            var nodeName = nodeEntry.getKey();
            var node = nodeEntry.getValue();

            for (var target : incomingSockets(nodeName, node)) {

                var incomingEdges = edgesByTarget.get(target);

                if (incomingEdges == null || incomingEdges == 0)
                    ctx.error(String.format("Target [%s] is not supplied by any edge", target));

                else if (incomingEdges > 1)
                    ctx.error(String.format("Target [%s] is supplied by %d edges", target, incomingEdges));
            }
        }

        return ctx;
    }

    private static ValidationContext noUnusedNodes(FlowDefinition flow, ValidationContext ctx) {

        var usedNodes = flow.getEdgesList().stream()
                .map(FlowEdge::getSource)
                .map(FlowSocket::getNode)
                .collect(Collectors.toSet());

        for (var nodeEntry : flow.getNodesMap().entrySet()) {

            var nodeName = nodeEntry.getKey();
            var node = nodeEntry.getValue();

            if (node.getNodeType() == FlowNodeType.INPUT_NODE && !usedNodes.contains(nodeName))
                ctx = ctx.error(String.format("Input node [%s] is not used", nodeName));

            if (node.getNodeType() == FlowNodeType.MODEL_NODE && !usedNodes.contains(nodeName))
                ctx = ctx.error(String.format("The outputs of model node [%s] are not used", nodeName));
        }

        return ctx;
    }

    private static ValidationContext cyclicRedundancyCheck(FlowDefinition flow, ValidationContext ctx) {

        // https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

        var remainingNodes = new HashMap<>(flow.getNodesMap());
        var reachableNodes = new HashMap<String, FlowNode>();

        var edgesBySource = new HashMap<String, List<FlowEdge>>();
        var edgesByTarget = new HashMap<String, List<FlowEdge>>();

        for (var edge : flow.getEdgesList()) {

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
        for (var node : remainingNodes.entrySet()) {
            if (node.getValue().getNodeType() == FlowNodeType.INPUT_NODE)
                reachableNodes.put(node.getKey(), node.getValue());
        }

        for (var node: reachableNodes.keySet())
            remainingNodes.remove(node);

        while (!reachableNodes.isEmpty()) {

            var nodeKey = reachableNodes.keySet().stream().findAny();
            var nodeName = nodeKey.get();

            reachableNodes.remove(nodeName);

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
            ctx.error(String.format("Flow node [%s] is not reachable (this may indicate a cyclic dependency)", node));
        }

        return ctx;
    }

    private static String socketKey(FlowSocket socket) {

        return socket.getSocket().isEmpty()
                ? socket.getNode()
                : socket.getNode() + '.' + socket.getSocket();
    }

    private static Iterable<String> incomingSockets(String nodeName, FlowNode node) {

        switch (node.getNodeType()) {

            case INPUT_NODE:
                return List.of();

            case OUTPUT_NODE:
                return List.of(nodeName);

            case MODEL_NODE:
                return node.getInputsList()
                        .stream()
                        .map(socket -> nodeName + '.' + socket)
                        .collect(Collectors.toList());

            default:
                // Nodes have already been through individual validation, which checks node type is set
                throw new EUnexpected();
        }
    }
}
