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

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.metadata.FlowDefinition;
import org.finos.tracdap.metadata.FlowEdge;
import org.finos.tracdap.metadata.FlowNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class GraphBuilder {

    private static final String ROOT_NODE_NAME = "trac_root";
    private static final Map<String, SocketId> NO_DEPENDENCIES = Map.of();
    private static final List<String> NO_OUTPUTS = List.of();
    private static final List<String> SINGLE_OUTPUT = List.of("");
    private static final String SINGLE_INPUT = "";

    private final NodeNamespace namespace;

    public GraphBuilder(NodeNamespace namespace) {
        this.namespace = namespace;
    }

    public Graph<FlowNode> buildFlow(FlowDefinition flow) {

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

        // Initial set of reachable nodes - no edge has edge.target == node
        var reachableNodes = nodeIds.values().stream()
                .filter(n -> !edgesByTarget.containsKey(n))
                .collect(Collectors.toList());

        var nodes = new HashMap<NodeId, Node<FlowNode>>();

        while (!reachableNodes.isEmpty()) {

            var nodeId = reachableNodes.remove(reachableNodes.size() - 1);
            var flowNode = flow.getNodesOrThrow(nodeId.name());

            var edges = edgesByTarget.get(nodeId);
            var dependencies = flowNodeDependencies(flowNode, edges, nodeIds);
            var results = flowNodeResults(flowNode);

            var node = new Node<>(nodeId, dependencies, results, flowNode);
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

        var rootId = new NodeId(ROOT_NODE_NAME, namespace);

        return new Graph<>(nodes, rootId);
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
}
