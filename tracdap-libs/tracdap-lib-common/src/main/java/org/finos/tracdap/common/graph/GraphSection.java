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

import java.util.List;
import java.util.Map;


public class GraphSection<TPayload> {

    private final Map<NodeId, Node<TPayload>> nodes;
    private final List<NodeId> inputs;
    private final List<NodeId> outputs;

    public GraphSection(Map<NodeId, Node<TPayload>> nodes, List<NodeId> inputs, List<NodeId> outputs) {
        this.nodes = nodes;
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public Map<NodeId, Node<TPayload>> nodes() {
        return nodes;
    }

    public List<NodeId> inputs() {
        return inputs;
    }

    public List<NodeId> outputs() {
        return outputs;
    }
}
