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

public class Node<TPayload> {

    private final NodeId nodeId;
    private final Map<String, SocketId> dependencies;
    private final List<String> outputs;
    private final TPayload payload;

    public Node(NodeId nodeId, Map<String, SocketId> dependencies, List<String> outputs, TPayload payload) {
        this.nodeId = nodeId;
        this.dependencies = dependencies;
        this.outputs = outputs;
        this.payload = payload;
    }

    public NodeId nodeId() {
        return nodeId;
    }

    public Map<String, SocketId> dependencies() {
        return dependencies;
    }

    public List<String> outputs() {
        return outputs;
    }

    public TPayload payload() {
        return payload;
    }
}
