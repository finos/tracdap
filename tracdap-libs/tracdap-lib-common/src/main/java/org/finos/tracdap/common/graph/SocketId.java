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

import java.util.Objects;


public class SocketId {

    public static final String SINGLE_INPUT = "";
    public static final String SINGLE_OUTPUT = "";

    private final NodeId nodeId;
    private final String socket;

    public SocketId(NodeId nodeId, String socket) {
        this.nodeId = nodeId;
        this.socket = socket;
    }

    public NodeId nodeId() {
        return nodeId;
    }

    public String socket() {
        return socket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SocketId socketId = (SocketId) o;
        return Objects.equals(nodeId, socketId.nodeId) && Objects.equals(socket, socketId.socket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, socket);
    }

    @Override
    public String toString() {
        return String.format("%s.%s / %s", nodeId.name(), socket, nodeId.namespace());
    }
}
