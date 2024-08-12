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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class NodeNamespace {

    public static final NodeNamespace ROOT = new NodeNamespace("", /* isRoot = */ true);

    private final String name;
    private final NodeNamespace parent;

    private NodeNamespace(String name, boolean isRoot) {
        this.name = name;
        this.parent = isRoot ? null : ROOT;
    }

    public NodeNamespace(String name) {
        this(name, ROOT);
    }

    public NodeNamespace(String name, NodeNamespace parent) {
        this.name = name;
        this.parent = parent != null ? parent : ROOT;
    }

    public String name() {
        return name;
    }

    public NodeNamespace parent() {
        return parent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeNamespace that = (NodeNamespace) o;
        return Objects.equals(name, that.name) && Objects.equals(parent, that.parent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, parent);
    }

    @Override
    public String toString() {
        return String.join(", ", components());
    }

    private List<String> components() {
        return components(new ArrayList<>());
    }

    private List<String> components(List<String> components) {

        if (this == ROOT)
            return List.of("ROOT");

        components.add(this.name);

        if (this.parent == null || this.parent.equals(ROOT))
            return components;
        else
            return parent.components(components);
    }
}
