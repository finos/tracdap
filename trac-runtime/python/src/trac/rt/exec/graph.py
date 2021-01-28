#  Copyright 2020 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import typing as tp
import dataclasses as dc

import trac.rt.metadata as meta


@dc.dataclass(frozen=True)
class NodeNamespace:

    name: str
    parent: tp.Optional[NodeNamespace] = None

    def __repr__(self):
        return "NAMESPACE"


@dc.dataclass(frozen=True)
class NodeId:

    namespace: NodeNamespace
    name: str


@dc.dataclass(frozen=True)
class Node:

    """A node in the TRAC execution graph"""

    id: NodeId
    """ID of this node"""

    dependencies: tp.FrozenSet[NodeId] = dc.field(init=False)
    """Set of node IDs that are dependencies for this node"""


NodeMap = tp.Dict[NodeId, Node]


@dc.dataclass(frozen=True)
class Graph:

    nodes: NodeMap
    root_id: NodeId


@dc.dataclass(frozen=True)
class IdentityNode(Node):

    proxy_for: dc.InitVar[tp.Union[NodeId, tp.FrozenSet[NodeId]]]

    def __post_init__(self, proxy_for: tp.Union[NodeId, tp.FrozenSet[NodeId]]):
        if isinstance(proxy_for, frozenset):
            object.__setattr__(self, 'dependencies', proxy_for)
        else:
            object.__setattr__(self, 'dependencies', frozenset({proxy_for}))


@dc.dataclass(frozen=True)
class JobNode(Node):

    # job_def: meta.JobDefinition
    root_exec_node: NodeId

    def __post_init__(self):
        object.__setattr__(self, 'dependencies', frozenset([self.root_exec_node]))


@dc.dataclass(frozen=True)
class ContextPushNode(Node):

    """Push a new execution context onto the stack"""

    namespace: NodeNamespace
    mapping: tp.Dict[NodeId, NodeId] = dc.field(default_factory=dict)
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being pushed)"""

    def __post_init__(self):
        # Since this is a context push, dependencies are the IDs from the outer context
        object.__setattr__(self, 'dependencies', frozenset(self.mapping.values()))


@dc.dataclass(frozen=True)
class ContextPopNode(Node):

    namespace: NodeNamespace
    mapping: tp.Dict[NodeId, NodeId]
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being popped)"""

    def __post_init__(self):
        # Since this is a context pop, dependencies are the IDs from the inner context
        object.__setattr__(self, 'dependencies', frozenset(self.mapping.keys()))


@dc.dataclass(frozen=True)
class LoadDataNode(Node):

    node_id: NodeId
    data_def: meta.DataDefinition


@dc.dataclass(frozen=True)
class ModelNode(Node):

    model_def: meta.ModelDefinition
    input_ids: dc.InitVar[tp.FrozenSet[NodeId]]

    def __post_init__(self, input_ids):
        object.__setattr__(self, 'dependencies', input_ids)
