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

    def __str__(self):
        return ", ".join(self.components())

    def __repr__(self):
        return self.components()

    def components(self) -> [str]:
        level = self
        while level is not None:
            yield level.name
            level = level.parent


@dc.dataclass(frozen=True)
class NodeId:

    name: str
    namespace: NodeNamespace

    def __str__(self):
        return f"{self.name} / {self.namespace}"


@dc.dataclass(frozen=True)
class DependencyType:

    immediate: bool = True
    tolerant: bool = False

    HARD: tp.ClassVar[DependencyType]
    TOLERANT: tp.ClassVar[DependencyType]


DependencyType.HARD = DependencyType(immediate=True, tolerant=False)
DependencyType.TOLERANT = DependencyType(immediate=True, tolerant=True)


@dc.dataclass(frozen=True)
class Node:

    """A node in the TRAC execution graph"""

    id: NodeId
    """ID of this node"""

    dependencies: tp.Dict[NodeId, DependencyType] = dc.field(init=False)
    """Set of node IDs that are dependencies for this node"""


NodeMap = tp.Dict[NodeId, Node]


@dc.dataclass(frozen=True)
class Graph:

    nodes: NodeMap
    root_id: NodeId


@dc.dataclass(frozen=True)
class IdentityNode(Node):

    proxy_for: dc.InitVar[tp.Union[NodeId, tp.Dict[NodeId, DependencyType]]]

    def __post_init__(self, proxy_for):
        if isinstance(proxy_for, NodeId):
            object.__setattr__(self, 'dependencies', {proxy_for: DependencyType.HARD})
        else:
            object.__setattr__(self, 'dependencies', proxy_for)


@dc.dataclass(frozen=True)
class ContextPushNode(Node):

    """Push a new execution context onto the stack"""

    namespace: NodeNamespace
    mapping: tp.Dict[NodeId, NodeId] = dc.field(default_factory=dict)
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being pushed)"""

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):

        # Since this is a context push, dependencies are the IDs from the outer context
        mapping_dependencies = {src_id: DependencyType.HARD for src_id in self.mapping.values()}
        object.__setattr__(self, 'dependencies', mapping_dependencies)

        if explicit_deps:
            self.dependencies.update({dep: DependencyType.HARD for dep in explicit_deps})


@dc.dataclass(frozen=True)
class ContextPopNode(Node):

    namespace: NodeNamespace
    mapping: tp.Dict[NodeId, NodeId]
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being popped)"""

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        # Since this is a context pop, dependencies are the IDs from the inner context
        mapping_dependencies = {src_id: DependencyType.HARD for src_id in self.mapping.keys()}
        object.__setattr__(self, 'dependencies', mapping_dependencies)

        if explicit_deps:
            self.dependencies.update({dep: DependencyType.HARD for dep in explicit_deps})


@dc.dataclass(frozen=True)
class MappingNode(Node):
    pass


@dc.dataclass(frozen=True)
class IdentityNode(MappingNode):

    """Map one graph node directly from another (identity function)"""

    src_id: NodeId

    def __post_init__(self):
        object.__setattr__(self, 'dependencies', {self.src_id: DependencyType.HARD})


@dc.dataclass(frozen=True)
class KeyedItemNode(MappingNode):

    """Map a graph node from a keyed item in an existing node (dictionary lookup)"""

    src_id: NodeId
    src_item: str

    def __post_init__(self):
        object.__setattr__(self, 'dependencies', {self.src_id: DependencyType.HARD})


@dc.dataclass(frozen=True)
class DataViewNode(MappingNode):

    schema: meta.SchemaDefinition
    root_item: NodeId

    def __post_init__(self):
        eager_data_deps = {self.root_item: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', eager_data_deps)


@dc.dataclass(frozen=True)
class DataItemNode(MappingNode):

    """Map a data item out of an assembled data view"""

    data_view_id: NodeId
    data_item: str

    def __post_init__(self):
        object.__setattr__(self, 'dependencies', {self.data_view_id: DependencyType.HARD})


@dc.dataclass(frozen=True)
class LoadDataNode(Node):

    """
    Load an individual data item from storage
    The latest incarnation of the item will be loaded from any available copy
    """

    data_item: str
    data_def: meta.DataDefinition
    storage_def: meta.StorageDefinition

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):

        object.__setattr__(self, 'dependencies', {})

        if explicit_deps:
            self.dependencies.update({dep: DependencyType.HARD for dep in explicit_deps})


@dc.dataclass(frozen=True)
class SaveDataNode(Node):

    """
    Save an individual data item to storage
    """

    data_item: NodeId
    data_def: meta.DataDefinition
    storage_def: meta.StorageDefinition

    def __post_init__(self):
        object.__setattr__(self, 'dependencies', {self.data_item: DependencyType.HARD})


@dc.dataclass(frozen=True)
class ModelNode(Node):

    model_def: meta.ModelDefinition
    input_ids: tp.FrozenSet[NodeId]

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):

        input_dependencies = {input_id: DependencyType.HARD for input_id in self.input_ids}
        object.__setattr__(self, 'dependencies', input_dependencies)

        if explicit_deps:
            self.dependencies.update({dep: DependencyType.HARD for dep in explicit_deps})


@dc.dataclass(frozen=True)
class JobOutputMetadataNode(Node):

    data_view_id: NodeId
    physical_items: tp.Dict[NodeId, str]

    def __post_init__(self):

        output_meta_deps = {physical_node_id: DependencyType.HARD for physical_node_id in self.physical_items}
        output_meta_deps[self.data_view_id] = DependencyType.HARD

        object.__setattr__(self, 'dependencies', output_meta_deps)


@dc.dataclass(frozen=True)
class JobResultMetadataNode(Node):

    outputs: tp.FrozenSet[NodeId]

    def __post_init__(self):

        output_deps = {output_meta_id: DependencyType.HARD for output_meta_id in self.outputs}
        object.__setattr__(self, 'dependencies', output_deps)


@dc.dataclass(frozen=True)
class JobMetricsNode(Node):

    pass


@dc.dataclass(frozen=True)
class JobLogsNode(Node):

    pass


@dc.dataclass(frozen=True)
class JobNode(Node):

    # job_def: meta.JobDefinition
    root_exec_node: NodeId

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        object.__setattr__(self, 'dependencies', {self.root_exec_node: DependencyType.HARD})

        if explicit_deps:
            self.dependencies.update({dep: DependencyType.HARD for dep in explicit_deps})


