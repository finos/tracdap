#  Copyright 2022 Accenture Global Solutions Limited
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

import pathlib
import typing as tp
import dataclasses as dc

import tracdap.rt.impl.data as _data
import tracdap.rt.metadata as meta
import tracdap.rt.config as cfg


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


_T = tp.TypeVar('_T')


@dc.dataclass(frozen=True)
class NodeId(tp.Generic[_T]):

    @staticmethod
    def of(name: str, namespace: NodeNamespace, result_type: tp.Type[_T]) -> NodeId[_T]:
        return NodeId(name, namespace, result_type)

    name: str
    namespace: NodeNamespace

    result_type: tp.Type[_T] = dc.field(default=type(None), init=True, compare=False, hash=False)

    def __str__(self):
        return f"{self.name} / {self.namespace}"


@dc.dataclass(frozen=True)
class DependencyType:

    immediate: bool = True
    tolerant: bool = False

    HARD: tp.ClassVar[DependencyType]
    TOLERANT: tp.ClassVar[DependencyType]


DependencyType.HARD = DependencyType(immediate=True, tolerant=False)
DependencyType.SOFT = DependencyType(immediate=False, tolerant=True)
DependencyType.TOLERANT = DependencyType(immediate=True, tolerant=True)
DependencyType.DELAYED = DependencyType(immediate=False, tolerant=False)


@dc.dataclass(frozen=True)
class Node(tp.Generic[_T]):

    """A node in the TRAC execution graph"""

    id: NodeId[_T]
    """ID of this node"""

    dependencies: tp.Dict[NodeId, DependencyType] = dc.field(init=False)
    """Set of node IDs that are dependencies for this node"""


NodeMap = tp.Dict[NodeId, Node]
ObjectMap = tp.Dict[str, meta.ObjectDefinition]


@dc.dataclass(frozen=True)
class Graph:

    nodes: NodeMap
    root_id: NodeId


@dc.dataclass(frozen=True)
class JobResultSpec:

    save_result: bool = False
    result_dir: tp.Union[str, pathlib.Path] = None
    result_format: str = None


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
class MappingNode(Node[_T]):
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
class SetParametersNode(Node):

    parameters: tp.Dict[str, meta.Value]

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        dependencies = {dep: DependencyType.HARD for dep in explicit_deps} if explicit_deps else {}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class StaticDataSpecNode(Node[_data.DataItemSpec]):

    data_spec: _data.DataItemSpec

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        dependencies = {dep: DependencyType.HARD for dep in explicit_deps} if explicit_deps else {}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class DynamicDataSpecNode(Node[_data.DataItemSpec]):

    data_view_id: NodeId[_data.DataView]

    data_obj_id: meta.TagHeader
    storage_obj_id: meta.TagHeader

    prior_data_spec: tp.Optional[_data.DataItemSpec]

    def __post_init__(self):
        dependencies = {self.data_view_id: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class DataViewNode(Node[_data.DataView]):

    schema: meta.SchemaDefinition
    root_item: NodeId[_data.DataItem]

    def __post_init__(self):
        dependencies = {self.root_item: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class DataItemNode(MappingNode):

    """Map a data item out of an assembled data view"""

    data_view_id: NodeId[_data.DataView]

    def __post_init__(self):
        dependencies = {self.data_view_id: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class DataResultNode(Node[ObjectMap]):

    output_name: str
    data_spec_id: NodeId[_data.DataItemSpec]
    data_save_id: NodeId[type(None)]

    data_key: str
    storage_key: str

    def __post_init__(self):
        dependencies = {self.data_spec_id: DependencyType.HARD, self.data_save_id: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class LoadDataNode(Node[_data.DataItem]):

    """
    Load an individual data item from storage
    The latest incarnation of the item will be loaded from any available copy
    """

    spec_id: NodeId[_data.DataItemSpec]

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        dependencies = {dep: DependencyType.HARD for dep in explicit_deps} if explicit_deps else {}
        dependencies[self.spec_id] = DependencyType.HARD
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class SaveDataNode(Node):

    """
    Save an individual data item to storage
    """

    spec_id: NodeId[_data.DataItemSpec]
    data_item_id: NodeId[_data.DataItem]

    def __post_init__(self):
        dependencies = {self.spec_id: DependencyType.HARD, self.data_item_id: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class ImportModelNode(Node[meta.ModelDefinition]):

    model_scope: str
    import_details: meta.ImportModelJob

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        dependencies = {dep: DependencyType.HARD for dep in explicit_deps} if explicit_deps else {}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class ImportModelResultNode(Node[ObjectMap]):

    import_id: NodeId[meta.ModelDefinition]
    object_id: meta.TagHeader

    def __post_init__(self):
        dependencies = {self.import_id: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class RunModelNode(Node):

    model_scope: str
    model_def: meta.ModelDefinition
    parameter_ids: tp.FrozenSet[NodeId]
    input_ids: tp.FrozenSet[NodeId]

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        dependencies = {dep: DependencyType.HARD for dep in explicit_deps} if explicit_deps else {}
        dependencies.update({param_id: DependencyType.HARD for param_id in self.parameter_ids})
        dependencies.update({input_id: DependencyType.HARD for input_id in self.input_ids})
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class BuildJobResultNode(Node[cfg.JobResult]):

    job_id: meta.TagHeader
    result_ids: tp.List[NodeId[ObjectMap]]

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        dependencies = {dep: DependencyType.HARD for dep in explicit_deps} if explicit_deps else {}
        dependencies.update({result_id: DependencyType.HARD for result_id in self.result_ids})
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class SaveJobResultNode(Node[type(None)]):

    job_result_id: NodeId[cfg.JobResult]
    result_spec: JobResultSpec

    def __post_init__(self):
        dependencies = {self.job_result_id: DependencyType.HARD}
        object.__setattr__(self, 'dependencies', dependencies)


@dc.dataclass(frozen=True)
class JobMetricsNode(Node):

    pass


@dc.dataclass(frozen=True)
class JobLogsNode(Node):

    pass


@dc.dataclass(frozen=True)
class JobNode(Node):

    # job_def: meta.JobDefinition
    target_node_id: NodeId
    result_node_id: NodeId

    explicit_deps: dc.InitVar[tp.Optional[tp.List[NodeId]]] = None

    def __post_init__(self, explicit_deps):
        dependencies = {dep: DependencyType.HARD for dep in explicit_deps} if explicit_deps else {}
        dependencies[self.target_node_id] = DependencyType.HARD
        dependencies[self.result_node_id] = DependencyType.HARD
        object.__setattr__(self, 'dependencies', dependencies)
