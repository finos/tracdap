#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import typing as _tp
import dataclasses as _dc

import tracdap.rt._impl.core.data as _data
import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg


_T = _tp.TypeVar('_T')


@_dc.dataclass(frozen=True)
class NodeNamespace:

    __ROOT = None

    @classmethod
    def root(cls):
        if cls.__ROOT is None:
            cls.__ROOT = NodeNamespace("", parent=None)
        return cls.__ROOT

    name: str
    parent: "_tp.Optional[NodeNamespace]" = _dc.field(default_factory=lambda: NodeNamespace.root())

    def __str__(self):
        if self is self.__ROOT:
            return "ROOT"
        else:
            return ", ".join(self.components())

    def __repr__(self):
        return repr(self.components())

    def components(self) -> [str]:
        if self == self.__ROOT:
            return ["ROOT"]
        elif self.parent is self.__ROOT or self.parent is None:
            return [self.name]
        else:
            return [self.name] + self.parent.components()


@_dc.dataclass(frozen=True)
class NodeId(_tp.Generic[_T]):

    @staticmethod
    def of(name: str, namespace: NodeNamespace, result_type: _tp.Type[_T]) -> "NodeId[_T]":
        return NodeId(name, namespace, result_type)

    name: str
    namespace: NodeNamespace

    result_type: _tp.Type[_T] = _dc.field(default=type(None), init=True, compare=False, hash=False)

    def __str__(self):
        return f"{self.name} / {self.namespace}"

    def __repr__(self):
        return f"{self.name} / {repr(self.namespace)}"


@_dc.dataclass(frozen=True)
class DependencyType:

    immediate: bool = True
    tolerant: bool = False

    HARD: "_tp.ClassVar[DependencyType]"
    TOLERANT: "_tp.ClassVar[DependencyType]"


DependencyType.HARD = DependencyType(immediate=True, tolerant=False)
DependencyType.SOFT = DependencyType(immediate=False, tolerant=True)
DependencyType.TOLERANT = DependencyType(immediate=True, tolerant=True)
DependencyType.DELAYED = DependencyType(immediate=False, tolerant=False)


@_dc.dataclass(frozen=True)
class Dependency:

    node_id: NodeId
    dependency_type: DependencyType


@_dc.dataclass(frozen=True)
class Node(_tp.Generic[_T]):

    """A node in the TRAC execution graph"""

    id: NodeId[_T]
    """ID of this node"""

    dependencies: _tp.Dict[NodeId, DependencyType] = _dc.field(init=False, default_factory=dict)
    """Set of node IDs that are dependencies for this node"""

    bundle_result: bool = _dc.field(init=False, default=False)
    """Flag indicating whether the result of this node is a bundle"""

    bundle_namespace: _tp.Optional[NodeNamespace] = _dc.field(init=False, default=None)
    """If the result is a bundle, indicates the namespace that the bundle will be unpacked into"""

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {}


def _node_type(node_class):

    # One approach for class decorators is to create a dynamic wrapper class, e.g. class NodeWrapper(node_class)
    # However, there are a *lot* of special variables that need to be copied across for everything to work
    # Instead, this approach mutates the class being decorated by adding members directly
    # So we guarantee all the special attributes of the original class are left in place
    # The dataclasses module itself works the same way

    class NodeBuilder(Node):

        explicit_deps: _dc.InitVar[_tp.List[NodeId]] = None

        bundle: _dc.InitVar[NodeNamespace] = None

        def __post_init__(self, explicit_deps: _tp.List[NodeId], bundle: NodeNamespace):
            dependencies = self._node_dependencies()
            if explicit_deps:
                dependencies.update({dep_id: DependencyType.HARD for dep_id in explicit_deps})
            object.__setattr__(self, "dependencies", dependencies)
            if bundle:
                object.__setattr__(self, "bundle_result", True)
                object.__setattr__(self, "bundle_namespace", bundle)

    setattr(node_class, "explicit_deps", NodeBuilder.explicit_deps)
    setattr(node_class, "bundle", NodeBuilder.bundle)
    setattr(node_class, "__post_init__", NodeBuilder.__post_init__)

    node_class.__annotations__.update(NodeBuilder.__annotations__)

    return _dc.dataclass(frozen=True)(node_class)


NodeMap = _tp.Dict[NodeId, Node]


@_dc.dataclass(frozen=True)
class GraphContext:

    job_id: _meta.TagHeader
    job_namespace: NodeNamespace
    ctx_namespace: NodeNamespace

    storage_config: _cfg.StorageConfig


@_dc.dataclass(frozen=True)
class Graph:

    nodes: NodeMap
    root_id: NodeId


@_dc.dataclass(frozen=False)
class GraphSection:

    nodes: NodeMap
    inputs: _tp.Set[NodeId] = _dc.field(default_factory=set)
    outputs: _tp.Set[NodeId] = _dc.field(default_factory=set)
    must_run: _tp.List[NodeId] = _dc.field(default_factory=list)


@_dc.dataclass(frozen=False)
class GraphUpdate:

    nodes: NodeMap = _dc.field(default_factory=dict)
    dependencies: _tp.Dict[NodeId, _tp.List[Dependency]] = _dc.field(default_factory=dict)


@_dc.dataclass
class GraphOutput:

    objectId: _meta.TagHeader
    definition: _meta.ObjectDefinition
    attrs: _tp.List[_meta.TagUpdate] = _dc.field(default_factory=list)


Bundle = _tp.Dict[str, _T]
ObjectBundle = Bundle[_meta.ObjectDefinition]

JOB_OUTPUT_TYPE = _tp.Union[GraphOutput, _data.DataSpec]


# ----------------------------------------------------------------------------------------------------------------------
#  NODE DEFINITIONS
# ----------------------------------------------------------------------------------------------------------------------


# STATIC VALUES

@_node_type
class NoopNode(Node):
    pass


@_node_type
class StaticValueNode(Node[_T]):
    value: _T


# MAPPING OPERATIONS

class MappingNode(Node[_T]):
    pass


@_node_type
class IdentityNode(MappingNode[_T]):

    """Map one graph node directly from another (identity function)"""

    src_id: NodeId[_T]

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {self.src_id: DependencyType.HARD}


@_node_type
class KeyedItemNode(MappingNode[_T]):

    """Map a graph node from a keyed item in an existing node (dictionary lookup)"""

    src_id: NodeId[Bundle[_T]]
    src_item: str

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {self.src_id: DependencyType.HARD}


@_node_type
class BundleItemNode(MappingNode[_T]):

    bundle_id: NodeId[Bundle[_T]]
    bundle_item: str

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {self.bundle_id: DependencyType.HARD}


@_node_type
class ContextPushNode(Node[Bundle[_tp.Any]]):

    """Push a new execution context onto the stack"""

    namespace: NodeNamespace

    mapping: _tp.Dict[NodeId, NodeId] = _dc.field(default_factory=dict)
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being pushed)"""

    def _node_dependencies(self):
        # Since this is a context push, dependencies are the IDs from the outer context
        return {nid: DependencyType.HARD for nid in self.mapping.values()}


@_node_type
class ContextPopNode(Node[Bundle[_tp.Any]]):

    namespace: NodeNamespace

    mapping: _tp.Dict[NodeId, NodeId] = _dc.field(default_factory=dict)
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being popped)"""

    def _node_dependencies(self):
        # Since this is a context pop, dependencies are the IDs from the inner context
        return {nid: DependencyType.HARD for nid in self.mapping.keys()}


# DATA HANDLING


@_node_type
class DataSpecNode(Node[_data.DataSpec]):

    data_view_id: NodeId[_data.DataView]

    data_obj_id: _meta.TagHeader
    storage_obj_id: _meta.TagHeader
    context_key: str

    storage_config: _cfg.StorageConfig

    prior_data_spec: _tp.Optional[_data.DataSpec]

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {self.data_view_id: DependencyType.HARD}


@_node_type
class DataViewNode(Node[_data.DataView]):

    schema: _meta.SchemaDefinition
    root_item: NodeId[_data.DataItem]

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {self.root_item: DependencyType.HARD}


@_node_type
class DataItemNode(MappingNode[_data.DataItem]):

    """Map a data item out of an assembled data view"""

    data_view_id: NodeId[_data.DataView]

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {self.data_view_id: DependencyType.HARD}


@_node_type
class LoadDataNode(Node[_data.DataItem]):

    """
    Load an individual data item from storage
    The latest incarnation of the item will be loaded from any available copy
    """

    spec_id: _tp.Optional[NodeId[_data.DataSpec]] = None
    spec: _tp.Optional[_data.DataSpec] = None

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        deps = dict()
        if self.spec_id is not None:
            deps[self.spec_id] = DependencyType.HARD
        return deps


@_node_type
class SaveDataNode(Node[_data.DataSpec]):

    """
    Save an individual data item to storage
    """

    data_item_id: NodeId[_data.DataItem]

    spec_id: _tp.Optional[NodeId[_data.DataSpec]] = None
    spec: _tp.Optional[_data.DataSpec] = None

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        deps = {self.data_item_id: DependencyType.HARD}
        if self.spec_id is not None:
            deps[self.spec_id] = DependencyType.HARD
        return deps


# MODEL EXECUTION

@_node_type
class ImportModelNode(Node[GraphOutput]):

    model_id: _meta.TagHeader

    import_details: _meta.ImportModelJob
    import_scope: str


@_node_type
class RunModelNode(Node[Bundle[_data.DataView]]):

    model_def: _meta.ModelDefinition
    model_scope: str

    parameter_ids: _tp.FrozenSet[NodeId]
    input_ids: _tp.FrozenSet[NodeId]

    storage_access: _tp.Optional[_tp.List[str]] = None

    graph_context: _tp.Optional[GraphContext] = None

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        return {dep_id: DependencyType.HARD for dep_id in [*self.parameter_ids, *self.input_ids]}


# RESULTS PROCESSING

@_node_type
class JobResultNode(Node[_cfg.JobResult]):

    job_id: _meta.TagHeader
    result_id: _meta.TagHeader

    named_outputs: _tp.Dict[str, JOB_OUTPUT_TYPE] = _dc.field(default_factory=dict)
    unnamed_outputs: _tp.List[NodeId[JOB_OUTPUT_TYPE]] = _dc.field(default_factory=list)

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        dep_ids = [*self.named_outputs.values(), *self.unnamed_outputs]
        return {node_id: DependencyType.HARD for node_id in dep_ids}


@_node_type
class DynamicOutputsNode(Node["DynamicOutputsNode"]):

    named_outputs: _tp.Dict[str, JOB_OUTPUT_TYPE] = _dc.field(default_factory=dict)
    unnamed_outputs: _tp.List[NodeId[JOB_OUTPUT_TYPE]] = _dc.field(default_factory=list)

    def _node_dependencies(self) -> _tp.Dict[NodeId, DependencyType]:
        dep_ids = [*self.named_outputs.values(), *self.unnamed_outputs]
        return {node_id: DependencyType.HARD for node_id in dep_ids}


# MISC NODE TYPES

@_node_type
class ChildJobNode(Node[_cfg.JobResult]):

    job_id: _meta.TagHeader
    job_def: _meta.JobDefinition

    graph: Graph
