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

from __future__ import annotations

import pathlib
import typing as tp
import dataclasses as dc

import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt.metadata as meta
import tracdap.rt.config as cfg


_T = tp.TypeVar('_T')


@dc.dataclass(frozen=True)
class NodeNamespace:

    __ROOT = None

    @classmethod
    def root(cls):
        if cls.__ROOT is None:
            cls.__ROOT = NodeNamespace("", parent=None)
        return cls.__ROOT

    name: str
    parent: tp.Optional[NodeNamespace] = dc.field(default_factory=lambda: NodeNamespace.root())

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

    def __repr__(self):
        return f"{self.name} / {repr(self.namespace)}"


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

    dependencies: tp.Dict[NodeId, DependencyType] = dc.field(init=False, default_factory=dict)
    """Set of node IDs that are dependencies for this node"""

    bundle_result: bool = dc.field(init=False, default=False)
    """Flag indicating whether the result of this node is a bundle"""

    bundle_namespace: tp.Optional[NodeNamespace] = dc.field(init=False, default=None)
    """If the result is a bundle, indicates the namespace that the bundle will be unpacked into"""

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {}


def _node_type(node_class):

    # One approach for class decorators is to create a dynamic wrapper class, e.g. class NodeWrapper(node_class)
    # However, there are a *lot* of special variables that need to be copied across for everything to work
    # Instead, this approach mutates the class being decorated by adding members directly
    # So we guarantee all the special attributes of the original class are left in place
    # The dataclasses module itself works the same way

    class NodeBuilder(Node):

        explicit_deps: dc.InitVar[tp.List[NodeId]] = None

        bundle: dc.InitVar[NodeNamespace] = None

        def __post_init__(self, explicit_deps: tp.List[NodeId], bundle: NodeNamespace):
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

    return dc.dataclass(frozen=True)(node_class)


NodeMap = tp.Dict[NodeId, Node]


@dc.dataclass(frozen=True)
class Graph:

    nodes: NodeMap
    root_id: NodeId


@dc.dataclass(frozen=False)
class GraphSection:

    nodes: NodeMap
    inputs: tp.Set[NodeId] = dc.field(default_factory=set)
    outputs: tp.Set[NodeId] = dc.field(default_factory=set)
    must_run: tp.List[NodeId] = dc.field(default_factory=list)


# TODO: Where does this go?
@dc.dataclass(frozen=True)
class JobResultSpec:

    save_result: bool = False
    result_dir: tp.Union[str, pathlib.Path] = None
    result_format: str = None


# ----------------------------------------------------------------------------------------------------------------------
#  NODE DEFINITIONS
# ----------------------------------------------------------------------------------------------------------------------


Bundle: tp.Generic[_T] = tp.Dict[str, _T]
ObjectBundle = Bundle[meta.ObjectDefinition]


@_node_type
class NoopNode(Node):
    pass


@_node_type
class StaticValueNode(Node[_T]):
    value: _T


class MappingNode(Node[_T]):
    pass


@_node_type
class BundleItemNode(MappingNode[_T]):

    bundle_id: NodeId[Bundle[_T]]
    bundle_item: str

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.bundle_id: DependencyType.HARD}


@_node_type
class ContextPushNode(Node[Bundle[tp.Any]]):

    """Push a new execution context onto the stack"""

    namespace: NodeNamespace

    mapping: tp.Dict[NodeId, NodeId] = dc.field(default_factory=dict)
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being pushed)"""

    def _node_dependencies(self):
        # Since this is a context push, dependencies are the IDs from the outer context
        return {nid: DependencyType.HARD for nid in self.mapping.values()}


@_node_type
class ContextPopNode(Node[Bundle[tp.Any]]):

    namespace: NodeNamespace

    mapping: tp.Dict[NodeId, NodeId] = dc.field(default_factory=dict)
    """Mapping of node IDs from the inner to the outer context (i.e. keys are in the context being popped)"""

    def _node_dependencies(self):
        # Since this is a context pop, dependencies are the IDs from the inner context
        return {nid: DependencyType.HARD for nid in self.mapping.keys()}


@_node_type
class IdentityNode(MappingNode[_T]):

    """Map one graph node directly from another (identity function)"""

    src_id: NodeId[_T]

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.src_id: DependencyType.HARD}


@_node_type
class KeyedItemNode(MappingNode[_T]):

    """Map a graph node from a keyed item in an existing node (dictionary lookup)"""

    src_id: NodeId[Bundle[_T]]
    src_item: str

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.src_id: DependencyType.HARD}


@_node_type
class DynamicDataSpecNode(Node[_data.DataSpec]):

    data_view_id: NodeId[_data.DataView]

    data_obj_id: meta.TagHeader
    storage_obj_id: meta.TagHeader

    prior_data_spec: tp.Optional[_data.DataSpec]

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.data_view_id: DependencyType.HARD}


@_node_type
class DataViewNode(Node[_data.DataView]):

    schema: meta.SchemaDefinition
    root_item: NodeId[_data.DataItem]

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.root_item: DependencyType.HARD}


@_node_type
class DataItemNode(MappingNode[_data.DataItem]):

    """Map a data item out of an assembled data view"""

    data_view_id: NodeId[_data.DataView]

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.data_view_id: DependencyType.HARD}


@_node_type
class DataResultNode(Node[ObjectBundle]):

    output_name: str
    data_spec_id: NodeId[_data.DataSpec]
    data_save_id: NodeId[type(None)]

    data_key: str
    storage_key: str

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:

        return {
            self.data_spec_id: DependencyType.HARD,
            self.data_save_id: DependencyType.HARD}


@_node_type
class LoadDataNode(Node[_data.DataItem]):

    """
    Load an individual data item from storage
    The latest incarnation of the item will be loaded from any available copy
    """

    spec_id: NodeId[_data.DataSpec]

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.spec_id: DependencyType.HARD}


@_node_type
class SaveDataNode(Node[None]):

    """
    Save an individual data item to storage
    """

    spec_id: NodeId[_data.DataSpec]
    data_item_id: NodeId[_data.DataItem]

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.spec_id: DependencyType.HARD, self.data_item_id: DependencyType.HARD}


@_node_type
class ImportModelNode(Node[meta.ObjectDefinition]):

    model_scope: str
    import_details: meta.ImportModelJob


@_node_type
class RunModelNode(Node[Bundle[_data.DataView]]):

    model_scope: str
    model_def: meta.ModelDefinition
    parameter_ids: tp.FrozenSet[NodeId]
    input_ids: tp.FrozenSet[NodeId]

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {dep_id: DependencyType.HARD for dep_id in [*self.parameter_ids, *self.input_ids]}


@_node_type
class BuildJobResultNode(Node[cfg.JobResult]):

    job_id: meta.TagHeader

    objects: tp.Dict[str, NodeId[meta.ObjectDefinition]] = dc.field(default_factory=dict)
    bundles: tp.List[NodeId[ObjectBundle]] = dc.field(default_factory=list)

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        dep_ids = [*self.bundles, *self.objects.values()]
        return {node_id: DependencyType.HARD for node_id in dep_ids}


@_node_type
class SaveJobResultNode(Node[None]):

    job_result_id: NodeId[cfg.JobResult]
    result_spec: JobResultSpec

    def _node_dependencies(self) -> tp.Dict[NodeId, DependencyType]:
        return {self.job_result_id: DependencyType.HARD}
