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

from .graph import *
from .context import ModelContext

import trac.rt.api as api
import trac.rt.impl.repositories as repos

import abc
import typing as tp


class GraphFunction:

    def __init__(self):
        pass

    @abc.abstractmethod
    def __call__(self, ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:
        pass


class PushContextFunc(GraphFunction):

    def __init__(self, namespace: list[str], mapping: tp.Dict[str, NodeId]):
        super().__init__()
        self.mapping = mapping

    def __call__(self, ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:

        push_ctx = dict()

        for (name, source_id) in self.mapping:

            if source_id not in ctx:
                raise RuntimeError()  # TODO

            target_ctx = ['']  # TODO
            target_id = NodeId(name, target_ctx)

            push_ctx[target_id] = ctx[source_id]

        return push_ctx


class PopContextFunc(GraphFunction):

    def __call__(self, ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:
        pass


class MapDataFunc(GraphFunction):

    def __init__(self):
        super().__init__()

    def __call__(self, ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:
        pass


class LoadDataFunc(GraphFunction):

    def __init__(self):
        super().__init__()

    def __call__(self, ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:
        pass


class SaveDataFunc(GraphFunction):

    def __init__(self):
        super().__init__()

    def __call__(self, ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:
        pass


class ModelFunc(GraphFunction):

    def __init__(self, node: ModelNode, model_class: api.TracModel.__class__):
        super().__init__()
        self.node = node
        self.model_class = model_class

    def __call__(self, graph_ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:

        model_ctx = ModelContext()
        model: api.TracModel = self.model_class()

        model.run_model(model_ctx)

        return graph_ctx


class FunctionResolver:

    def __init__(self, repositories: repos.Repositories):
        self._repos = repositories

    def resolve_node(self, node: Node) -> GraphFunction:

        resolve_func = self.__node_mapping[node.__class__]

        if resolve_func is None:
            raise RuntimeError()  # TODO: Error

        return resolve_func(self, node)

    def resolve_model_node(self, node: ModelNode) -> GraphFunction:

        model_loader = self._repos.get_model_loader(node.model_def.repository)
        model_class = model_loader.load_model(node.model_def)

        return ModelFunc(node, model_class)

    __node_mapping: tp.Dict[Node.__class__, tp.Callable] = {
        ModelNode: resolve_model_node
    }
