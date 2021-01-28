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
import trac.rt.config as config
import trac.rt.impl.repositories as repos

import abc
import typing as tp


NodeContext = tp.Dict[NodeId, object]  # Available prior node results when a node function is called
NodeResult = tp.Any  # Result of a node function (will be recorded against the node ID)


class NodeFunction(tp.Callable[[NodeContext], NodeResult]):

    def __init__(self):
        pass

    @abc.abstractmethod
    def __call__(self, ctx: NodeContext) -> NodeResult:
        pass


class IdentityFunc(NodeFunction):

    def __call__(self, ctx: NodeContext) -> NodeResult:
        return ctx


class JobNodeFunc(NodeFunction):

    def __call__(self, ctx: NodeContext) -> NodeResult:
        return ctx


class ContextPushFunc(NodeFunction):

    def __init__(self, namespace: NodeNamespace, mapping: tp.Dict[NodeId, NodeId]):
        super().__init__()
        self.namespace = namespace
        self.mapping = mapping

    def __call__(self, ctx: NodeContext) -> NodeResult:

        target_ctx = dict()

        for target_id, source_id in self.mapping.items():

            source_item = ctx.get(source_id)

            if source_item is None:
                raise RuntimeError()  # TODO, should never happen

            if target_id.namespace != self.namespace:
                raise RuntimeError()  # TODO, should never happen

            target_ctx[target_id] = source_item

        return target_ctx


class ContextPopFunc(NodeFunction):

    def __init__(self, namespace: NodeNamespace, mapping: tp.Dict[NodeId, NodeId]):
        super().__init__()
        self.namespace = namespace
        self.mapping = mapping

    def __call__(self, ctx: NodeContext) -> NodeResult:

        target_ctx = dict()

        for source_id, target_id in self.mapping.items():

            source_item = ctx.get(source_id)

            if source_item is None:
                raise RuntimeError()  # TODO, should never happen

            target_ctx[target_id] = source_item

        return target_ctx


class MapDataFunc(NodeFunction):

    def __init__(self):
        super().__init__()

    def __call__(self, ctx: NodeContext) -> NodeResult:
        pass


class LoadDataFunc(NodeFunction):

    def __init__(self):
        super().__init__()

    def __call__(self, ctx: NodeContext) -> NodeResult:
        pass


class SaveDataFunc(NodeFunction):

    def __init__(self):
        super().__init__()

    def __call__(self, ctx: NodeContext) -> NodeResult:
        pass


class ModelFunc(NodeFunction):

    def __init__(self, job_config: config.JobConfig, node: ModelNode, model_class: api.TracModel.__class__):
        super().__init__()
        self.job_config = job_config
        self.node = node
        self.model_class = model_class

    def __call__(self, ctx: NodeContext) -> NodeResult:

        model_ctx = ModelContext(self.node.model_def, self.model_class, self.job_config.parameters)
        model: api.TracModel = self.model_class()

        model.run_model(model_ctx)

        return dict()


class FunctionResolver:

    __ResolveFunc = tp.Callable[['FunctionResolver', config.JobConfig, Node], NodeFunction]

    def __init__(self, repositories: repos.Repositories):
        self._repos = repositories

    def resolve_node(self, job_config, node: Node) -> NodeFunction:

        resolve_func = self.__node_mapping[node.__class__]

        if resolve_func is None:
            raise RuntimeError()  # TODO: Error

        return resolve_func(self, job_config, node)

    def resolve_context_push(self, job_config: config.JobConfig, node: ContextPushNode):
        return ContextPushFunc(node.namespace, node.mapping)

    def resolve_context_pop(self, job_config: config.JobConfig, node: ContextPopNode):
        return ContextPopFunc(node.namespace, node.mapping)

    def resolve_model_node(self, job_config: config.JobConfig, node: ModelNode) -> NodeFunction:

        model_loader = self._repos.get_model_loader(node.model_def.repository)
        model_class = model_loader.load_model(node.model_def)

        return ModelFunc(job_config, node, model_class)

    __node_mapping: tp.Dict[Node.__class__, __ResolveFunc] = {

        IdentityNode: lambda s, j, n: IdentityFunc(),
        JobNode: lambda s, j, n: JobNodeFunc(),

        ContextPushNode: resolve_context_push,
        ContextPopNode: resolve_context_pop,
        ModelNode: resolve_model_node
    }
