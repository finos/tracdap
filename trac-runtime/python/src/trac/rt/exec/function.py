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

import abc
import typing as tp

from .graph import NodeId


class GraphFunction:

    def __init__(self):
        pass

    @abc.abstractmethod
    def __call__(self, ctx: tp.Dict[NodeId, object]) -> tp.Dict[NodeId, object]:
        pass


class PushContextFunc:

    def __init__(self, namespace: list[str], mapping: tp.Dict[str, NodeId]):
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


class PopContextFunc:

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

    def __init__(self):
        super().__init__()

    def __call__(self):
        pass
