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

import typing as tp
from dataclasses import dataclass

import trac.rt.metadata as meta


@dataclass(frozen=True)
class NodeCtx:

    namespace: str
    parent: tp.Optional['NodeCtx'] = None


@dataclass(frozen=True)
class NodeId:

    ctx: NodeCtx
    name: str


@dataclass
class Node:
    pass


@dataclass
class Graph:

    nodes: tp.Dict[NodeId, Node]
    root_id: NodeId


@dataclass
class ContextPushNode(Node):

    mapping: tp.Dict[str, NodeId]


@dataclass
class ContextPopNode(Node):

    mapping: tp.Dict[str, NodeId]


@dataclass
class LoadDataNode(Node):

    node_id: NodeId
    data_def: meta.DataDefinition


@dataclass
class ModelNode(Node):

    model_def: meta.ModelDefinition
