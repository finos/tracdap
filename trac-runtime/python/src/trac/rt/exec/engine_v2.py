#  Copyright 2021 Accenture Global Solutions Limited
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
from copy import copy

import trac.rt.impl.util as util
import trac.rt.exec.actors as actors
from trac.rt.exec.graph import NodeId


class GraphContextNode:

    def __init__(self):
        self.dependencies: tp.List[NodeId] = list()


class GraphContext:

    def __init__(self, nodes: tp.Dict[NodeId, GraphContextNode]):
        self.nodes = nodes
        self.pending_nodes: tp.Set[NodeId] = set()
        self.active_nodes: tp.Set[NodeId] = set()
        self.succeeded_nodes: tp.Set[NodeId] = set()
        self.failed_nodes: tp.Set[NodeId] = set()


class NodeProcessor(actors.Actor):

    def __init__(self, graph: GraphContext, node_id: str, node: GraphContextNode):
        super().__init__()
        self.graph = graph
        self.node_id = node_id
        self.node = node

    def on_start(self):
        pass


class GraphProcessor(actors.Actor):

    def __init__(self, graph: GraphContext):
        super().__init__()
        self.graph = graph
        self.processors: tp.Dict[NodeId, actors.ActorRef] = dict()

    def on_start(self):

        self.submit_viable_nodes()

    @actors.Message
    def submit_viable_nodes(self, ctx: actors.ActorContext):

        pending_nodes = copy(self.graph.pending_nodes)
        active_nodes = copy(self.graph.active_nodes)
        processors = dict()

        for node_id, node in self.graph.nodes:
            if self._is_viable_node(node_id, node):

                node_ref = ctx.spawn(NodeProcessor, self.graph, node_id, node)
                processors[node_id] = node_ref

                pending_nodes.discard(node_id)
                active_nodes.add(node_id)

        new_graph = copy(self.graph)
        new_graph.pending_nodes = pending_nodes
        new_graph.active_nodes = active_nodes

        self.graph = new_graph
        self.processors = {**self.processors, **processors}

    def _is_viable_node(self, node_id: NodeId, node: GraphContextNode):

        return \
            node_id in self.graph.pending_nodes and \
            all(map(lambda dep: dep in self.graph.succeeded_nodes, node.dependencies))

    @actors.Message
    def node_succeeded(self, ctx: actors.ActorContext, node_id: NodeId, result):

        old_node = self.graph.nodes[node_id]
        node = copy(old_node)
        node.result = result

        nodes = {**self.graph.nodes, node_id: node}

        active_nodes = copy(self.graph.active_nodes)
        active_nodes.remove(node_id)

        succeeded_nodes = copy(self.graph.succeeded_nodes)
        succeeded_nodes.add(node_id)

        new_graph = copy(self.graph)
        new_graph.nodes = nodes
        new_graph.active_nodes = active_nodes
        new_graph.succeeded_nodes = succeeded_nodes

        self.graph = new_graph

        # Only submit new nodes if there have not been any failures
        if any(self.graph.pending_nodes) and not any(self.graph.failed_nodes):
            self.submit_viable_nodes()

        # If processing is complete, report the final status to the engine
        elif not any(self.graph.active_nodes):
            if any(self.graph.failed_nodes):
                ctx.send(None, 'job_failed')
            else:
                ctx.send(None, "job_succeeded")

    @actors.Message
    def node_failed(self, ctx: actors.ActorContext, node_id: NodeId, error):

        old_node = self.graph.nodes[node_id]
        node = copy(old_node)
        node.error = error

        nodes = {**self.graph.nodes, node_id: node}

        active_nodes = copy(self.graph.active_nodes)
        active_nodes.remove(node_id)

        failed_nodes = copy(self.graph.succeeded_nodes)
        failed_nodes.add(node_id)

        new_graph = copy(self.graph)
        new_graph.nodes = nodes
        new_graph.active_nodes = active_nodes
        new_graph.failed_nodes = failed_nodes

        self.graph = new_graph

        # If other nodes are still active, allow those nodes to complete
        # Otherwise, report a failed status to the engine right away
        if not any(self.graph.active_nodes):
            ctx.send(None, 'job_failed')


class JobBuilder(actors.Actor):

    def __init__(self, job_info: object):
        super().__init__()
        self.job_info = job_info
        self.graph: tp.Optional[GraphContext] = None

    def on_start(self):

        # build graph context

        # store graph context

        # post graph context to parent

        pass

    def get_execution_graph(self):

        pass  # post graph context to parent


class EngineContext:

    def __init__(self):
        self.jobs = {}
        self.data = {}


class TracEngine(actors.Actor):

    def __init__(self):
        super().__init__()
        self.engine_ctx = EngineContext()
        self._log = util.logger_for_object(self)

    def on_start(self):
        self._log.info("Engine is up and running")

    def on_stop(self):
        pass

    @actors.Message("new_job")
    def job_info_received(self, ctx: actors.ActorContext, job_info: object):

        self._log.info("A job has been submitted")

        ctx.spawn(JobBuilder, job_info)

    @actors.Message("new_job_graph")
    def job_graph_built(self, ctx: actors.ActorContext, job_graph: GraphContext):

        pass

    @actors.Message
    def job_succeeded(self):
        pass

    @actors.Message
    def job_failed(self):
        pass
