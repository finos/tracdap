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

from __future__ import annotations

import typing as tp
from copy import copy
from dataclasses import dataclass

import trac.rt.impl.util as util
import trac.rt.exec.actors as actors
from trac.rt.exec.graph import NodeId


@dataclass
class GraphContextNode:

    """
    Represents the state of a single node in the execution graph for processing by the TRAC engine
    """

    dependencies: tp.List[NodeId]
    result: tp.Optional[tp.Any]
    error: tp.Optional[str]


@dataclass
class GraphContext:

    """
    Represents the state of an execution graph being processed by the TRAC engine
    """

    nodes: tp.Dict[NodeId, GraphContextNode]
    pending_nodes: tp.Set[NodeId]
    active_nodes: tp.Set[NodeId]
    succeeded_nodes: tp.Set[NodeId]
    failed_nodes: tp.Set[NodeId]


class GraphBuilder(actors.Actor):

    """
    GraphBuilder is a worker (actors.Worker) responsible for building the execution graph for a job
    The logic for graph building is provided in graph_builder.py
    """

    def __init__(self, job_info: object):
        super().__init__()
        self.job_info = job_info
        self.graph: tp.Optional[GraphContext] = None
        self._log = util.logger_for_object(self)

    def on_start(self):

        self._log.info("Begin building graph...")

        # build graph context

        # store graph context

        # post graph context to parent

    @actors.Message
    def get_execution_graph(self):

        pass  # post graph context to parent


class GraphProcessor(actors.Actor):

    """
    GraphProcessor is the actor that runs the execution graph for a job
    Nodes are dispatched for execution as soon as all their dependencies are met
    When a node completes, the remaining nodes are checked to see if more can be dispatched
    The graph is fully processed when all its nodes are processed

    If there is an error with any node, the job is considered to have failed
    In this case, nodes already running are allowed to complete but no new nodes are submitted
    Once all running nodes are stopped, an error is reported to the parent
    """

    def __init__(self, graph: GraphContext):
        super().__init__()
        self.graph = graph
        self.processors: tp.Dict[NodeId, actors.ActorId] = dict()

    def on_start(self):

        self.submit_viable_nodes()

    @actors.Message
    def submit_viable_nodes(self):

        pending_nodes = copy(self.graph.pending_nodes)
        active_nodes = copy(self.graph.active_nodes)
        processors = dict()

        for node_id, node in self.graph.nodes:
            if self._is_viable_node(node_id, node):

                node_ref = self.actors().spawn(NodeProcessor, self.graph, node_id, node)
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
    def node_succeeded(self, node_id: NodeId, result):

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
                self.actors().send_parent("job_failed")
            else:
                self.actors().send_parent("job_succeeded")

    @actors.Message
    def node_failed(self, node_id: NodeId, error):

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
            self.actors().send_parent("job_failed")


class NodeProcessor(actors.Actor):

    """
    Processor responsible for running individual nodes in an execution graph
    TODO: How to decide when to allocate an actors.Worker (long running, separate thread)
    """

    def __init__(self, graph: GraphContext, node_id: str, node: GraphContextNode):
        super().__init__()
        self.graph = graph
        self.node_id = node_id
        self.node = node

    def on_start(self):
        pass


class JobProcessor(actors.Actor):

    """
    JobProcessor oversees the whole lifecycle of a job
    This includes setup (GraphBuilder), execution (GraphProcessor) and reporting results
    """

    def __init__(self, job_id, job_config):
        super().__init__()
        self.job_id = job_id
        self.job_config = job_config
        self._log = util.logger_for_object(self)

    def on_start(self):
        self._log.info("Starting job")
        self.actors().spawn(GraphBuilder, self.job_config)

    @actors.Message
    def job_graph_ready(self, graph: GraphContext):
        self.actors().spawn(GraphProcessor, graph)

    @actors.Message
    def job_succeeded(self, job_id):
        self._log.info(f"Batch job succeeded {job_id}")
        self.actors().send_parent("actors:shutdown")

    @actors.Message
    def job_failed(self, job_id):
        self._log.info(f"Batch job failed {job_id}")
        self.actors().send_parent("actors:shutdown")


@dataclass
class EngineContext:

    jobs: tp.Dict
    data: tp.Dict


class TracEngine(actors.Actor):

    """
    TracEngine is the main actor that controls all operations in the TRAC runtime
    Messages may be passed in externally via ActorSystem, e.g. commands to launch jobs
    """

    def __init__(self):
        super().__init__()
        self.engine_ctx = EngineContext(jobs={}, data={})
        self._log = util.logger_for_object(self)

    def on_start(self):

        self._log.info("Engine is up and running")

    def on_stop(self):
        pass

    @actors.Message
    def submit_job(self, job_info: object):

        job_id = 'test_job'

        self._log.info("A job has been submitted")
        self.actors().spawn(JobProcessor, job_id, job_info)

    @actors.Message
    def job_graph_built(self, job_graph: GraphContext):

        pass

    @actors.Message
    def job_succeeded(self):
        pass

    @actors.Message
    def job_failed(self):
        pass
