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

import trac.rt.impl.util as util
import trac.rt.exec.actors as actors
import trac.rt.exec.graph as graph


class GraphContextNode:

    def __init__(self):
        pass


class GraphContext:

    def __init__(self, nodes: tp.Dict[graph.NodeId, GraphContextNode]):
        self.nodes = nodes


class NodeProcessor(actors.Actor[GraphContext]):

    def __init__(self, ctx: GraphContext, node_id: str, task: callable):
        super().__init__(ctx)

    def start(self):
        pass


class GraphProcessor(actors.Actor[GraphContext]):

    def __init__(self, ctx: GraphContext, tasks: dict):
        super().__init__(ctx)
        self.__tasks = tasks

    def start(self):

        remaining_nodes = self._submit_viable_nodes(self._ctx.nodes)
        self.become(GraphContext(remaining_nodes))

    @actors.Message
    def submit_viable_nodes(self):

        remaining_nodes = self._submit_viable_nodes(self._ctx.nodes)
        self.become(GraphContext(remaining_nodes))

    def _submit_viable_nodes(self, nodes: dict):

        remaining_nodes = nodes.copy()

        for node_id, node in nodes:

            if node.is_viable:
                processor = self._system.spawn(NodeProcessor, self._ctx, node_id, node.task)
                processor.start()
                remaining_nodes.pop(node_id)

        return remaining_nodes

    @actors.Message
    def node_succeeded(self, node_id, result):

        new_nodes = {**self._ctx.nodes, node_id: result}
        new_tasks = {**self.__tasks}
        new_tasks.pop(node_id)

        remaining_nodes = self._submit_viable_nodes(new_nodes)
        active_nodes = []  # TODO

        # Check for completion
        if not any(remaining_nodes):
            self._parent.send('done')

        # Check for cyclic dependency lockup (should never happen)
        elif len(active_nodes) == 0:
            self._parent.send('failed', 'Cyclic dependencies detected during processing')

        self.become(GraphContext(remaining_nodes))

    def node_failed(self, node_id, err):

        pass


class GraphBuilder(actors.Actor[None]):

    def __init__(self):
        super().__init__(None)

    def start(self):

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


class TracEngine(actors.Actor[EngineContext]):

    def __init__(self):
        super().__init__(EngineContext())
        self._log = util.logger_for_object(self)

    def start(self):
        self._log.info("Engine is up and running")

    @actors.Message
    def submit_job(self, job_info: object):
        pass
