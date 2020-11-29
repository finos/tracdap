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
import concurrent.futures as fut

from .graph import NodeId
from .function import *


class Actor:

    def become(self, **kwargs) -> 'Actor':

        clone = self.__class__.__new__(self.__class__)
        clone.__dict__ = self.__dict__

        for (k, v) in kwargs.items():
            clone.__dict__[k] = v

        return clone

    def send_message(self, target: 'Actor', message: tp.Callable, **params: tp.get_args(tp.Callable)):

        message(target, **params)


class ExecutionContext(Actor):

    def __init__(self):
        self.nodes: tp.Dict[NodeId, object] = dict()

    def filter(self, nodes: tp.Set[NodeId]) -> 'ExecutionContext':

        filtered_nodes = {
            nid: n for (nid, n)
            in self.nodes.items()
            if nid in nodes}

        return self.become(nodes=filtered_nodes)

    def with_node(self, node_id, node) -> 'ExecutionContext':

        cloned_nodes = {nid: n for (nid, n) in self.nodes.items()}
        cloned_nodes[node_id] = node

        return self.become(nodes=cloned_nodes)


class JobController(Actor):

    def __init__(self, base_ctx: ExecutionContext):

        # TODO: NodeId, global scope constant
        # TODO: Global scope should be filtered shallow copy
        self._scopes: tp.Dict[str, ExecutionContext] = dict()
        self._scopes['GLOBAL'] = base_ctx

        self.tasks = dict()

    def start(self):
        pass

    def cancel(self):
        pass

    def submit_task(self):

        # Create task context by filtered mapping of parent context
        # Create task with new context
        # Start task
        # Add task to tasks dict

        pass

    def complete_task(self, task_ctx):
        pass

    def cancel_task(self):
        pass


class TaskController(Actor):

    def __init__(self, func: GraphFunction, task_ctx: ExecutionContext, worker_pool: fut.ThreadPoolExecutor):
        self.func = func
        self.ctx = task_ctx
        self.parent: JobController = None
        self.worker_pool = worker_pool
        self._future: tp.Optional[fut.Future] = None

    def start(self):

        self._future = self.worker_pool.submit(self.wrap_task)

    def complete(self):

        result_ctx = self._future.result()

        self.send_message(self.parent, JobController.complete_task, task_ctx=result_ctx)

    def cancel(self):

        if self._future is not None:
            self._future.cancel()

    def wrap_task(self):

        try:

            return self.func(self.ctx.nodes)

        except Exception as e:  # noqa

            self.send_message(self.parent, '')


class TracEngine(Actor):

    def __init__(self):

        self.engine_ctx = ExecutionContext()
        self.jobs: tp.Dict[str, JobController] = dict()

    def submit_job(self) -> 'TracEngine':

        nodes = set()
        base_ctx = self.engine_ctx.filter(nodes)

        job_id = ''  # TODO
        job_ctrl = JobController(base_ctx)
        job_ctrl.start()

        jobs = {job_id: job for (job_id, job) in self.jobs.items()}
        jobs[job_id] = job_ctrl

        return self.become(jobs=jobs)
