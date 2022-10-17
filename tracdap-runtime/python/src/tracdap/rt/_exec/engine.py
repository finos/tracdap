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

import copy as cp
import dataclasses as dc
import enum
import typing as tp

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._exec.actors as _actors
import tracdap.rt._exec.graph_builder as _graph
import tracdap.rt._exec.functions as _func
import tracdap.rt._impl.models as _models  # noqa
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.util as _util  # noqa
from .actors import Signal

from .graph import NodeId


@dc.dataclass
class _EngineNode:

    """
    Represents the state of a single node in the execution graph for processing by the TRAC engine
    """

    node: _graph.Node
    dependencies: tp.Dict[NodeId, _graph.DependencyType]
    function: tp.Optional[_func.NodeFunction] = None
    complete: bool = False
    result: tp.Optional[tp.Any] = None
    error: tp.Optional[str] = None

    def __post_init__(self):
        if not self.dependencies:
            self.dependencies = cp.copy(self.node.dependencies)


@dc.dataclass
class _EngineContext:

    """
    Represents the state of an execution graph being processed by the TRAC engine
    """

    nodes: tp.Dict[NodeId, _EngineNode]
    pending_nodes: tp.Set[NodeId] = dc.field(default_factory=set)
    active_nodes: tp.Set[NodeId] = dc.field(default_factory=set)
    succeeded_nodes: tp.Set[NodeId] = dc.field(default_factory=set)
    failed_nodes: tp.Set[NodeId] = dc.field(default_factory=set)


class TracEngine(_actors.Actor):

    """
    TracEngine is the main actor that controls all operations in the TRAC runtime
    Messages may be passed in externally via ActorSystem, e.g. commands to launch jobs
    """

    def __init__(
            self, sys_config: _cfg.RuntimeConfig,
            models: _models.ModelLoader,
            storage: _storage.StorageManager,
            notify_callback: tp.Callable[[str, tp.Optional[_cfg.JobResult], tp.Optional[Exception]], None]):

        super().__init__()

        self._log = _util.logger_for_object(self)

        self._sys_config = sys_config
        self._models = models
        self._storage = storage
        self._notify_callback = notify_callback

        self._job_actors = dict()

    def on_start(self):

        self._log.info("Engine is up and running")

    def on_stop(self):

        self._log.info("Engine shutdown complete")

    def on_signal(self, signal: Signal) -> tp.Optional[bool]:

        # Failed signals can propagate from leaf nodes up the actor tree for a job
        # If the failure goes all the way up the tree without being handled, it will reach the engine node
        # In this case we want to fail the job, rather than propagating to root which would crash the engine
        # This also ensures notifications are delivered for jobs with unhandled errors

        if signal.message == _actors.SignalNames.FAILED:

            failed_job_key = None

            # Look for the job key corresponding to the failed actor
            for job_key, job_actor in self._job_actors.items():
                if job_actor == signal.sender:
                    failed_job_key = job_key

            # If the job key is not found, the job has already been stopped and no action is needed
            # If the job is still live, call job_failed explicitly
            if failed_job_key is not None:
                if isinstance(signal, _actors.ErrorSignal) and signal.error:
                    error = signal.error
                else:
                    error = _ex.ETracInternal("An unknown error occurred")
                self.actors().send("job_failed", failed_job_key, error)

            # Failed signal has been handled, do not propagate
            return True

        return super().on_signal(signal)

    @_actors.Message
    def submit_job(
            self, job_config: _cfg.JobConfig,
            job_result_dir: str,
            job_result_format: str):

        job_key = _util.object_key(job_config.jobId)

        result_needed = bool(job_result_dir)
        result_spec = _graph.JobResultSpec(result_needed, job_result_dir, job_result_format)

        self._log.info(f"Job submitted: [{job_key}]")

        job_actor_id = self.actors().spawn(
            JobProcessor, job_key,
            job_config, result_spec,
            self._models, self._storage)

        job_actors = {**self._job_actors, job_key: job_actor_id}
        self._job_actors = job_actors

    @_actors.Message
    def job_succeeded(self, job_key: str, job_result: _cfg.JobResult):

        # Ignore duplicate messages from the job processor (can happen in unusual error cases)
        if job_key not in self._job_actors:
            self._log.warning(f"Ignoring [job_succeeded] message, job [{job_key}] has already completed")
            return

        self._log.info(f"Recording job as successful: {job_key}")

        if self._notify_callback is not None:
            self._notify_callback(job_key, job_result, None)

        self._finalize_job(job_key)

    @_actors.Message
    def job_failed(self, job_key: str, error: Exception):

        # Ignore duplicate messages from the job processor (can happen in unusual error cases)
        if job_key not in self._job_actors:
            self._log.warning(f"Ignoring [job_failed] message, job [{job_key}] has already completed")
            return

        self._log.error(f"Recording job as failed: {job_key}")

        if self._notify_callback is not None:
            self._notify_callback(job_key, None, error)

        self._finalize_job(job_key)

    def _finalize_job(self, job_key: str):

        job_actors = self._job_actors
        job_actor_id = job_actors.pop(job_key)
        self.actors().stop(job_actor_id)
        self._job_actors = job_actors


class JobProcessor(_actors.Actor):

    """
    JobProcessor oversees the whole lifecycle of a job
    This includes setup (GraphBuilder), execution (GraphProcessor) and reporting results
    """

    def __init__(
            self, job_key, job_config: _cfg.JobConfig,
            result_spec: _graph.JobResultSpec,
            models: _models.ModelLoader,
            storage: _storage.StorageManager):

        super().__init__()
        self.job_key = job_key
        self.job_config = job_config
        self.result_spec = result_spec
        self._models = models
        self._storage = storage
        self._log = _util.logger_for_object(self)

    def on_start(self):
        self._log.info(f"Starting job [{self.job_key}]")
        self._models.create_scope(self.job_key)
        self.actors().spawn(GraphBuilder, self.job_config, self.result_spec, self._models, self._storage)

    def on_stop(self):
        self._log.info(f"Cleaning up job [{self.job_key}]")
        self._models.destroy_scope(self.job_key)

    @_actors.Message
    def job_graph(self, graph: _EngineContext, root_id: NodeId):
        self.actors().spawn(GraphProcessor, graph, root_id)
        self.actors().stop(self.actors().sender)

    @_actors.Message
    def job_succeeded(self, job_result: _cfg.JobResult):
        self._log.info(f"Job succeeded {self.job_key}")
        self.actors().send_parent("job_succeeded", self.job_key, job_result)

    @_actors.Message
    def job_failed(self, error: Exception):
        self._log.error(f"Job failed {self.job_key}")
        self.actors().send_parent("job_failed", self.job_key, error)


class GraphBuilder(_actors.Actor):

    """
    GraphBuilder is a worker (actors.Worker) responsible for building the execution graph for a job
    The logic for graph building is provided in graph_builder.py
    """

    def __init__(
            self, job_config: _cfg.JobConfig,
            result_spec: _graph.JobResultSpec,
            models: _models.ModelLoader,
            storage: _storage.StorageManager):

        super().__init__()
        self.job_config = job_config
        self.result_spec = result_spec
        self.graph: tp.Optional[_EngineContext] = None

        self._resolver = _func.FunctionResolver(models, storage)
        self._log = _util.logger_for_object(self)

    def on_start(self):

        self._log.info("Building execution graph")

        # TODO: Get sys config, or find a way to pass storage settings
        graph_data = _graph.GraphBuilder.build_job(self.job_config, self.result_spec)
        graph_nodes = {node_id: _EngineNode(node, {}) for node_id, node in graph_data.nodes.items()}
        graph = _EngineContext(graph_nodes, pending_nodes=set(graph_nodes.keys()))

        self._log.info("Resolving graph nodes to executable code")

        for node_id, node in graph.nodes.items():
            node.function = self._resolver.resolve_node(node.node)

        self.graph = graph
        self.actors().send_parent("job_graph", self.graph, graph_data.root_id)

    @_actors.Message
    def get_execution_graph(self):

        self.actors().send(self.actors().sender, "job_graph", self.graph)


class GraphProcessor(_actors.Actor):

    """
    GraphProcessor is the actor that runs the execution graph for a job
    Nodes are dispatched for execution as soon as all their dependencies are met
    When a node completes, the remaining nodes are checked to see if more can be dispatched
    The graph is fully processed when all its nodes are processed

    If there is an error with any node, the job is considered to have failed
    In this case, nodes already running are allowed to complete but no new nodes are submitted
    Once all running nodes are stopped, an error is reported to the parent
    """

    def __init__(self, graph: _EngineContext, root_id: NodeId):
        super().__init__()
        self.graph = graph
        self.root_id = root_id
        self.processors: tp.Dict[NodeId, _actors.ActorId] = dict()
        self._log = _util.logger_for_object(self)

    def on_start(self):

        self._log.info("Begin processing graph")
        self.actors().send(self.actors().id, "submit_viable_nodes")

    @_actors.Message
    def submit_viable_nodes(self):

        node_processors = dict()

        def process_graph(graph: _EngineContext) -> _EngineContext:

            pending_nodes = cp.copy(graph.pending_nodes)
            active_nodes = cp.copy(graph.active_nodes)
            failed_nodes = cp.copy(graph.failed_nodes)

            for node_id in graph.pending_nodes:

                node = self.graph.nodes[node_id]

                if self._upstream_failure(node, graph):

                    self._log.warning(f"SKIP {str(node_id)} (upstream failure)")

                    pending_nodes.discard(node_id)
                    failed_nodes.add(node_id)

                elif self._is_viable_node(node, graph):

                    node_ref = self.actors().spawn(NodeProcessor, self.graph, node_id, node)
                    node_processors[node_id] = node_ref

                    pending_nodes.discard(node_id)
                    active_nodes.add(node_id)

            processed_graph = cp.copy(self.graph)
            processed_graph.pending_nodes = pending_nodes
            processed_graph.active_nodes = active_nodes
            processed_graph.failed_nodes = failed_nodes

            return processed_graph

        current_graph = self.graph
        new_graph = process_graph(current_graph)

        # Let errors propagate as far as they can without any nodes being evaluated
        while len(new_graph.failed_nodes) > len(current_graph.failed_nodes):
            current_graph = new_graph
            new_graph = process_graph(current_graph)

        self.graph = new_graph
        self.processors = {**self.processors, **node_processors}

        # Job may have completed due to error propagation
        self.check_job_status(do_submit=False)

    @classmethod
    def _is_viable_node(cls, node: _EngineNode, graph: _EngineContext):

        return all(dep in graph.succeeded_nodes for dep in node.dependencies)

    @classmethod
    def _upstream_failure(cls, node: _EngineNode, graph: _EngineContext):

        return any(not dep_type.tolerant and dep in graph.failed_nodes
                   for dep, dep_type in node.dependencies.items())

    @_actors.Message
    def node_succeeded(self, node_id: NodeId, result):

        old_node = self.graph.nodes[node_id]
        node = cp.copy(old_node)
        node.complete = True
        node.result = result
        results = {node_id: node}

        # For bundle nodes, add the individual bundle items to the result update

        if node.node.bundle_result:
            for item_name, item_result in result.items():

                item_id = NodeId(item_name, node.node.bundle_namespace)

                item_old_node = self.graph.nodes[item_id]
                item_node = cp.copy(item_old_node)
                item_node.complete = True
                item_node.result = item_result

                # Use the original node ID, to avoid overwriting the result type
                results[item_node.node.id] = item_node

        self._update_results(results)

    @_actors.Message
    def node_failed(self, node_id: NodeId, error):

        old_node = self.graph.nodes[node_id]
        node = cp.copy(old_node)
        node.complete = True
        node.error = error

        self._update_results({node_id: node})

    def _update_results(self, updates: tp.Dict[NodeId, _EngineNode]):

        nodes = {**self.graph.nodes, **updates}

        pending_nodes = cp.copy(self.graph.pending_nodes)
        active_nodes = cp.copy(self.graph.active_nodes)
        succeeded_nodes = cp.copy(self.graph.succeeded_nodes)
        failed_nodes = cp.copy(self.graph.failed_nodes)

        for node_id, node in updates.items():

            if node_id in active_nodes:
                active_nodes.remove(node_id)
            elif node_id in pending_nodes:
                pending_nodes.remove(node_id)  # TODO: check pending node ID is part of main node id bundle
            else:
                raise _ex.ETracInternal()

            if node.error:
                failed_nodes.add(node_id)
            else:
                succeeded_nodes.add(node_id)

            if node_id in self.processors:
                node_ref = self.processors.pop(node_id)
                self.actors().stop(node_ref)

        graph = _EngineContext(nodes, pending_nodes, active_nodes, succeeded_nodes, failed_nodes)

        self.graph = graph
        self.check_job_status()

    def check_job_status(self, do_submit=True):

        # Do not check final status if there are pending nodes to be submitted
        if do_submit and any(self.graph.pending_nodes):
            self.actors().send(self.actors().id, "submit_viable_nodes")
            return

        # If processing is complete, report the final status to the engine
        if not any(self.graph.active_nodes):

            if any(self.graph.pending_nodes):
                err_msg = "Processor has become deadlocked (cyclic dependency error)"
                self._log.error(err_msg)
                self.actors().send_parent("job_failed", _ex.ETracInternal(err_msg))

            elif any(self.graph.failed_nodes):

                errors = list(filter(
                    lambda e: e is not None,
                    iter(self.graph.nodes[n].error for n in self.graph.failed_nodes)))

                if len(errors) == 1:
                    self.actors().send_parent("job_failed", errors[0])
                else:
                    self.actors().send_parent("job_failed", _ex.EModelExec("Job suffered multiple errors", errors))

            else:
                job_result = self.graph.nodes[self.root_id].result
                self.actors().send_parent("job_succeeded", job_result)


class NodeProcessor(_actors.Actor):

    """
    Processor responsible for running individual nodes in an execution graph
    """

    # TODO: How to decide when to allocate an actors.Worker (long running, separate thread)

    __NONE_TYPE = type(None)

    def __init__(self, graph: _EngineContext, node_id: str, node: _EngineNode):
        super().__init__()
        self.graph = graph
        self.node_id = node_id
        self.node = node

    def on_start(self):

        self.actors().send(self.actors().id, "evaluate_node")

    @_actors.Message
    def graph_event(self):
        pass

    @_actors.Message
    def stream_event(self):
        pass

    @_actors.Message
    def evaluate_node(self):

        try:

            NodeLogger.log_node_start(self.node)

            ctx = NodeContextImpl(self.graph.nodes)
            result = self.node.function(ctx)

            self._check_result_type(result)
            self.actors().send_parent("node_succeeded", self.node_id, result)

            NodeLogger.log_node_succeeded(self.node)

        except Exception as e:

            self.actors().send_parent("node_failed", self.node_id, e)

            NodeLogger.log_node_failed(self.node, e)

    @classmethod
    def result_matches_type(cls, result, expected_type) -> bool:

        if expected_type is None or expected_type == cls.__NONE_TYPE:
            return result is None

        if expected_type == tp.Any:
            return True

        generic_type = _util.get_origin(expected_type)

        if generic_type is None:
            return isinstance(result, expected_type)

        if generic_type == list:

            list_type = _util.get_args(expected_type)[0]

            def list_type_check(item):
                return cls.result_matches_type(item, list_type)

            return isinstance(result, generic_type) and all(map(list_type_check, result))

        if generic_type == dict:

            dict_type_args = _util.get_args(expected_type)
            key_type = dict_type_args[0]
            value_type = dict_type_args[1]

            def dict_type_check(entry):
                key, value = entry
                return isinstance(key, key_type) and cls.result_matches_type(value, value_type)

            return isinstance(result, generic_type) and all(map(dict_type_check, result.items()))

        raise _ex.ETracInternal(f"Cannot enforce type check for generic type [{str(generic_type)}]")

    def _check_result_type(self, result):

        # Use an internal error if the result is the wrong type
        # Node functions should only ever return the expected type

        expected_type = self.node.node.id.result_type or self.__NONE_TYPE
        result_type = type(result)

        if not self.result_matches_type(result, expected_type):
            err = f"Node result is the wrong type, expected [{expected_type.__name__}], got [{result_type.__name__}]"
            raise _ex.ETracInternal(err)


class NodeLogger:

    """
    Log the activity of the NodeProcessor
    """

    # Separate out the logic for logging nodes, so the NodeProcessor itself stays a bit cleaner

    _log = _util.logger_for_class(NodeProcessor)

    class LoggingType(enum.Enum):
        DEFAULT = 0
        STATIC_VALUE = 1
        PUSH_POP = 2
        SIMPLE_MAPPING = 3
        MODEL = 4

    @classmethod
    def log_node_start(cls, node: _EngineNode):

        logging_type = cls._logging_type(node)
        node_name = node.node.id.name
        namespace = node.node.id.namespace

        if logging_type == cls.LoggingType.STATIC_VALUE:
            cls._log.info(f"SET {cls._value_type(node)} [{node_name}] / {namespace}")

        elif logging_type in [cls.LoggingType.SIMPLE_MAPPING]:
            cls._log.info(f"MAP {cls._value_type(node)} [{cls._mapping_source(node)}] -> [{node_name}] / {namespace}")

        else:
            cls._log.info(f"START {cls._func_type(node)} [{node_name}] / {namespace}")

    @classmethod
    def log_node_succeeded(cls, node: _EngineNode):

        logging_type = cls._logging_type(node)
        node_name = node.node.id.name
        namespace = node.node.id.namespace

        if logging_type in [cls.LoggingType.STATIC_VALUE, cls.LoggingType.SIMPLE_MAPPING]:
            return

        if logging_type == cls.LoggingType.PUSH_POP:
            cls._log_push_pop_node_details(node.node)  # noqa

        if logging_type == cls.LoggingType.MODEL:
            cls._log_model_node_details(node.node)  # noqa

        cls._log.info(f"DONE {cls._func_type(node)} [{node_name}] / {namespace}")

    @classmethod
    def log_node_failed(cls, node: _EngineNode, e: Exception):

        node_name = node.node.id.name
        namespace = node.node.id.namespace

        cls._log.error(f"FAILED {cls._func_type(node)} [{node_name}] / {namespace}")
        cls._log.exception(e)

    @classmethod
    def _log_push_pop_node_details(cls, node: tp.Union[_graph.ContextPushNode, _graph.ContextPopNode]):

        push_or_pop = "PUSH" if isinstance(node, _graph.ContextPushNode) else "POP"
        direction = "->" if isinstance(node, _graph.ContextPushNode) else "<-"

        for inner_id, outer_id in node.mapping.items():
            item_type = cls._type_str(inner_id.result_type)
            msg = f"{push_or_pop} {item_type} [{outer_id.name}] {direction} [{inner_id.name}] / {node.id.namespace}"
            cls._log.info(msg)

    @classmethod
    def _log_model_node_details(cls, node: _graph.RunModelNode):

        cls._type_str(_data.DataView)

        for output in node.model_def.outputs:
            result_type = cls._type_str(_data.DataView)
            msg = f"RESULT {result_type} [{output}] / {node.bundle_namespace}"
            cls._log.info(msg)

    @classmethod
    def _logging_type(cls, node: _EngineNode) -> LoggingType:

        if isinstance(node.node, _graph.StaticValueNode):
            return cls.LoggingType.STATIC_VALUE

        if isinstance(node.node, _graph.ContextPushNode) or isinstance(node.node, _graph.ContextPopNode):
            return cls.LoggingType.PUSH_POP

        if isinstance(node.node, _graph.IdentityNode):
            return cls.LoggingType.SIMPLE_MAPPING

        if isinstance(node.node, _graph.RunModelNode):
            return cls.LoggingType.MODEL

        return cls.LoggingType.DEFAULT

    @classmethod
    def _value_type(cls, node: _EngineNode) -> str:

        return cls._type_str(node.node.id.result_type)

    @classmethod
    def _type_str(cls, result_type: type) -> str:

        if result_type is not None:
            return result_type.__name__
        else:
            return str(None)

    @classmethod
    def _func_type(cls, node: _EngineNode) -> str:

        # Remove "Func" from "xxxFunc"

        func_type = type(node.function)
        return func_type.__name__[:-4]

    @classmethod
    def _mapping_source(cls, node: _EngineNode) -> str:

        graph_node = node.node

        if isinstance(graph_node, _graph.IdentityNode):
            return graph_node.src_id.name

        return "(unknown)"


class NodeContextImpl(_func.NodeContext):

    __T = tp.TypeVar("__T")

    def __init__(self, nodes: tp.Dict[NodeId, _EngineNode]):
        self.__nodes = nodes

    def lookup(self, node_id: NodeId[__T]) -> __T:

        engine_node = self.__nodes.get(node_id)

        # Use internal errors if any of the checks fail on a result lookup
        # The engine should guarantee that all these conditions are met before a node is executed

        if engine_node is None:
            raise _ex.ETracInternal(f"Node [{node_id.name}] does not exist in execution context [{node_id.namespace}]")

        if not engine_node.complete:
            raise _ex.ETracInternal(f"Node [{node_id.name}] still pending in execution context [{node_id.namespace}]")

        if engine_node.error:
            raise _ex.ETracInternal(f"Node [{node_id.name}] failed in execution context [{node_id.namespace}]")

        if not NodeProcessor.result_matches_type(engine_node.result, node_id.result_type):

            expected_type = node_id.result_type or type(None)
            result_type = type(engine_node.result)

            err = f"Wrong type for node [{node_id.name}] in execution context [{node_id.namespace}]" \
                + f" (expected [{expected_type}], got [{result_type}])"

            raise _ex.ETracInternal(err)

        return engine_node.result

    def iter_items(self) -> tp.Iterator[tp.Tuple[NodeId, tp.Any]]:

        for node_id, node in self.__nodes.items():
            if node.complete and not node.error:
                yield node_id, node.result
