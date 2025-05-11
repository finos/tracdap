#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import copy as cp
import dataclasses as dc
import enum
import io
import pathlib
import typing as tp

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.exec.actors as _actors
import tracdap.rt._impl.exec.graph_builder as _graph
import tracdap.rt._impl.exec.functions as _func
import tracdap.rt._impl.core.config_parser as _cfg_p
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.models as _models
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.util as _util

from .graph import NodeId


@dc.dataclass
class _EngineNode:

    """
    Represents the state of a single node in the execution graph for processing by the TRAC engine
    """

    node: _graph.Node
    function: tp.Optional[_func.NodeFunction] = None

    dependencies: tp.Dict[NodeId, _graph.DependencyType] = dc.field(default_factory=dict)
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

    engine_id: _actors.ActorId
    job_key: str
    root_id: NodeId

    nodes: tp.Dict[NodeId, _EngineNode]
    pending_nodes: tp.Set[NodeId] = dc.field(default_factory=set)
    active_nodes: tp.Set[NodeId] = dc.field(default_factory=set)
    succeeded_nodes: tp.Set[NodeId] = dc.field(default_factory=set)
    failed_nodes: tp.Set[NodeId] = dc.field(default_factory=set)

    def with_updates(
            self, nodes,
            pending_nodes, active_nodes,
            succeeded_nodes, failed_nodes) -> "_EngineContext":

        return _EngineContext(
            self.engine_id, self.job_key, self.root_id, nodes,
            pending_nodes, active_nodes, succeeded_nodes, failed_nodes)


@dc.dataclass
class _JobResultSpec:

    save_result: bool = False
    result_dir: tp.Union[str, pathlib.Path] = None
    result_format: str = None


@dc.dataclass
class _JobLog:

    log_init: "dc.InitVar[tp.Optional[_JobLog]]" = None

    log_file_needed: bool = True

    log_buffer: io.BytesIO = None
    log_provider: _logging.LogProvider = None

    def __post_init__(self, log_init):

        if log_init is not None:
            self.log_provider = log_init.log_provider
            self.log_file_needed = False
        elif self.log_file_needed:
            self.log_buffer = io.BytesIO()
            self.log_provider = _logging.job_log_provider(self.log_buffer)
        else:
            self.log_provider = _logging.LogProvider()


@dc.dataclass
class _JobState:

    job_id: _meta.TagHeader

    actor_id: _actors.ActorId = None
    monitors: tp.List[_actors.ActorId] = dc.field(default_factory=list)

    job_config: _cfg.JobConfig = None
    job_result: _cfg.JobResult = None
    job_error: Exception = None

    parent_key: str = None
    result_spec: _JobResultSpec = None

    job_log: _JobLog = None


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

        self._log = _logging.logger_for_object(self)

        self._sys_config = sys_config
        self._models = models
        self._storage = storage
        self._notify_callback = notify_callback

        self._jobs: tp.Dict[str, _JobState] = dict()

    def on_start(self):

        self._log.info("Engine is up and running")

    def on_stop(self):

        self._log.info("Engine shutdown complete")

    def on_signal(self, signal: _actors.Signal) -> tp.Optional[bool]:

        # Failed signals can propagate from leaf nodes up the actor tree for a job
        # If the failure goes all the way up the tree without being handled, it will reach the engine node
        # In this case we want to fail the job, rather than propagating to root which would crash the engine
        # This also ensures notifications are delivered for jobs with unhandled errors

        if signal.message == _actors.SignalNames.FAILED:

            failed_job_key = None

            # Look for the job key corresponding to the failed actor
            for job_key, job_state in self._jobs.items():
                if job_state.actor_id == signal.sender:
                    failed_job_key = job_key

            # If the job is still live, call job_failed explicitly
            if failed_job_key is not None:

                cause = signal.error if isinstance(signal, _actors.ErrorSignal) else None
                cause_message = str(cause) if cause is not None else "(unknown)"
                message = f"There was an error in the TRAC execution engine: " + \
                          f"failed node = [{signal.sender}], cause = {cause_message}"
                error = _ex .ETracInternal(message)
                error.__cause__ = cause

                self.actors().send(self.actors().id, "job_failed", failed_job_key, error)

                # Failed signal has been handled, do not propagate
                return True

        return super().on_signal(signal)

    @_actors.Message
    def submit_job(
            self, job_config: _cfg.JobConfig,
            job_result_dir: str,
            job_result_format: str):

        job_key = _util.object_key(job_config.jobId)

        self._log.info(f"Received a new job: [{job_key}]")

        result_needed = bool(job_result_dir)
        result_spec = _JobResultSpec(result_needed, job_result_dir, job_result_format)
        job_log = _JobLog(log_file_needed=result_needed)

        job_state = _JobState(job_config.jobId)
        job_state.job_log = job_log

        job_processor = JobProcessor(
            self._sys_config, self._models, self._storage,
            job_key, job_config, graph_spec=None, job_log=job_state.job_log)

        job_actor_id = self.actors().spawn(job_processor)

        job_monitor_success = lambda ctx, key, result: self._notify_callback(key, result, None)
        job_monitor_failure = lambda ctx, key, error: self._notify_callback(key, None, error)
        job_monitor = JobMonitor(job_key, job_monitor_success, job_monitor_failure)
        job_monitor_id = self.actors().spawn(job_monitor)

        job_state.actor_id = job_actor_id
        job_state.monitors.append(job_monitor_id)
        job_state.job_config = job_config
        job_state.result_spec = result_spec

        self._jobs[job_key] = job_state

    @_actors.Message
    def submit_child_job(self, parent_key: str, child_id: _meta.TagHeader, child_graph: _graph.Graph, monitor_id: _actors.ActorId):

        parent_state = self._jobs.get(parent_key)

        # Ignore duplicate messages from the job processor (can happen in unusual error cases)
        if parent_state is None:
            self._log.warning(f"Ignoring [submit_child_job] message, parent [{parent_key}] has already completed")
            return

        child_key = _util.object_key(child_id)

        self._log.info(f"Received a child job: [{child_key}] for parent [{parent_key}]")

        child_job_log = _JobLog(parent_state.job_log)

        child_processor = JobProcessor(
            self._sys_config, self._models, self._storage,
            child_key, None, graph_spec=child_graph, job_log=child_job_log)

        child_actor_id = self.actors().spawn(child_processor)

        child_state = _JobState(child_id)
        child_state.job_log = child_job_log
        child_state.actor_id = child_actor_id
        child_state.monitors.append(monitor_id)
        child_state.parent_key = parent_key
        child_state.result_spec = _JobResultSpec(False)  # Do not output separate results for child jobs

        self._jobs[child_key] = child_state

    @_actors.Message
    def get_job_list(self):

        job_list = list(map(self._get_job_info, self._jobs.keys()))
        self.actors().reply("job_list", job_list)

    @_actors.Message
    def get_job_details(self, job_key: str, details: bool):

        details = self._get_job_info(job_key, details)
        self.actors().reply("job_details", details)

    @_actors.Message
    def job_succeeded(self, job_key: str, job_result: _cfg.JobResult):

        # Ignore duplicate messages from the job processor (can happen in unusual error cases)
        if job_key not in self._jobs:
            self._log.warning(f"Ignoring [job_succeeded] message, job [{job_key}] has already completed")
            return

        self._log.info(f"Marking job as successful: {job_key}")

        job_state = self._jobs[job_key]
        job_state.job_result = job_result

        for monitor_id in job_state.monitors:
            self.actors().send(monitor_id, "job_succeeded", job_result)

        self._finalize_job(job_key)

    @_actors.Message
    def job_failed(self, job_key: str, error: Exception, job_result: tp.Optional[_cfg.JobResult] = None):

        # Ignore duplicate messages from the job processor (can happen in unusual error cases)
        if job_key not in self._jobs:
            self._log.warning(f"Ignoring [job_failed] message, job [{job_key}] has already completed")
            return

        self._log.error(f"Marking job as failed: {job_key}")

        job_state = self._jobs[job_key]

        # Build a failed result if none is supplied by the job processor (should not normally happen)
        # In this case, no job log will be included in the output
        if job_result is None and job_state.job_config is not None:
            job_id = job_state.job_id
            result_id = job_state.job_config.resultId
            result_def = _meta.ResultDefinition()
            result_def.jobId = _util.selector_for(job_state.job_id)
            result_def.statusCode = _meta.JobStatusCode.FAILED
            result_def.statusMessage = str(error)
            job_result = _cfg.JobResult(job_id, result_id, result_def)

        job_state.job_result = job_result
        job_state.job_error = error

        for monitor_id in job_state.monitors:
            self.actors().send(monitor_id, "job_failed", error)

        self._finalize_job(job_key)

    def _finalize_job(self, job_key: str):

        # Stop the actor but keep the job state available for status / results queries

        # In the future, job state will need to be expunged after some period of time
        # For now each instance of the runtime only processes one job so no need to worry

        job_state = self._jobs.get(job_key)

        # Record output metadata if required (not needed for local runs or when using API server)
        if job_state.parent_key is None and job_state.result_spec.save_result:
            self._save_job_result(job_key, job_state)

        # Stop any monitors that were created directly by the engine
        # (Other actors are responsible for stopping their own monitors)
        while job_state.monitors:
            monitor_id = job_state.monitors.pop()
            monitor_parent = monitor_id[:monitor_id.rfind('/')]
            if self.actors().id == monitor_parent:
                self.actors().stop(monitor_id)

        if job_state.actor_id is not None:
            self.actors().stop(job_state.actor_id)
            job_state.actor_id = None

    def _save_job_result(self, job_key: str, job_state: _JobState):

        self._log.info(f"Saving job result for [{job_key}]")

        # It might be better abstract reporting of results, job status etc., perhaps with a job monitor

        if job_state.result_spec.save_result:

            result_format = job_state.result_spec.result_format
            result_dir = job_state.result_spec.result_dir
            result_file = f"job_result_{job_key}.{result_format}"
            result_path = pathlib.Path(result_dir).joinpath(result_file)

            with open(result_path, "xt") as result_stream:
                result_content = _cfg_p.ConfigQuoter.quote(job_state.job_result, result_format)
                result_stream.write(result_content)

    def _get_job_info(self, job_key: str, details: bool = False) -> tp.Optional[_cfg.JobResult]:

        job_state = self._jobs.get(job_key)

        if job_state is None:
            return None

        job_result = _cfg.JobResult()
        job_result.jobId = job_state.job_id
        job_result.resultId = job_state.job_config.resultId
        job_result.result = _meta.ResultDefinition()
        job_result.result.jobId = _util.selector_for(job_state.job_id)

        if job_state.actor_id is not None:
            job_result.result.statusCode = _meta.JobStatusCode.RUNNING

        elif job_state.job_result is not None:
            job_result.result.statusCode = job_state.job_result.result.statusCode
            job_result.result.statusMessage = job_state.job_result.result.statusMessage
            if details:
                job_result.objectIds = job_state.job_result.objectIds or list()
                job_result.objects = job_state.job_result.objects or dict()

        elif job_state.job_error is not None:
            job_result.result.statusCode = _meta.JobStatusCode.FAILED
            job_result.result.statusMessage = str(job_state.job_error.args[0])

        else:
            # Alternatively return UNKNOWN status or throw an error here
            job_result.result.statusCode = _meta.JobStatusCode.FAILED
            job_result.result.statusMessage = "No details available"

        return job_result


class JobMonitor(_actors.Actor):

    def __init__(
            self, job_key: str,
            success_func: tp.Callable[[_actors.ActorContext, str, _cfg.JobResult], None],
            failure_func: tp.Callable[[_actors.ActorContext, str, Exception], None]):

        super().__init__()
        self._job_key = job_key
        self._success_func = success_func
        self._failure_func = failure_func
        self._signal_sent = False

    @_actors.Message
    def job_succeeded(self, job_result: _cfg.JobResult):
        self._success_func(self.actors(), self._job_key, job_result)
        self._signal_sent = True

    @_actors.Message
    def job_failed(self, error: Exception):
        self._failure_func(self.actors(), self._job_key, error)
        self._signal_sent = True

    def on_stop(self):
        if not self._signal_sent:
            error = _ex.ETracInternal(f"No result was received for job [{self._job_key}]")
            self._failure_func(self.actors(), self._job_key, error)


class JobProcessor(_actors.Actor):

    """
    JobProcessor oversees the whole lifecycle of a job
    This includes setup (GraphBuilder), execution (GraphProcessor) and reporting results
    """

    def __init__(
            self, sys_config: _cfg.RuntimeConfig,
            models: _models.ModelLoader, storage: _storage.StorageManager,
            job_key: str, job_config: tp.Optional[_cfg.JobConfig], graph_spec: tp.Optional[_graph.Graph],
            job_log: tp.Optional[_JobLog] = None):

        super().__init__()

        # Either a job config or a pre-built spec is required
        if not job_config and not graph_spec:
            raise _ex.EUnexpected()

        self.job_key = job_key
        self.job_config = job_config
        self.graph_spec = graph_spec
        self._sys_config = sys_config
        self._models = models
        self._storage = storage

        self._job_log = job_log or _JobLog()
        self._log_provider = self._job_log.log_provider
        self._log = self._job_log.log_provider.logger_for_object(self)

        self._log.info(f"New job created for [{self.job_key}]")

        self._resolver = _func.FunctionResolver(models, storage, self._log_provider)
        self._preallocated_ids: tp.Dict[_meta.ObjectType, tp.List[_meta.TagHeader]] = dict()

    def on_start(self):

        self._log.info(f"Starting job [{self.job_key}]")
        self._models.create_scope(self.job_key)

        if self.graph_spec is not None:
            self.actors().send(self.actors().id, "build_graph_succeeded", self.graph_spec)
        else:
            self.actors().spawn(GraphBuilder(self._sys_config, self.job_config, self._log_provider))

    def on_stop(self):

        self._log.info(f"Cleaning up job [{self.job_key}]")
        self._models.destroy_scope(self.job_key)

    def on_signal(self, signal: _actors.Signal) -> tp.Optional[bool]:

        if signal.message == _actors.SignalNames.FAILED and isinstance(signal, _actors.ErrorSignal):

            if isinstance(signal.error, _ex.ETrac):
                error = signal.error

            else:
                cause = str(signal.error) if signal.error is not None else "(unknown)"
                message = f"There was an error in the TRAC execution engine: " + \
                          f"failed node = [{signal.sender}], cause = {cause}"
                error = _ex .ETracInternal(message)
                error.__cause__ = signal.error

            self.actors().send(self.actors().id, "job_failed",  error)

            return True

        return super().on_signal(signal)

    @_actors.Message
    def build_graph_succeeded(self, graph_spec: _graph.Graph, unallocated_ids = None):

        # Save any unallocated IDs to use later (needed for saving the log file)
        self._preallocated_ids = unallocated_ids or dict()

        # Build a new engine context graph from the graph spec
        engine_id = self.actors().parent
        nodes = dict((node_id, _EngineNode(node)) for node_id, node in graph_spec.nodes.items())
        graph = _EngineContext(engine_id, self.job_key, graph_spec.root_id, nodes)

        # Add all the nodes as pending nodes to start
        graph.pending_nodes.update(graph.nodes.keys())

        self.actors().spawn(FunctionResolver(self._resolver, self._log_provider, graph))

        if self.actors().sender != self.actors().id and self.actors().sender != self.actors().parent:
            self.actors().stop(self.actors().sender)

    @_actors.Message
    def resolve_functions_succeeded(self, graph: _EngineContext):

        self.actors().spawn(GraphProcessor(graph, self._resolver, self._log_provider))

        if self.actors().sender != self.actors().id and self.actors().sender != self.actors().parent:
            self.actors().stop(self.actors().sender)

    @_actors.Message
    def job_succeeded(self, job_result: _cfg.JobResult):

        # This will be the last message in the job log file
        self._log.info(f"Job succeeded [{self.job_key}]")

        self._save_job_log_file(job_result)

        self.actors().stop(self.actors().sender)
        self.actors().send_parent("job_succeeded", self.job_key, job_result)

    @_actors.Message
    def job_failed(self, error: Exception):

        # This will be the last message in the job log file
        self._log.error(f"Job failed [{self.job_key}]")

        self.actors().stop(self.actors().sender)

        # For top level jobs, build a failed job result and save the log file
        if self.job_config is not None:

            job_id = self.job_config.jobId
            result_id = self.job_config.resultId
            result_def = _meta.ResultDefinition()
            result_def.jobId = _util.selector_for(job_id)
            result_def.statusCode = _meta.JobStatusCode.FAILED
            result_def.statusMessage = str(error)
            job_result = _cfg.JobResult(job_id, result_id, result_def)

            self._save_job_log_file(job_result)

            self.actors().send_parent("job_failed", self.job_key, error, job_result)

        # For child jobs, just send the error response
        # Result and log file will be handled in the top level job
        else:
            self.actors().send_parent("job_failed", self.job_key, error)

    def _save_job_log_file(self, job_result: _cfg.JobResult):

        # Do not save log files for child jobs, or if a log is not available
        if self._job_log.log_buffer is None:
            if self._job_log.log_file_needed:
                self._log.warning(f"Job log not available for [{self.job_key}]")
            return

        # Saving log files could go into a separate actor

        file_id = self._allocate_id(_meta.ObjectType.FILE)
        storage_id = self._allocate_id(_meta.ObjectType.STORAGE)

        file_name = "trac_job_log_file"
        file_type = _meta.FileType("TXT", "text/plain")

        file_spec = _data.build_file_spec(
            file_id, storage_id,
            file_name, file_type,
            self._sys_config.storage)

        file_def = file_spec.definition
        storage_def = file_spec.storage

        storage_item = storage_def.dataItems[file_def.dataItem].incarnations[0].copies[0]
        storage = self._storage.get_file_storage(storage_item.storageKey)

        with storage.write_byte_stream(storage_item.storagePath) as stream:
            stream.write(self._job_log.log_buffer.getbuffer())
            file_def.size = stream.tell()

        result_def = job_result.result
        result_def.logFileId = _util.selector_for(file_id)

        file_obj = _meta.ObjectDefinition(objectType=_meta.ObjectType.FILE, file=file_def)
        storage_obj = _meta.ObjectDefinition(objectType=_meta.ObjectType.STORAGE, storage=storage_def)

        job_result.objectIds.append(file_id)
        job_result.objectIds.append(storage_id)
        job_result.objects[_util.object_key(file_id)] = file_obj
        job_result.objects[_util.object_key(storage_id)] = storage_obj

    def _allocate_id(self, object_type: _meta.ObjectType):

        preallocated_ids = self._preallocated_ids.get(object_type)

        if preallocated_ids:
            # Preallocated IDs have objectVersion = 0, use a new version to get objectVersion = 1
            return _util.new_object_version(preallocated_ids.pop())
        else:
            return _util.new_object_id(object_type)


class GraphBuilder(_actors.Actor):

    """
    GraphBuilder is a worker (actor) to wrap the GraphBuilder logic from graph_builder.py
    """

    def __init__(self, sys_config: _cfg.RuntimeConfig, job_config: _cfg.JobConfig, log_provider: _logging.LogProvider):

        super().__init__()
        self.sys_config = sys_config
        self.job_config = job_config
        self._log = log_provider.logger_for_object(self)

    def on_start(self):
        self.build_graph(self, self.job_config)

    @_actors.Message
    def build_graph(self, job_config: _cfg.JobConfig):

        self._log.info("Building execution graph")

        graph_builder = _graph.GraphBuilder(self.sys_config, job_config)
        graph_spec = graph_builder.build_job(job_config.job)
        unallocated_ids = graph_builder.unallocated_ids()

        self.actors().reply("build_graph_succeeded", graph_spec, unallocated_ids)


class FunctionResolver(_actors.Actor):

    """
    FunctionResolver is a worker (actors) to wrap the FunctionResolver logic in functions.py
    """

    def __init__(self, resolver: _func.FunctionResolver, log_provider: _logging.LogProvider, graph: _EngineContext):
        super().__init__()
        self.graph = graph
        self._resolver = resolver
        self._log = log_provider.logger_for_object(self)

    def on_start(self):
        self.resolve_functions(self, self.graph)

    @_actors.Message
    def resolve_functions(self, graph: _EngineContext):

        self._log.info("Resolving graph nodes to executable code")

        for node_id, node in graph.nodes.items():
            node.function = self._resolver.resolve_node(node.node)

        self.actors().reply("resolve_functions_succeeded", graph)


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

    def __init__(self, graph: _EngineContext, resolver: _func.FunctionResolver, log_provider: _logging.LogProvider):
        super().__init__()
        self.graph = graph
        self.root_id_ = graph.root_id
        self.processors: tp.Dict[NodeId, _actors.ActorId] = dict()
        self._resolver = resolver
        self._log = log_provider.logger_for_object(self)
        self._graph_logger = GraphLogger(log_provider)
        self._node_logger = NodeLogger(log_provider)

    def on_start(self):

        self._log.info("Begin processing graph")
        self.actors().send(self.actors().id, "submit_viable_nodes")

    @_actors.Message
    def submit_viable_nodes(self):

        node_processors = dict()

        def process_graph(graph: _EngineContext) -> _EngineContext:

            processed_graph = cp.copy(graph)
            processed_graph.nodes = cp.copy(graph.nodes)

            # Start by removing any nodes that are no longer needed
            for node_id, node in graph.nodes.items():
                if node_id in graph.succeeded_nodes and not self._is_required_node(node, graph):
                    node = processed_graph.nodes.pop(node_id)
                    self._node_logger.log_node_evict(node)
                    del node

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

                    # This is very basic separation of different node types
                    # Model and data nodes map to different thread pools in the actors engine
                    # There is scope for a much more sophisticated approach, with prioritized scheduling

                    if isinstance(node.node, _graph.ChildJobNode):
                        processor = ChildJobNodeProcessor(processed_graph, node, self._node_logger)
                    elif isinstance(node.node, _graph.RunModelNode) or isinstance(node.node, _graph.ImportModelNode):
                        processor = ModelNodeProcessor(processed_graph, node, self._node_logger)
                    elif isinstance(node.node, _graph.LoadDataNode) or isinstance(node.node, _graph.SaveDataNode):
                        processor = DataNodeProcessor(processed_graph, node, self._node_logger)
                    else:
                        processor = NodeProcessor(processed_graph, node, self._node_logger)

                    # New nodes can be launched with the updated graph
                    # Anything that was pruned is not needed by the new node
                    node_ref = self.actors().spawn(processor)
                    node_processors[node_id] = node_ref

                    pending_nodes.discard(node_id)
                    active_nodes.add(node_id)

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

    @_actors.Message
    def update_graph(self, requestor_id: NodeId, update: _graph.GraphUpdate):

        new_graph = cp.copy(self.graph)
        new_graph.nodes = cp.copy(new_graph.nodes)

        # Attempt to insert a duplicate node is always an error
        node_collision = list(filter(lambda nid: nid in self.graph.nodes, update.nodes))

        # Only allow adding deps to pending nodes for now (adding deps to active nodes will require more work)
        dep_collision = list(filter(lambda nid: nid not in self.graph.pending_nodes, update.dependencies))

        # Only allow adding deps to new nodes (deps to existing nodes should not be part of an update)
        dep_invalid = list(filter(
            lambda ds: any(filter(lambda d: d.node_id not in update.nodes, ds)),
            update.dependencies.values()))

        if any(node_collision) or any(dep_collision) or any(dep_invalid):

            self._log.error(f"Node collision during graph update (requested by {requestor_id})")
            self._log.error(f"Duplicate node IDs: {node_collision or 'None'}")
            self._log.error(f"Dependency updates for dead nodes: {dep_collision or 'None'}")
            self._log.error(f"Dependencies added for existing nodes: {dep_invalid or 'None'}")

            # Set an error on the node, and wait for it to complete normally
            # The error will be picked up when the result is recorded
            # If dependencies are added for an active node, more signalling will be needed
            requestor = cp.copy(new_graph.nodes[requestor_id])
            requestor.error = _ex.ETracInternal("Node collision during graph update")
            new_graph.nodes[requestor_id] = requestor

            self.graph = new_graph

            return

        new_graph.pending_nodes = cp.copy(new_graph.pending_nodes)

        for node_id, node in update.nodes.items():
            self._graph_logger.log_node_add(node)
            node_func = self._resolver.resolve_node(node)
            new_node = _EngineNode(node, node_func)
            new_graph.nodes[node_id] = new_node
            new_graph.pending_nodes.add(node_id)

        for node_id, deps in update.dependencies.items():
            engine_node = cp.copy(new_graph.nodes[node_id])
            engine_node.dependencies = cp.copy(engine_node.dependencies)
            for dep in deps:
                self._graph_logger.log_dependency_add(node_id, dep.node_id)
                engine_node.dependencies[dep.node_id] = dep.dependency_type
            new_graph.nodes[node_id] = engine_node

        self.graph = new_graph

        self.actors().send(self.actors().id, "submit_viable_nodes")

    @classmethod
    def _is_required_node(cls, node: _EngineNode, graph: _EngineContext):

        if node.node.id not in graph.succeeded_nodes:
            return False

        def live_node(nid_n):
            nid_, _ = nid_n
            return nid_ in graph.active_nodes or nid_ in graph.pending_nodes

        def use_target(nid_n):
            _, n_ = nid_n
            return node.node.id in n_.dependencies

        return any(map(use_target, filter(live_node, graph.nodes.items())))

    @classmethod
    def _is_viable_node(cls, node: _EngineNode, graph: _EngineContext):

        return all(dep in graph.succeeded_nodes for dep in node.dependencies)

    @classmethod
    def _upstream_failure(cls, node: _EngineNode, graph: _EngineContext):

        return any(not dep_type.tolerant and dep in graph.failed_nodes
                   for dep, dep_type in node.dependencies.items())

    @_actors.Message
    def node_succeeded(self, node_id: NodeId, result):

        # Child node is no longer needed, it can be stopped to release resources
        processor = self.processors.pop(node_id)
        self.actors().stop(processor)

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

        if isinstance(node.node, _graph.ContextPopNode):
            self._update_results(results, context_pop=node.node.id.namespace)
        else:
            self._update_results(results)

    @_actors.Message
    def node_failed(self, node_id: NodeId, error):

        # Child node is no longer needed, it can be stopped to release resources
        processor = self.processors.pop(node_id)
        self.actors().stop(processor)

        old_node = self.graph.nodes[node_id]
        node = cp.copy(old_node)
        node.complete = True
        node.error = error

        self._update_results({node_id: node})

    def _update_results(self, updates: tp.Dict[NodeId, _EngineNode], context_pop: _graph.NodeNamespace = None):

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

        # For context pop, remove all the nodes inside the pop context
        # The graph builder is responsible for consistency, i.e. no dependencies across contexts
        if context_pop:
            for node_id in list(filter(lambda n: n.namespace == context_pop, nodes)):
                nodes.pop(node_id)

        self.graph = self.graph.with_updates(
            nodes, pending_nodes, active_nodes,
            succeeded_nodes, failed_nodes)

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
                job_result = self.graph.nodes[self.graph.root_id].result
                self.actors().send_parent("job_succeeded", job_result)


class NodeProcessor(_actors.Actor):

    """
    Processor responsible for running individual nodes in an execution graph
    """

    __NONE_TYPE = type(None)

    def __init__(self, graph: _EngineContext, node: _EngineNode, node_logger: "NodeLogger"):
        super().__init__()
        self.graph = graph
        self.node = node
        self.node_id = node.node.id
        self.node_logger = node_logger


    def on_start(self):

        self.actors().send(self.actors().id, "evaluate_node")

    def on_stop(self):

        # Something in the engine occasionally holds onto node processors
        # These hold a copy of the graph with the pre-execution version of their own engine node
        # I.e. the engine node that gets retained does not hold any data

        # This is not a serious issue in most cases, but it is still better to release the resources
        # We can do this by unsetting the references to self.node and self.graph

        # It would be good to find out where the reference to the processor is being held and fix at source
        # We could also add some accounting to the _EngineNode class

        self.node = None
        self.graph = None

    @_actors.Message
    def graph_event(self):
        pass

    @_actors.Message
    def stream_event(self):
        pass

    @_actors.Message
    def evaluate_node(self):

        try:

            self.node_logger.log_node_start(self.node)

            # Context contains only node states available when the context is set up
            ctx = NodeContextImpl(self.graph.nodes)

            # Callback remains valid because it only lives inside the call stack for this message
            callback = NodeCallbackImpl(self.actors(), self.node_id)

            # Execute the node function
            result = self.node.function(ctx, callback)

            self._check_result_type(result)

            self.node_logger.log_node_succeeded(self.node)

            self.actors().send_parent("node_succeeded", self.node_id, result)

        except Exception as e:

            self.node_logger.log_node_failed(self.node, e)

            self.actors().send_parent("node_failed", self.node_id, e)

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


class ModelNodeProcessor(NodeProcessor):

    def __init__(self, graph: _EngineContext, node: _EngineNode, node_logger: "NodeLogger"):
        super().__init__(graph, node, node_logger)


class DataNodeProcessor(NodeProcessor):

    def __init__(self, graph: _EngineContext, node: _EngineNode, node_logger: "NodeLogger"):
        super().__init__(graph, node, node_logger)


class ChildJobNodeProcessor(NodeProcessor):

    def __init__(self, graph: _EngineContext, node: _EngineNode, node_logger: "NodeLogger"):
        super().__init__(graph, node, node_logger)

    @_actors.Message
    def evaluate_node(self):

        self.node_logger.log_node_start(self.node)

        job_id = self.node.node.job_id  # noqa
        job_key = _util.object_key(job_id)
        parent_key = self.graph.job_key

        node_id = self.actors().id

        def success_callback(ctx, _, result):
            ctx.send(node_id, "child_job_succeeded", result)

        def failure_callback(ctx, _, error):
            ctx.send(node_id, "child_job_failed", error)

        monitor = JobMonitor(job_key, success_callback, failure_callback)
        monitor_id = self.actors().spawn(monitor)

        graph_spec: _graph.Graph = self.node.node.graph  # noqa

        self.actors().send(self.graph.engine_id, "submit_child_job", parent_key, job_id, graph_spec, monitor_id)

    @_actors.Message
    def child_job_succeeded(self, job_result: _cfg.JobResult):

        self._check_result_type(job_result)

        self.node_logger.log_node_succeeded(self.node)

        self.actors().send_parent("node_succeeded", self.node_id, job_result)

    @_actors.Message
    def child_job_failed(self, job_error: Exception):

        self.node_logger.log_node_failed(self.node, job_error)

        self.actors().send_parent("node_failed", self.node_id, job_error)


class GraphLogger:

    """
    Log the activity of the GraphProcessor
    """

    def __init__(self, log_provider: _logging.LogProvider):
        self._log = log_provider.logger_for_class(GraphProcessor)

    def log_node_add(self, node: _graph.Node):

        node_name = node.id.name
        namespace = node.id.namespace

        self._log.info(f"ADD {self._func_type(node)} [{node_name}] / {namespace}")

    def log_dependency_add(self, node_id: NodeId, dep_id: NodeId):

        if node_id.namespace == dep_id.namespace:
            self._log.info(f"ADD DEPENDENCY [{node_id.name}] -> [{dep_id.name}] / {node_id.namespace}")
        else:
            self._log.info(f"ADD DEPENDENCY [{node_id.name}] / {node_id.namespace} -> [{dep_id.name}] / {dep_id.namespace}")

    @classmethod
    def _func_type(cls, node: _graph.Node):

        func_type = type(node)
        return func_type.__name__[:-4]


class NodeLogger:

    """
    Log the activity of the NodeProcessor
    """

    # Separate out the logic for logging nodes, so the NodeProcessor itself stays a bit cleaner

    def __init__(self, log_provider: _logging.LogProvider):
        self._log = log_provider.logger_for_class(NodeProcessor)

    class LoggingType(enum.Enum):
        DEFAULT = 0
        STATIC_VALUE = 1
        PUSH_POP = 2
        SIMPLE_MAPPING = 3
        MODEL = 4

    def log_node_start(self, node: _EngineNode):

        logging_type = self._logging_type(node)
        node_name = node.node.id.name
        namespace = node.node.id.namespace

        if logging_type == self.LoggingType.STATIC_VALUE:
            self._log.info(f"SET {self._value_type(node)} [{node_name}] / {namespace}")

        elif logging_type in [self.LoggingType.SIMPLE_MAPPING]:
            self._log.info(f"MAP {self._value_type(node)} [{self._mapping_source(node)}] -> [{node_name}] / {namespace}")

        else:
            self._log.info(f"START {self._func_type(node)} [{node_name}] / {namespace}")

    def log_node_succeeded(self, node: _EngineNode):

        logging_type = self._logging_type(node)
        node_name = node.node.id.name
        namespace = node.node.id.namespace

        if logging_type in [self.LoggingType.STATIC_VALUE, self.LoggingType.SIMPLE_MAPPING]:
            return

        if logging_type == self.LoggingType.PUSH_POP:
            self._log_push_pop_node_details(node.node)  # noqa

        if logging_type == self.LoggingType.MODEL:
            self._log_model_node_details(node.node)  # noqa

        self._log.info(f"DONE {self._func_type(node)} [{node_name}] / {namespace}")

    def log_node_failed(self, node: _EngineNode, e: Exception):

        node_name = node.node.id.name
        namespace = node.node.id.namespace

        self._log.error(f"FAILED {self._func_type(node)} [{node_name}] / {namespace}")
        self._log.exception(e)

    def log_node_evict(self, node: _EngineNode):

        logging_type = self._logging_type(node)
        node_name = node.node.id.name
        namespace = node.node.id.namespace

        if logging_type in [self.LoggingType.STATIC_VALUE, self.LoggingType.SIMPLE_MAPPING]:
            return

        self._log.info(f"EVICT {self._func_type(node)} [{node_name}] / {namespace}")

    def _log_push_pop_node_details(self, node: tp.Union[_graph.ContextPushNode, _graph.ContextPopNode]):

        push_or_pop = "PUSH" if isinstance(node, _graph.ContextPushNode) else "POP"
        direction = "->" if isinstance(node, _graph.ContextPushNode) else "<-"

        for inner_id, outer_id in node.mapping.items():
            item_type = self._type_str(inner_id.result_type)
            msg = f"{push_or_pop} {item_type} [{outer_id.name}] {direction} [{inner_id.name}] / {node.id.namespace}"
            self._log.info(msg)

    def _log_model_node_details(self, node: _graph.RunModelNode):

        self._type_str(_data.DataView)

        for output in node.model_def.outputs:
            result_type = self._type_str(_data.DataView)
            msg = f"RESULT {result_type} [{output}] / {node.bundle_namespace}"
            self._log.info(msg)

    @classmethod
    def _logging_type(cls, node: _EngineNode) -> LoggingType:

        if isinstance(node.node, _graph.StaticValueNode):
            return cls.LoggingType.STATIC_VALUE

        if isinstance(node.node, _graph.ContextPushNode) or isinstance(node.node, _graph.ContextPopNode):
            return cls.LoggingType.PUSH_POP

        if isinstance(node.node, _graph.IdentityNode) or isinstance(node.node, _graph.BundleItemNode):
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

        # Remove "Node" from "xxxNode"

        func_type = type(node.node)
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


class NodeCallbackImpl(_func.NodeCallback):

    """
    Callback impl is passed to node functions so they can call into the engine
    It is only valid as long as the node function runs inside the call stack of a single message
    """

    def __init__(self, actor_ctx: _actors.ActorContext, node_id: NodeId):
        self.__actor_ctx = actor_ctx
        self.__node_id = node_id

    def send_graph_update(self, update: _graph.GraphUpdate):
        self.__actor_ctx.send_parent("update_graph", self.__node_id, update)
