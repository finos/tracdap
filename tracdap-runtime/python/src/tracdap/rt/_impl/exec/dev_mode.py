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

import re
import typing as tp
import copy
import pathlib

import tracdap.rt.api as _api
import tracdap.rt.config as _cfg
import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.config_parser as _cfg_p
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.models as _models
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.type_system as _types
import tracdap.rt._impl.core.util as _util


DEV_MODE_JOB_CONFIG = [
    re.compile(r"job\.\w+\.parameters\.\w+"),
    re.compile(r"job\.\w+\.inputs\.\w+"),
    re.compile(r"job\.\w+\.outputs\.\w+"),
    re.compile(r"job\.\w+\.models\.\w+"),
    re.compile(r"job\.\w+\.model"),
    re.compile(r"job\.\w+\.flow"),

    re.compile(r".*\.jobs\[\d+]\.\w+\.parameters\.\w+"),
    re.compile(r".*\.jobs\[\d+]\.\w+\.inputs\.\w+"),
    re.compile(r".*\.jobs\[\d+]\.\w+\.outputs\.\w+"),
    re.compile(r".*\.jobs\[\d+]\.\w+\.models\.\w+"),
    re.compile(r".*\.jobs\[\d+]\.\w+\.model"),
    re.compile(r".*\.jobs\[\d+]\.\w+\.flow")
]

DEV_MODE_SYS_CONFIG = []


class DevModeTranslator:

    _log: tp.Optional[_logging.Logger] = None

    @classmethod
    def translate_sys_config(cls, sys_config: _cfg.RuntimeConfig, config_mgr: _cfg_p.ConfigManager):

        cls._log.info(f"Applying dev mode config translation to system config")

        # TODO: In code gen, default object types to a new object unless the field is marked as optional
        # This would match the general semantics of protobuf
        if sys_config.storage is None:
            sys_config.storage = _cfg.StorageConfig()

        sys_config = cls._add_integrated_repo(sys_config)
        sys_config = cls._process_storage(sys_config, config_mgr)

        return sys_config

    @classmethod
    def _add_integrated_repo(cls, sys_config: _cfg.RuntimeConfig) -> _cfg.RuntimeConfig:

        # Add the integrated model repo trac_integrated

        integrated_repo_config = _cfg.PluginConfig(
            protocol="integrated",
            properties={})

        sys_config.repositories["trac_integrated"] = integrated_repo_config

        return sys_config

    @classmethod
    def _process_storage(
            cls, sys_config: _cfg.RuntimeConfig,
            config_mgr: _cfg_p.ConfigManager):

        storage_config = copy.deepcopy(sys_config.storage)
        storage_config.defaultLayout = _meta.StorageLayout.DEVELOPER_LAYOUT

        for bucket_key, bucket_config in storage_config.buckets.items():
            storage_config.buckets[bucket_key] = cls._resolve_storage_location(
                bucket_key, bucket_config, config_mgr)

        for bucket_key, bucket_config in storage_config.external.items():
            storage_config.external[bucket_key] = cls._resolve_storage_location(
                bucket_key, bucket_config, config_mgr)

        sys_config = copy.copy(sys_config)
        sys_config.storage = storage_config

        return sys_config

    @classmethod
    def _resolve_storage_location(cls, bucket_key, bucket_config, config_mgr: _cfg_p.ConfigManager):

        if bucket_config.protocol != "LOCAL":
            return bucket_config

        if "rootPath" not in bucket_config.properties:
            return bucket_config

        root_path = pathlib.Path(bucket_config.properties["rootPath"])

        if root_path.is_absolute():
            return bucket_config

        cls._log.info(f"Resolving relative path for [{bucket_key}] local storage...")

        sys_config_path = config_mgr.config_dir_path()
        if sys_config_path is not None:
            absolute_path = sys_config_path.joinpath(root_path).resolve()
            if absolute_path.exists():
                cls._log.info(f"Resolved [{root_path}] -> [{absolute_path}]")
                bucket_config.properties["rootPath"] = str(absolute_path)
                return bucket_config

        cwd = pathlib.Path.cwd()
        absolute_path = cwd.joinpath(root_path).resolve()

        if absolute_path.exists():
            cls._log.info(f"Resolved [{root_path}] -> [{absolute_path}]")
            bucket_config.properties["rootPath"] = str(absolute_path)
            return bucket_config

        msg = f"Failed to resolve relative storage path [{root_path}]"
        cls._log.error(msg)
        raise _ex.EConfigParse(msg)


    def __init__(
            self, sys_config: _cfg.RuntimeConfig, config_mgr: _cfg_p.ConfigManager, scratch_dir: pathlib.Path = None,
            model_loader: _models.ModelLoader = None, storage_manager: _storage.StorageManager = None):

        self._sys_config = sys_config
        self._config_mgr = config_mgr
        self._model_loader = model_loader or _models.ModelLoader(self._sys_config, scratch_dir)
        self._storage_manager = storage_manager or _storage.StorageManager(self._sys_config)

    def translate_job_config(
            self, job_config: _cfg.JobConfig,
            model_class: tp.Optional[_api.TracModel.__class__] = None) \
            -> _cfg.JobConfig:

        try:
            self._log.info(f"Applying dev mode config translation to job config")
            self._model_loader.create_scope("DEV_MODE_TRANSLATION")

            job_config = copy.deepcopy(job_config)
            job_def = job_config.job

            # Protobuf semantics for a blank jobId should be an object, but objectId will be an empty string
            if not job_config.jobId or not job_config.jobId.objectId:
                job_config = self._process_job_id(job_config)

            job_config, job_def = self.translate_job_def(job_config, job_def, model_class)
            job_config.job = job_def

            return job_config

        finally:
            self._model_loader.destroy_scope("DEV_MODE_TRANSLATION")

    def translate_job_def(
            self, job_config: _cfg.JobConfig, job_def: _meta.JobDefinition,
            model_class: tp.Optional[_api.TracModel.__class__] = None) \
            -> tp.Tuple[_cfg.JobConfig, _meta.JobDefinition]:

        if job_def.jobType is None or job_def.jobType == _meta.JobType.JOB_TYPE_NOT_SET:
            job_def = self._process_job_type(job_def)

        # Load and populate any models provided as a Python class or class name
        job_config, job_def = self._process_models(job_config, job_def, model_class)

        # Fow flows, load external flow definitions then perform auto-wiring and type inference
        if job_def.jobType == _meta.JobType.RUN_FLOW:
            job_config, job_def = self._process_flow_definition(job_config, job_def)

        if job_def.jobType == _meta.JobType.JOB_GROUP:
            job_config, job_def = self.translate_job_group(job_config, job_def)

        # Apply processing to the parameters, inputs and outputs
        job_config, job_def = self._process_parameters(job_config, job_def)
        job_config, job_def = self._process_inputs_and_outputs(job_config, job_def)

        return job_config, job_def

    def translate_job_group(
            self, job_config: _cfg.JobConfig, job_def: _meta.JobDefinition) \
            -> tp.Tuple[_cfg.JobConfig, _meta.JobDefinition]:

        job_group = job_def.jobGroup

        if job_group.jobGroupType is None or job_group.jobGroupType == _meta.JobGroupType.JOB_GROUP_TYPE_NOT_SET:
            job_group = self._process_job_group_type(job_group)

        group_details = self._get_job_group_detail(job_group)

        if hasattr(group_details, "jobs"):
            child_jobs = []
            for child_def in group_details.jobs:
                job_config, child_def = self.translate_job_def(job_config, child_def)
                child_jobs.append(child_def)
            group_details.jobs = child_jobs

        job_def.jobGroup = job_group

        return job_config, job_def

    @classmethod
    def _add_job_metadata(
            cls, job_config: _cfg.JobConfig,
            obj_id: _meta.TagHeader, obj: _meta.ObjectDefinition) \
            -> _cfg.JobConfig:

        obj_key = _util.object_key(obj_id)
        job_config.objects[obj_key] = obj

        return job_config

    @classmethod
    def _process_job_id(cls, job_config: _cfg.JobConfig) -> _cfg.JobConfig:

        job_id = _util.new_object_id(_meta.ObjectType.JOB)
        result_id = _util.new_object_id(_meta.ObjectType.RESULT)

        cls._log.info(f"Assigning job ID = [{_util.object_key(job_id)}]")
        cls._log.info(f"Assigning result ID = [{_util.object_key(result_id)}]")

        job_config.jobId = job_id
        job_config.resultId = result_id

        return job_config

    @classmethod
    def _process_job_type(cls, job_def: _meta.JobDefinition):

        if job_def.runModel is not None:
            job_type = _meta.JobType.RUN_MODEL

        elif job_def.runFlow is not None:
            job_type = _meta.JobType.RUN_FLOW

        elif job_def.importModel is not None:
            job_type = _meta.JobType.IMPORT_MODEL

        elif job_def.importData is not None:
            job_type = _meta.JobType.IMPORT_DATA

        elif job_def.exportData is not None:
            job_type = _meta.JobType.EXPORT_DATA

        elif job_def.jobGroup is not None:
            job_type = _meta.JobType.JOB_GROUP

        else:
            cls._log.error("Could not infer job type")
            raise _ex.EConfigParse("Could not infer job type")

        cls._log.info(f"Inferred job type = [{job_type.name}]")

        job_def = copy.copy(job_def)
        job_def.jobType = job_type

        return job_def

    @classmethod
    def _process_job_group_type(cls, job_group: _meta.JobGroup) -> _meta.JobGroup:

        if job_group.sequential is not None:
            job_group_type = _meta.JobGroupType.SEQUENTIAL_JOB_GROUP

        elif job_group.parallel is not None:
            job_group_type = _meta.JobGroupType.PARALLEL_JOB_GROUP

        else:
            cls._log.error("Could not infer job group type")
            raise _ex.EConfigParse("Could not infer job group type")

        cls._log.info(f"Inferred job group type = [{job_group_type.name}]")

        job_group = copy.copy(job_group)
        job_group.jobGroupType = job_group_type

        return job_group

    @classmethod
    def _get_job_detail(cls, job_def: _meta.JobDefinition):

        if job_def.jobType == _meta.JobType.RUN_MODEL:
            return job_def.runModel

        if job_def.jobType == _meta.JobType.RUN_FLOW:
            return job_def.runFlow

        if job_def.jobType == _meta.JobType.IMPORT_MODEL:
            return job_def.importModel

        if job_def.jobType == _meta.JobType.IMPORT_DATA:
            return job_def.importData

        if job_def.jobType == _meta.JobType.EXPORT_DATA:
            return job_def.exportData

        if job_def.jobType == _meta.JobType.JOB_GROUP:
            return job_def.jobGroup

        raise _ex.EConfigParse(f"Could not get job details for job type [{job_def.jobType}]")

    @classmethod
    def _get_job_group_detail(cls, job_group: _meta.JobGroup):

        if job_group.jobGroupType == _meta.JobGroupType.SEQUENTIAL_JOB_GROUP:
            return job_group.sequential

        if job_group.jobGroupType == _meta.JobGroupType.PARALLEL_JOB_GROUP:
            return job_group.parallel

        raise _ex.EConfigParse(f"Could not get job group details for group type [{job_group.jobGroupType}]")

    def _process_models(
            self, job_config: _cfg.JobConfig, job_def: _meta.JobDefinition,
            model_class: tp.Optional[_api.TracModel.__class__]) \
            -> tp.Tuple[_cfg.JobConfig, _meta.JobDefinition]:

        # This processing works on the assumption that job details follow a convention for addressing models
        # Jobs requiring a single model have a field called "model"
        # Jobs requiring multiple models have a field called "models@, which is a dict

        job_detail = self._get_job_detail(job_def)

        # If a model class is supplied in code, use that to generate the model def
        if model_class is not None:

            # Passing a model class via launch_model() is only supported for job types with a single model
            if not hasattr(job_detail, "model"):
                raise _ex.EJobValidation(f"Job type [{job_def.jobType}] cannot be launched using launch_model()")

            model_id, model_obj = self._generate_model_for_class(model_class)
            job_detail.model = _util.selector_for(model_id)
            job_config = self._add_job_metadata(job_config, model_id, model_obj)

        # Otherwise look for models specified as a single string, and take that as the entry point
        else:

            # Jobs with a single model
            if hasattr(job_detail, "model") and isinstance(job_detail.model, str):
                model_id, model_obj = self._generate_model_for_entry_point(job_detail.model)  # noqa
                job_detail.model = _util.selector_for(model_id)
                job_config = self._add_job_metadata(job_config, model_id, model_obj)

            elif hasattr(job_detail, "model") and isinstance(job_detail.model, _meta.TagSelector):
                if job_detail.model.objectType == _meta.ObjectType.OBJECT_TYPE_NOT_SET:
                    error = f"Missing required property [model] for job type [{job_def.jobType.name}]"
                    self._log.error(error)
                    raise _ex.EJobValidation(error)

            # Jobs with multiple models
            elif hasattr(job_detail, "models") and isinstance(job_detail.models, dict):
                for model_key, model_detail in job_detail.models.items():
                    if isinstance(model_detail, str):
                        model_id, model_obj = self._generate_model_for_entry_point(model_detail)
                        job_detail.models[model_key] = _util.selector_for(model_id)
                        job_config = self._add_job_metadata(job_config, model_id, model_obj)

        return job_config, job_def

    def _generate_model_for_class(
            self, model_class: _api.TracModel.__class__) \
            -> (_meta.TagHeader, _meta.ObjectDefinition):

        model_entry_point = f"{model_class.__module__}.{model_class.__name__}"
        return self._generate_model_for_entry_point(model_entry_point)

    def _generate_model_for_entry_point(
            self, model_entry_point: str) \
            -> (_meta.TagHeader, _meta.ObjectDefinition):

        model_id = _util.new_object_id(_meta.ObjectType.MODEL)
        model_key = _util.object_key(model_id)

        self._log.info(f"Generating model definition for [{model_entry_point}] with ID = [{model_key}]")

        skeleton_modeL_def = _meta.ModelDefinition(  # noqa
            language="python",
            repository="trac_integrated",
            entryPoint=model_entry_point,

            parameters={},
            inputs={},
            outputs={})

        model_class = self._model_loader.load_model_class("DEV_MODE_TRANSLATION", skeleton_modeL_def)
        model_def = self._model_loader.scan_model(skeleton_modeL_def, model_class)

        model_object = _meta.ObjectDefinition(
            objectType=_meta.ObjectType.MODEL,
            model=model_def)

        return model_id, model_object

    def _process_flow_definition(
            self, job_config: _cfg.JobConfig, job_def: _meta.JobDefinition) \
            -> tp.Tuple[_cfg.JobConfig, _meta.JobDefinition]:

        flow_details = job_def.runFlow.flow

        # Do not apply translation if flow is specified as an object ID / selector (assume full config is supplied)
        if isinstance(flow_details, _meta.TagHeader) or isinstance(flow_details, _meta.TagSelector):
            return job_config, job_def

        # Otherwise, flow is specified as the path to dev-mode flow definition
        if not isinstance(flow_details, str):
            err = f"Invalid config value for [job.runFlow.flow]: Expected path or tag selector, got [{flow_details}])"
            self._log.error(err)
            raise _ex.EConfigParse(err)

        flow_id = _util.new_object_id(_meta.ObjectType.FLOW)
        flow_key = _util.object_key(flow_id)

        self._log.info(f"Generating flow definition from [{flow_details}] with ID = [{flow_key}]")

        flow_def = self._config_mgr.load_config_object(flow_details, _meta.FlowDefinition)

        # Validate models against the flow (this could move to _impl.validation and check prod jobs as well)
        self._check_models_for_flow(flow_def, job_def, job_config)

        # Auto-wiring and inference only applied to externally loaded flows for now
        flow_def = self._autowire_flow(flow_def, job_def, job_config)
        flow_def = self._apply_type_inference(flow_def, job_def, job_config)

        flow_obj = _meta.ObjectDefinition(
            objectType=_meta.ObjectType.FLOW,
            flow=flow_def)

        job_def = copy.copy(job_def)
        job_def.runFlow = copy.copy(job_def.runFlow)
        job_def.runFlow.flow = _util.selector_for(flow_id)

        job_config = copy.copy(job_config)
        job_config.objects = copy.copy(job_config.objects)
        job_config = self._add_job_metadata(job_config, flow_id, flow_obj)

        return job_config, job_def

    @classmethod
    def _check_models_for_flow(cls, flow: _meta.FlowDefinition, job_def: _meta.JobDefinition, job_config: _cfg.JobConfig):

        model_nodes = dict(filter(lambda n: n[1].nodeType == _meta.FlowNodeType.MODEL_NODE, flow.nodes.items()))

        missing_models = list(filter(lambda m: m not in job_def.runFlow.models, model_nodes.keys()))
        extra_models = list(filter(lambda m: m not in model_nodes, job_def.runFlow.models.keys()))

        if any(missing_models):
            error = f"Missing models in job definition: {', '.join(missing_models)}"
            cls._log.error(error)
            raise _ex.EJobValidation(error)

        if any (extra_models):
            error = f"Extra models in job definition: {', '.join(extra_models)}"
            cls._log.error(error)
            raise _ex.EJobValidation(error)

        for model_name, model_node in model_nodes.items():

            model_selector = job_def.runFlow.models[model_name]
            model_obj = _util.get_job_metadata(model_selector, job_config)

            model_inputs = set(model_obj.model.inputs.keys())
            model_outputs = set(model_obj.model.outputs.keys())

            if model_inputs != set(model_node.inputs) or model_outputs != set(model_node.outputs):
                error = f"The model supplied for [{model_name}] does not match the flow definition"
                cls._log.error(error)
                raise _ex.EJobValidation(error)

    @classmethod
    def _autowire_flow(cls, flow: _meta.FlowDefinition, job_def: _meta.JobDefinition, job_config: _cfg.JobConfig):

        job = job_def.runFlow
        nodes = copy.copy(flow.nodes)
        edges: tp.Dict[str, _meta.FlowEdge] = dict()

        sources: tp.Dict[str, _meta.FlowSocket] = dict()
        duplicates: tp.Dict[str, tp.List[_meta.FlowSocket]] = dict()
        errors: tp.Dict[str, str] = dict()

        def socket_key(socket: _meta.FlowSocket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        # Before starting, add any edges defined explicitly in the flow
        # These take precedence over auto-wired edges
        for edge in flow.edges:
            edges[socket_key(edge.target)] = edge

        def add_source(name: str, socket: _meta.FlowSocket):
            if name in duplicates:
                duplicates[name].append(socket)
            elif name in sources:
                duplicates[name] = [sources[name], socket]
                del sources[name]
            else:
                sources[name] = socket

        def add_param_to_flow(nodel_node: str, param: str):
            target = f"{nodel_node}.{param}"
            if target not in edges and param not in nodes:
                param_node = _meta.FlowNode(_meta.FlowNodeType.PARAMETER_NODE)
                nodes[param] = param_node
                socket = _meta.FlowSocket(param)
                add_source(param, socket)

        def add_edge(target: _meta.FlowSocket):
            target_key = socket_key(target)
            if target_key in edges:
                return
            target_name = target.socket if target.socket else target.node
            if target_name in sources:
                edges[target_key] = _meta.FlowEdge(sources[target_name], target)
            elif target_name in duplicates:
                sources_info = ', '.join(map(socket_key, duplicates[target_name]))
                errors[target_key] = f"Flow target {target_name} is provided by multiple nodes: [{sources_info}]"
            else:
                errors[target_key] = f"Flow target {target_name} is not provided by any node"

        for node_name, node in flow.nodes.items():
            if node.nodeType == _meta.FlowNodeType.INPUT_NODE or node.nodeType == _meta.FlowNodeType.PARAMETER_NODE:
                add_source(node_name, _meta.FlowSocket(node_name))
            if node.nodeType == _meta.FlowNodeType.MODEL_NODE:
                for model_output in node.outputs:
                    add_source(model_output, _meta.FlowSocket(node_name, model_output))
                # Generate node param sockets needed by the model
                if node_name in job.models:
                    model_selector = job.models[node_name]
                    model_obj = _util.get_job_metadata(model_selector, job_config)
                    for param_name in model_obj.model.parameters:
                        add_param_to_flow(node_name, param_name)
                        if param_name not in node.parameters:
                            node.parameters.append(param_name)

        # Look at the new set of nodes, which includes any added by auto-wiring
        for node_name, node in nodes.items():
            if node.nodeType == _meta.FlowNodeType.OUTPUT_NODE:
                add_edge(_meta.FlowSocket(node_name))
            if node.nodeType == _meta.FlowNodeType.MODEL_NODE:
                for model_input in node.inputs:
                    add_edge(_meta.FlowSocket(node_name, model_input))
                for model_param in node.parameters:
                    add_edge(_meta.FlowSocket(node_name, model_param))

        if any(errors):

            err_line_break = "\n" if len(errors) > 1 else ""
            err_details = "\n".join(errors.values())
            err = f"Flow could not be auto-wired: {err_line_break}{err_details}"

            cls._log.error(err)
            raise _ex.EConfigParse(err)

        autowired_flow = copy.copy(flow)
        autowired_flow.nodes = nodes
        autowired_flow.edges = list(edges.values())

        return autowired_flow

    @classmethod
    def _apply_type_inference(
            cls, flow: _meta.FlowDefinition,
            job_def: _meta.JobDefinition, job_config: _cfg.JobConfig) \
            -> _meta.FlowDefinition:

        updated_flow = copy.copy(flow)
        updated_flow.parameters = copy.copy(flow.parameters)
        updated_flow.inputs = copy.copy(flow.inputs)
        updated_flow.outputs = copy.copy(flow.outputs)

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        # Build a map of edges by source socket, mapping to all edges flowing from that source
        edges_by_source = {socket_key(edge.source): [] for edge in flow.edges}
        edges_by_target = {socket_key(edge.target): [] for edge in flow.edges}
        for edge in flow.edges:
            edges_by_source[socket_key(edge.source)].append(edge.target)
            edges_by_target[socket_key(edge.target)].append(edge.source)

        for node_name, node in flow.nodes.items():

            if node.nodeType == _meta.FlowNodeType.PARAMETER_NODE and node_name not in flow.parameters:
                targets = edges_by_source.get(node_name) or []
                model_parameter = cls._infer_parameter(node_name, targets, job_def, job_config)
                updated_flow.parameters[node_name] = model_parameter

            if node.nodeType == _meta.FlowNodeType.INPUT_NODE and node_name not in flow.inputs:
                targets = edges_by_source.get(node_name) or []
                model_input = cls._infer_input_schema(node_name, targets, job_def, job_config)
                updated_flow.inputs[node_name] = model_input

            if node.nodeType == _meta.FlowNodeType.OUTPUT_NODE and node_name not in flow.outputs:
                sources = edges_by_target.get(node_name) or []
                model_output = cls._infer_output_schema(node_name, sources, job_def, job_config)
                updated_flow.outputs[node_name] = model_output

        return updated_flow

    @classmethod
    def _infer_parameter(
            cls, param_name: str, targets: tp.List[_meta.FlowSocket],
            job_def: _meta.JobDefinition, job_config: _cfg.JobConfig) \
            -> _meta.ModelParameter:

        model_params = []

        for target in targets:

            model_selector = job_def.runFlow.models.get(target.node)
            model_obj = _util.get_job_metadata(model_selector, job_config)
            model_param = model_obj.model.parameters.get(target.socket)
            model_params.append(model_param)

        if len(model_params) == 0:
            err = f"Flow parameter [{param_name}] is not connected to any models, type information cannot be inferred" \
                  + f" (either remove the parameter or connect it to a model)"
            cls._log.error(err)
            raise _ex.EJobValidation(err)

        if len(model_params) == 1:
            return model_params[0]

        model_param = model_params[0]

        for i in range(1, len(targets)):
            next_param = model_params[i]
            if next_param.paramType != model_param.paramType:
                err = f"Parameter is ambiguous for [{param_name}]: " + \
                      f"Types are different for [{cls._socket_key(targets[0])}] and [{cls._socket_key(targets[i])}]"
                raise _ex.EJobValidation(err)
            if next_param.defaultValue is None or next_param.defaultValue != model_param.defaultValue:
                model_param.defaultValue = None

        return model_param

    @classmethod
    def _infer_input_schema(
            cls, input_name: str, targets: tp.List[_meta.FlowSocket],
            job_def: _meta.JobDefinition, job_config: _cfg.JobConfig) \
            -> _meta.ModelInputSchema:

        model_inputs = []

        for target in targets:

            model_selector = job_def.runFlow.models.get(target.node)
            model_obj = _util.get_job_metadata(model_selector, job_config)
            model_input = model_obj.model.inputs.get(target.socket)
            model_inputs.append(model_input)

        if len(model_inputs) == 0:
            err = f"Flow input [{input_name}] is not connected to any models, schema cannot be inferred" \
                  + f" (either remove the input or connect it to a model)"
            cls._log.error(err)
            raise _ex.EJobValidation(err)

        if len(model_inputs) == 1:
            return model_inputs[0]

        model_input = model_inputs[0]

        for i in range(1, len(targets)):
            next_input = model_inputs[i]
            # Very strict rules on inputs, they must have the exact same schema
            # The Java code includes a combineSchema() method which could be used here as well
            if next_input != model_input:
                raise _ex.EJobValidation(f"Multiple models use input [{input_name}] but expect different schemas")

        return model_input

    @classmethod
    def _infer_output_schema(
            cls, output_name: str, sources: tp.List[_meta.FlowSocket],
            job_def: _meta.JobDefinition, job_config: _cfg.JobConfig) \
            -> _meta.ModelOutputSchema:

        model_outputs = []

        for source in sources:

            model_selector = job_def.runFlow.models.get(source.node)
            model_obj = _util.get_job_metadata(model_selector, job_config)
            model_input = model_obj.model.outputs.get(source.socket)
            model_outputs.append(model_input)

        if len(model_outputs) == 0:
            err = f"Flow output [{output_name}] is not connected to any models, schema cannot be inferred" \
                  + f" (either remove the output or connect it to a model)"
            cls._log.error(err)
            raise _ex.EJobValidation(err)

        if len(model_outputs) > 1:
            err = f"Flow output [{output_name}] is not to multiple models" \
                  + f" (only one model can supply one output)"
            cls._log.error(err)
            raise _ex.EJobValidation(err)

        return model_outputs[0]

    @classmethod
    def _socket_key(cls, socket):
        return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

    @classmethod
    def _process_parameters(
            cls, job_config: _cfg.JobConfig, job_def: _meta.JobDefinition) \
            -> tp.Tuple[_cfg.JobConfig, _meta.JobDefinition]:

        # This relies on convention for naming properties across similar job types

        job_detail = cls._get_job_detail(job_def)

        if hasattr(job_detail, "model"):
            model_key = _util.object_key(job_detail.model)
            model_or_flow = job_config.objects[model_key].model
        elif hasattr(job_detail, "flow"):
            flow_key = _util.object_key(job_detail.flow)
            model_or_flow = job_config.objects[flow_key].flow
        else:
            model_or_flow = None

        if model_or_flow is not None:

            param_specs = model_or_flow.parameters
            raw_values = job_detail.parameters

            job_detail.parameters = cls._process_parameters_dict(param_specs, raw_values)

        return job_config, job_def

    @classmethod
    def _process_parameters_dict(
            cls, param_specs: tp.Dict[str, _meta.ModelParameter],
            raw_values: tp.Dict[str, _meta.Value]) -> tp.Dict[str, _meta.Value]:

        unknown_params = list(filter(lambda p: p not in param_specs, raw_values))

        if any(unknown_params):
            msg = f"Unknown parameters cannot be translated: [{', '.join(unknown_params)}]"
            cls._log.error(msg)
            raise _ex.EConfigParse(msg)

        encoded_values = dict()

        for p_name, p_value in raw_values.items():

            if isinstance(p_value, _meta.Value):
                encoded_values[p_name] = p_value

            else:
                p_spec = param_specs[p_name]

                try:
                    cls._log.info(f"Encoding parameter [{p_name}] as {p_spec.paramType.basicType.name}")
                    encoded_value = _types.MetadataCodec.convert_value(p_value, p_spec.paramType)
                    encoded_values[p_name] = encoded_value

                except Exception as e:
                    msg = f"Failed to encode parameter [{p_name}]: {str(e)}"
                    cls._log.error(msg)
                    raise _ex.EConfigParse(msg) from e

        return encoded_values

    def _process_inputs_and_outputs(
            self, job_config: _cfg.JobConfig, job_def: _meta.JobDefinition) \
            -> tp.Tuple[_cfg.JobConfig, _meta.JobDefinition]:

        job_detail = self._get_job_detail(job_def)

        if hasattr(job_detail, "model"):
            model_obj = _util.get_job_metadata(job_detail.model, job_config)
            required_inputs = model_obj.model.inputs
            expected_outputs = model_obj.model.outputs

        elif hasattr(job_detail, "flow"):
            flow_obj = _util.get_job_metadata(job_detail.flow, job_config)
            required_inputs = flow_obj.flow.inputs
            expected_outputs = flow_obj.flow.outputs

        else:
            return job_config, job_def

        job_metadata = job_config.objects
        job_inputs = job_detail.inputs
        job_outputs = job_detail.outputs
        job_prior_outputs = job_detail.priorOutputs

        for key, schema in required_inputs.items():
            if key not in job_inputs:
                if not schema.optional:
                    raise _ex.EJobValidation(f"Missing required input [{key}]")
                continue
            supplied_input = job_inputs.pop(key) if key in job_inputs else None
            input_selector = self._process_socket(key, schema, supplied_input, job_metadata, is_output=False)
            if input_selector is not None:
                job_inputs[key] = input_selector

        for key, schema in expected_outputs.items():
            if key not in job_outputs:
                raise _ex.EJobValidation(f"Missing required output [{key}]")
            supplied_output = job_outputs.pop(key)
            output_selector = self._process_socket(key, schema, supplied_output, job_metadata, is_output=True)
            if output_selector is not None:
                job_prior_outputs[key] = output_selector

        return job_config, job_def

    def _process_socket(self, key, socket, supplied_value, job_metadata, is_output) -> _meta.TagSelector:

        if socket.objectType == _meta.ObjectType.DATA:
            schema = socket.schema if socket and not socket.dynamic else None
            return self._process_data_socket(key, supplied_value, schema, job_metadata, is_output)

        elif socket.objectType == _meta.ObjectType.FILE:
            file_type = socket.fileType
            return self._process_file_socket(key, supplied_value, file_type, job_metadata, is_output)

        else:
            raise _ex.EUnexpected()

    def _process_data_socket(
            self, data_key, data_value, schema: tp.Optional[_meta.SchemaDefinition],
            job_metadata: tp.Dict[str, _meta.ObjectDefinition], is_output: bool)\
            -> _meta.TagSelector:

        data_id = _util.new_object_id(_meta.ObjectType.DATA)
        storage_id = _util.new_object_id(_meta.ObjectType.STORAGE)

        if isinstance(data_value, str):
            storage_path = data_value
            storage_key = self._sys_config.storage.defaultBucket
            storage_format = self.infer_format(storage_path, self._sys_config.storage, schema)

        elif isinstance(data_value, dict):

            storage_path = data_value.get("path")

            if not storage_path:
                raise _ex.EConfigParse(f"Invalid configuration for input [{data_key}] (missing required value 'path'")

            storage_key = data_value.get("storageKey") or self._sys_config.storage.defaultBucket
            storage_format = data_value.get("format") or self.infer_format(storage_path, self._sys_config.storage, schema)

        else:
            raise _ex.EConfigParse(f"Invalid configuration for input '{data_key}'")

        # Scan for existing versions using hte DEVELOPER storage layout

        self._log.info(f"Looking for {'output' if is_output else 'input'} [{data_key}]...")

        storage_path, version = self._find_latest_version(storage_key, storage_path)
        data_id.objectVersion = version

        if version > 0:
            self._log.info(f"Found {'output' if is_output else 'input'} [{data_key}] version {version}")
            self._log.info(f"Generating {'prior' if is_output else 'data'} definition for [{data_key}] with ID = [{_util.object_key(data_id)}]")
        elif is_output:
            self._log.info(f"No prior data for output [{data_key}]")
        else:
            # This is allowed for some scenarios, e.g. inside a job group
            self._log.warning(f"No data found for input [{data_key}]")

        part_key = _meta.PartKey(opaqueKey="part-root", partType=_meta.PartType.PART_ROOT)
        snap_index = version - 1 if version > 0 else 0
        delta_index = 0
        incarnation_index = 0

        # This is also defined in functions.DynamicDataSpecFunc, maybe centralize?
        data_item = f"data/table/{data_id.objectId}/{part_key.opaqueKey}/snap-{snap_index}/delta-{delta_index}"

        data_obj = self._generate_data_definition(
            part_key, snap_index, delta_index, data_item,
            schema, storage_id)

        storage_obj = self._generate_storage_definition(
            storage_id, storage_key, storage_path, storage_format,
            data_item, incarnation_index)

        job_metadata[_util.object_key(data_id)] = data_obj
        job_metadata[_util.object_key(storage_id)] = storage_obj

        return _util.selector_for(data_id)

    def _process_file_socket(
            self, file_key, file_value, file_type: _meta.FileType,
            job_metadata: tp.Dict[str, _meta.ObjectDefinition], is_output: bool) \
            -> tp.Optional[_meta.TagSelector]:

        file_id = _util.new_object_id(_meta.ObjectType.FILE)
        storage_id = _util.new_object_id(_meta.ObjectType.STORAGE)

        if isinstance(file_value, str):

            storage_key = self._sys_config.storage.defaultBucket
            storage_path = file_value

        elif isinstance(file_value, dict):

            storage_key = file_value.get("storageKey") or self._sys_config.storage.defaultBucket
            storage_path = file_value.get("path")

            if not storage_path:
                raise _ex.EConfigParse(f"Invalid configuration for input [{file_key}] (missing required value 'path'")

        else:
            raise _ex.EConfigParse(f"Invalid configuration for input '{file_key}'")

        # Scan for existing versions using hte DEVELOPER storage layout

        self._log.info(f"Looking for {'output' if is_output else 'input'} [{file_key}]...")

        storage_path, version = self._find_latest_version(storage_key, storage_path)
        file_id.objectVersion = version

        if version > 0:
            self._log.info(f"Found {'output' if is_output else 'input'} [{file_key}] version {version}")
            self._log.info(f"Generating {'prior' if is_output else 'file'} definition for [{file_key}] with ID = [{_util.object_key(file_id)}]")
        elif is_output:
            self._log.info(f"No prior data for output [{file_key}]")
        else:
            # This is allowed for some scenarios, e.g. inside a job group
            self._log.warning(f"No data found for input [{file_key}]")

        storage = self._storage_manager.get_file_storage(storage_key)
        file_size = storage.size(storage_path) if storage.exists(storage_path) else 0

        storage_format = "application/x-binary"

        data_item = f"file/{file_id.objectId}/version-{version}"
        file_name = f"{file_key}.{file_type.extension}"

        file_obj = self._generate_file_definition(
            file_name, file_type, file_size,
            storage_id, data_item)

        storage_obj = self._generate_storage_definition(
            storage_id, storage_key, storage_path, storage_format,
            data_item, incarnation_index=0)

        job_metadata[_util.object_key(file_id)] = file_obj
        job_metadata[_util.object_key(storage_id)] = storage_obj

        return _util.selector_for(file_id)

    @staticmethod
    def infer_format(storage_path: str, storage_config: _cfg.StorageConfig, schema: tp.Optional[_meta.SchemaDefinition]):

        schema_type = schema.schemaType if schema and schema.schemaType else _meta.SchemaType.TABLE

        if re.match(r'.*\.\w+$', storage_path):
            extension = pathlib.Path(storage_path).suffix
            # Only try to map TABLE codecs through IDataFormat for now
            if schema_type == _meta.SchemaType.TABLE:
                codec = _storage.FormatManager.get_data_format(extension, format_options={})
                return codec.format_code()
            else:
                return extension[1:] if extension.startswith(".") else extension

        else:
            return storage_config.defaultFormat

    def _find_latest_version(self, storage_key, storage_path):

        storage = self._storage_manager.get_file_storage(storage_key)
        orig_path = pathlib.PurePath(storage_path)
        version = 0

        if not storage.exists(str(orig_path.parent)):
            return storage_path, version

        listing = storage.ls(str(orig_path.parent))
        existing_files = list(map(lambda stat: stat.file_name, listing))

        next_version = version + 1
        next_name = f"{orig_path.stem}{orig_path.suffix}"

        while next_name in existing_files:

            storage_path = str(orig_path.parent.joinpath(next_name))
            version = next_version

            next_version = version + 1
            next_name = f"{orig_path.stem}-{next_version}{orig_path.suffix}"

        return storage_path, version

    @classmethod
    def _generate_data_definition(
            cls, part_key: _meta.PartKey, snap_index: int, delta_index: int, data_item: str,
            schema: tp.Optional[_meta.SchemaDefinition], storage_id: _meta.TagHeader) \
            -> (_meta.ObjectDefinition, _meta.ObjectDefinition):

        delta = _meta.DataDefinition.Delta(
            deltaIndex=delta_index,
            dataItem=data_item)

        snap = _meta.DataDefinition.Snap(
            snapIndex=snap_index,
            deltas=[delta])

        part = _meta.DataDefinition.Part(
            partKey=part_key,
            snap=snap)

        data_def = _meta.DataDefinition()
        data_def.parts[part_key.opaqueKey] = part
        data_def.schema = schema
        data_def.storageId = _util.selector_for(storage_id)

        return _meta.ObjectDefinition(objectType=_meta.ObjectType.DATA, data=data_def)

    @classmethod
    def _generate_file_definition(
            cls, file_name: str, file_type: _meta.FileType, file_size: int,
            storage_id: _meta.TagHeader, data_item: str) \
            -> _meta.ObjectDefinition:

        file_def = _meta.FileDefinition(
            name=file_name, extension=file_type.extension, mimeType=file_type.mimeType,
            storageId=_util.selector_for(storage_id), dataItem=data_item, size=file_size)

        return _meta.ObjectDefinition(objectType=_meta.ObjectType.FILE, file=file_def)

    @classmethod
    def _generate_storage_definition(
            cls, storage_id: _meta.TagHeader,
            storage_key: str, storage_path: str, storage_format: str,
            data_item: str, incarnation_index: int) \
            -> _meta.ObjectDefinition:

        storage_copy = _meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=_meta.CopyStatus.COPY_AVAILABLE)

        storage_incarnation = _meta.StorageIncarnation(
            incarnationIndex=incarnation_index,
            incarnationTimestamp=storage_id.objectTimestamp,
            incarnationStatus=_meta.IncarnationStatus.INCARNATION_AVAILABLE,
            copies=[storage_copy])

        storage_item = _meta.StorageItem(
            incarnations=[storage_incarnation])

        storage_def = _meta.StorageDefinition()
        storage_def.dataItems[data_item] = storage_item
        storage_def.layout = _meta.StorageLayout.DEVELOPER_LAYOUT

        if storage_format.lower() == "csv":
            storage_def.storageOptions["lenient_csv_parser"] = _types.MetadataCodec.encode_value(True)

        return _meta.ObjectDefinition(objectType=_meta.ObjectType.STORAGE, storage=storage_def)



DevModeTranslator._log = _logging.logger_for_class(DevModeTranslator)
