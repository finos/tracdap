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

import re
import typing as tp
import copy
import pathlib

import tracdap.rt.api as _api
import tracdap.rt.config as _cfg
import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.config_parser as _cfg_p  # noqa
import tracdap.rt._impl.models as _models  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.type_system as _types  # noqa
import tracdap.rt._impl.util as _util  # noqa


DEV_MODE_JOB_CONFIG = [
    re.compile(r"job\.\w+\.parameters\.\w+"),
    re.compile(r"job\.\w+\.inputs\.\w+"),
    re.compile(r"job\.\w+\.outputs\.\w+"),
    re.compile(r"job\.\w+\.models\.\w+"),
    re.compile(r"job\.\w+\.model"),
    re.compile(r"job\.\w+\.flow")]

DEV_MODE_SYS_CONFIG = []


class DevModeTranslator:

    _log: tp.Optional[_util.logging.Logger] = None

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
    def translate_job_config(
            cls,
            sys_config: _cfg.RuntimeConfig,
            job_config: _cfg.JobConfig,
            scratch_dir: pathlib.Path,
            config_mgr: _cfg_p.ConfigManager,
            model_class: tp.Optional[_api.TracModel.__class__]) \
            -> _cfg.JobConfig:

        cls._log.info(f"Applying dev mode config translation to job config")

        # Protobuf semantics for a blank jobId should be an object, but objectId will be an empty string
        if not job_config.jobId or not job_config.jobId.objectId:
            job_config = cls._process_job_id(job_config)

        if job_config.job.jobType is None or job_config.job.jobType == _meta.JobType.JOB_TYPE_NOT_SET:
            job_config = cls._process_job_type(job_config)

        # Load and populate any models provided as a Python class or class name
        job_config = cls._process_models(sys_config, job_config, scratch_dir, model_class)

        # Fow flows, load external flow definitions then perform auto-wiring and type inference
        if job_config.job.jobType == _meta.JobType.RUN_FLOW:
            job_config = cls._process_flow_definition(job_config, config_mgr)

        # Apply processing to the parameters, inputs and outputs
        job_config = cls._process_parameters(job_config)
        job_config = cls._process_inputs_and_outputs(sys_config, job_config)

        return job_config

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

    @classmethod
    def _add_job_resource(
            cls, job_config: _cfg.JobConfig,
            obj_id: _meta.TagHeader, obj: _meta.ObjectDefinition) \
            -> _cfg.JobConfig:

        obj_key = _util.object_key(obj_id)
        job_config.resources[obj_key] = obj

        return job_config

    @classmethod
    def _process_job_id(cls, job_config: _cfg.JobConfig):

        job_id = _util.new_object_id(_meta.ObjectType.JOB)

        cls._log.info(f"Assigning job ID = [{_util.object_key(job_id)}]")

        translated_config = copy.copy(job_config)
        translated_config.jobId = job_id

        return translated_config

    @classmethod
    def _process_job_type(cls, job_config: _cfg.JobConfig):

        if job_config.job.runModel is not None:
            job_type = _meta.JobType.RUN_MODEL

        elif job_config.job.runFlow is not None:
            job_type = _meta.JobType.RUN_FLOW

        elif job_config.job.importModel is not None:
            job_type = _meta.JobType.IMPORT_MODEL

        elif job_config.job.importData is not None:
            job_type = _meta.JobType.IMPORT_DATA

        elif job_config.job.exportData is not None:
            job_type = _meta.JobType.EXPORT_DATA

        else:
            cls._log.error("Could not infer job type")
            raise _ex.EConfigParse("Could not infer job type")

        cls._log.info(f"Inferred job type = [{job_type.name}]")

        job_def = copy.copy(job_config.job)
        job_def.jobType = job_type

        job_config = copy.copy(job_config)
        job_config.job = job_def

        return job_config

    @classmethod
    def _get_job_detail(cls, job_config: _cfg.JobConfig):

        if job_config.job.jobType == _meta.JobType.RUN_MODEL:
            return job_config.job.runModel

        if job_config.job.jobType == _meta.JobType.RUN_FLOW:
            return job_config.job.runFlow

        if job_config.job.jobType == _meta.JobType.IMPORT_MODEL:
            return job_config.job.importModel

        if job_config.job.jobType == _meta.JobType.IMPORT_DATA:
            return job_config.job.importData

        if job_config.job.jobType == _meta.JobType.EXPORT_DATA:
            return job_config.job.exportData

        raise _ex.EConfigParse(f"Could not get job details for job type [{job_config.job.jobType}]")

    @classmethod
    def _process_models(
            cls,
            sys_config: _cfg.RuntimeConfig,
            job_config: _cfg.JobConfig,
            scratch_dir: pathlib.Path,
            model_class: tp.Optional[_api.TracModel.__class__]) \
            -> _cfg.JobConfig:

        model_loader = _models.ModelLoader(sys_config, scratch_dir)
        model_loader.create_scope("DEV_MODE_TRANSLATION")

        # This processing works on the assumption that job details follow a convention for addressing models
        # Jobs requiring a single model have a field called "model"
        # Jobs requiring multiple models have a field called "models@, which is a dict

        job_detail = cls._get_job_detail(job_config)

        # If a model class is supplied in code, use that to generate the model def
        if model_class is not None:

            # Passing a model class via launch_model() is only supported for job types with a single model
            if not hasattr(job_detail, "model"):
                raise _ex.EJobValidation(f"Job type [{job_config.job.jobType}] cannot be launched using launch_model()")

            model_id, model_obj = cls._generate_model_for_class(model_loader, model_class)
            job_detail.model = _util.selector_for(model_id)
            job_config = cls._add_job_resource(job_config, model_id, model_obj)

        # Otherwise look for models specified as a single string, and take that as the entry point
        else:

            # Jobs with a single model
            if hasattr(job_detail, "model") and isinstance(job_detail.model, str):
                model_id, model_obj = cls._generate_model_for_entry_point(model_loader, job_detail.model)  # noqa
                job_detail.model = _util.selector_for(model_id)
                job_config = cls._add_job_resource(job_config, model_id, model_obj)

            # Jobs with multiple modlels
            elif hasattr(job_detail, "models") and isinstance(job_detail.models, dict):
                for model_key, model_detail in job_detail.models.items():
                    if isinstance(model_detail, str):
                        model_id, model_obj = cls._generate_model_for_entry_point(model_loader, model_detail)
                        job_detail.models[model_key] = _util.selector_for(model_id)
                        job_config = cls._add_job_resource(job_config, model_id, model_obj)

        model_loader.destroy_scope("DEV_MODE_TRANSLATION")

        return job_config

    @classmethod
    def _generate_model_for_class(
            cls, model_loader: _models.ModelLoader, model_class: _api.TracModel.__class__) \
            -> (_meta.TagHeader, _meta.ObjectDefinition):

        model_entry_point = f"{model_class.__module__}.{model_class.__name__}"

        return cls._generate_model_for_entry_point(model_loader, model_entry_point)

    @classmethod
    def _generate_model_for_entry_point(
            cls, model_loader: _models.ModelLoader, model_entry_point: str) \
            -> (_meta.TagHeader, _meta.ObjectDefinition):

        model_id = _util.new_object_id(_meta.ObjectType.MODEL)
        model_key = _util.object_key(model_id)

        cls._log.info(f"Generating model definition for [{model_entry_point}] with ID = [{model_key}]")

        skeleton_modeL_def = _meta.ModelDefinition(  # noqa
            language="python",
            repository="trac_integrated",
            entryPoint=model_entry_point,

            parameters={},
            inputs={},
            outputs={})

        model_class = model_loader.load_model_class("DEV_MODE_TRANSLATION", skeleton_modeL_def)
        model_def = model_loader.scan_model(skeleton_modeL_def, model_class)

        model_object = _meta.ObjectDefinition(
            objectType=_meta.ObjectType.MODEL,
            model=model_def)

        return model_id, model_object

    @classmethod
    def _process_flow_definition(cls, job_config: _cfg.JobConfig, config_mgr: _cfg_p.ConfigManager) -> _cfg.JobConfig:

        flow_details = job_config.job.runFlow.flow

        # Do not apply translation if flow is specified as an object ID / selector (assume full config is supplied)
        if isinstance(flow_details, _meta.TagHeader) or isinstance(flow_details, _meta.TagSelector):
            return job_config

        # Otherwise, flow is specified as the path to dev-mode flow definition
        if not isinstance(flow_details, str):
            err = f"Invalid config value for [job.runFlow.flow]: Expected path or tag selector, got [{flow_details}])"
            cls._log.error(err)
            raise _ex.EConfigParse(err)

        flow_id = _util.new_object_id(_meta.ObjectType.FLOW)
        flow_key = _util.object_key(flow_id)

        cls._log.info(f"Generating flow definition from [{flow_details}] with ID = [{flow_key}]")

        flow_def = config_mgr.load_config_object(flow_details, _meta.FlowDefinition)

        # Validate models against the flow (this could move to _impl.validation and check prod jobs as well)
        cls._check_models_for_flow(flow_def, job_config)

        # Auto-wiring and inference only applied to externally loaded flows for now
        flow_def = cls._autowire_flow(flow_def, job_config)
        flow_def = cls._apply_type_inference(flow_def, job_config)

        flow_obj = _meta.ObjectDefinition(
            objectType=_meta.ObjectType.FLOW,
            flow=flow_def)

        job_config = copy.copy(job_config)
        job_config.job = copy.copy(job_config.job)
        job_config.job.runFlow = copy.copy(job_config.job.runFlow)
        job_config.resources = copy.copy(job_config.resources)

        job_config = cls._add_job_resource(job_config, flow_id, flow_obj)
        job_config.job.runFlow.flow = _util.selector_for(flow_id)

        return job_config

    @classmethod
    def _check_models_for_flow(cls, flow: _meta.FlowDefinition, job_config: _cfg.JobConfig):

        model_nodes = dict(filter(lambda n: n[1].nodeType == _meta.FlowNodeType.MODEL_NODE, flow.nodes.items()))

        missing_models = list(filter(lambda m: m not in job_config.job.runFlow.models, model_nodes.keys()))
        extra_models = list(filter(lambda m: m not in model_nodes, job_config.job.runFlow.models.keys()))

        if any(missing_models):
            error = f"Missing models in job definition: {', '.join(missing_models)}"
            cls._log.error(error)
            raise _ex.EJobValidation(error)

        if any (extra_models):
            error = f"Extra models in job definition: {', '.join(extra_models)}"
            cls._log.error(error)
            raise _ex.EJobValidation(error)

        for model_name, model_node in model_nodes.items():

            model_selector = job_config.job.runFlow.models[model_name]
            model_obj = _util.get_job_resource(model_selector, job_config)

            model_inputs = set(model_obj.model.inputs.keys())
            model_outputs = set(model_obj.model.outputs.keys())

            if model_inputs != set(model_node.inputs) or model_outputs != set(model_node.outputs):
                error = f"The model supplied for [{model_name}] does not match the flow definition"
                cls._log.error(error)
                raise _ex.EJobValidation(error)

    @classmethod
    def _autowire_flow(cls, flow: _meta.FlowDefinition, job_config: _cfg.JobConfig):

        job = job_config.job.runFlow
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
                    model_obj = _util.get_job_resource(model_selector, job_config)
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
    def _apply_type_inference(cls, flow: _meta.FlowDefinition, job_config: _cfg.JobConfig) -> _meta.FlowDefinition:

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
                model_parameter = cls._infer_parameter(node_name, targets, job_config)
                updated_flow.parameters[node_name] = model_parameter

            if node.nodeType == _meta.FlowNodeType.INPUT_NODE and node_name not in flow.inputs:
                targets = edges_by_source.get(node_name) or []
                model_input = cls._infer_input_schema(node_name, targets, job_config)
                updated_flow.inputs[node_name] = model_input

            if node.nodeType == _meta.FlowNodeType.OUTPUT_NODE and node_name not in flow.outputs:
                sources = edges_by_target.get(node_name) or []
                model_output = cls._infer_output_schema(node_name, sources, job_config)
                updated_flow.outputs[node_name] = model_output

        return updated_flow

    @classmethod
    def _infer_parameter(
            cls, param_name: str, targets: tp.List[_meta.FlowSocket],
            job_config: _cfg.JobConfig) -> _meta.ModelParameter:

        model_params = []

        for target in targets:

            model_selector = job_config.job.runFlow.models.get(target.node)
            model_obj = _util.get_job_resource(model_selector, job_config)
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
            job_config: _cfg.JobConfig) -> _meta.ModelInputSchema:

        model_inputs = []

        for target in targets:

            model_selector = job_config.job.runFlow.models.get(target.node)
            model_obj = _util.get_job_resource(model_selector, job_config)
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
            job_config: _cfg.JobConfig) -> _meta.ModelOutputSchema:

        model_outputs = []

        for source in sources:

            model_selector = job_config.job.runFlow.models.get(source.node)
            model_obj = _util.get_job_resource(model_selector, job_config)
            model_input = model_obj.model.inputs.get(source.socket)
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
    def _process_parameters(cls, job_config: _cfg.JobConfig) -> _cfg.JobConfig:

        # This relies on convention for naming properties across similar job types

        job_detail = cls._get_job_detail(job_config)

        if hasattr(job_detail, "model"):
            model_key = _util.object_key(job_detail.model)
            model_or_flow = job_config.resources[model_key].model
        elif hasattr(job_detail, "flow"):
            flow_key = _util.object_key(job_detail.flow)
            model_or_flow = job_config.resources[flow_key].flow
        else:
            model_or_flow = None

        if model_or_flow is not None:

            param_specs = model_or_flow.parameters
            raw_values = job_detail.parameters

            job_detail.parameters = cls._process_parameters_dict(param_specs, raw_values)

        return job_config

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

                cls._log.info(f"Encoding parameter [{p_name}] as {p_spec.paramType.basicType}")

                encoded_value = _types.MetadataCodec.convert_value(p_value, p_spec.paramType)
                encoded_values[p_name] = encoded_value

        return encoded_values

    @classmethod
    def _process_inputs_and_outputs(cls, sys_config: _cfg.RuntimeConfig, job_config: _cfg.JobConfig) -> _cfg.JobConfig:

        job_detail = cls._get_job_detail(job_config)

        if hasattr(job_detail, "model"):
            model_obj = _util.get_job_resource(job_detail.model, job_config)
            required_inputs = model_obj.model.inputs
            required_outputs = model_obj.model.outputs

        elif hasattr(job_detail, "flow"):
            flow_obj = _util.get_job_resource(job_detail.flow, job_config)
            required_inputs = flow_obj.flow.inputs
            required_outputs = flow_obj.flow.outputs

        else:
            return job_config

        job_inputs = job_detail.inputs
        job_outputs = job_detail.outputs
        job_resources = job_config.resources

        for input_key, input_value in job_inputs.items():
            if not (isinstance(input_value, str) and input_value in job_resources):

                model_input = required_inputs[input_key]
                input_schema = model_input.schema if model_input and not model_input.dynamic else None

                input_id = cls._process_input_or_output(
                    sys_config, input_key, input_value, job_resources,
                    new_unique_file=False, schema=input_schema)

                job_inputs[input_key] = _util.selector_for(input_id)

        for output_key, output_value in job_outputs.items():
            if not (isinstance(output_value, str) and output_value in job_resources):

                model_output= required_outputs[output_key]
                output_schema = model_output.schema if model_output and not model_output.dynamic else None

                output_id = cls._process_input_or_output(
                    sys_config, output_key, output_value, job_resources,
                    new_unique_file=True, schema=output_schema)

                job_outputs[output_key] = _util.selector_for(output_id)

        return job_config

    @classmethod
    def _process_input_or_output(
            cls, sys_config, data_key, data_value,
            resources: tp.Dict[str, _meta.ObjectDefinition],
            new_unique_file=False,
            schema: tp.Optional[_meta.SchemaDefinition] = None) \
            -> _meta.TagHeader:

        data_id = _util.new_object_id(_meta.ObjectType.DATA)
        storage_id = _util.new_object_id(_meta.ObjectType.STORAGE)

        if isinstance(data_value, str):
            storage_path = data_value
            storage_key = sys_config.storage.defaultBucket
            storage_format = cls.infer_format(storage_path, sys_config.storage)
            snap_version = 1

        elif isinstance(data_value, dict):

            storage_path = data_value.get("path")

            if not storage_path:
                raise _ex.EConfigParse(f"Invalid configuration for input [{data_key}] (missing required value 'path'")

            storage_key = data_value.get("storageKey") or sys_config.storage.defaultBucket
            storage_format = data_value.get("format") or cls.infer_format(storage_path, sys_config.storage)
            snap_version = 1

        else:
            raise _ex.EConfigParse(f"Invalid configuration for input '{data_key}'")

        cls._log.info(f"Generating data definition for [{data_key}] with ID = [{_util.object_key(data_id)}]")

        # For unique outputs, increment the snap number to find a new unique snap
        # These are not incarnations, bc likely in dev mode model code and inputs are changing
        # Incarnations are for recreation of a dataset using the exact same code path and inputs

        if new_unique_file:

            x_storage_mgr = _storage.StorageManager(sys_config)
            x_storage = x_storage_mgr.get_file_storage(storage_key)
            x_orig_path = pathlib.PurePath(storage_path)
            x_name = x_orig_path.name

            if x_storage.exists(str(x_orig_path.parent)):
                listing = x_storage.ls(str(x_orig_path.parent))
                existing_files = list(map(lambda stat: stat.file_name, listing))
            else:
                existing_files = []

            while x_name in existing_files:

                snap_version += 1
                x_name = f"{x_orig_path.stem}-{snap_version}"
                storage_path = str(x_orig_path.parent.joinpath(x_name))

            cls._log.info(f"Output for [{data_key}] will be snap version {snap_version}")

        data_obj, storage_obj = cls._generate_input_definition(
            data_id, storage_id, storage_key, storage_path, storage_format,
            snap_index=snap_version, delta_index=1, incarnation_index=1,
            schema=schema)

        resources[_util.object_key(data_id)] = data_obj
        resources[_util.object_key(storage_id)] = storage_obj

        return data_id

    @staticmethod
    def infer_format(storage_path: str, storage_config: _cfg.StorageConfig):

        if re.match(r'.*\.\w+$', storage_path):
            extension = pathlib.Path(storage_path).suffix
            codec = _storage.FormatManager.get_data_format(extension, format_options={})
            return codec.format_code()

        else:
            return storage_config.defaultFormat

    @classmethod
    def _generate_input_definition(
            cls, data_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            storage_key: str, storage_path: str, storage_format: str,
            snap_index: int, delta_index: int, incarnation_index: int,
            schema: tp.Optional[_meta.SchemaDefinition] = None) \
            -> (_meta.ObjectDefinition, _meta.ObjectDefinition):

        part_key = _meta.PartKey(
            opaqueKey="part-root",
            partType=_meta.PartType.PART_ROOT)

        # This is also defined in functions.DynamicDataSpecFunc, maybe centralize?
        data_item = f"data/table/{data_id.objectId}/{part_key.opaqueKey}/snap-{snap_index}/delta-{delta_index}"

        delta = _meta.DataDefinition.Delta(
            deltaIndex=delta_index,
            dataItem=data_item)

        snap = _meta.DataDefinition.Snap(
            snapIndex=snap_index,
            deltas=[delta])

        part = _meta.DataDefinition.Part(
            partKey=part_key,
            snap=snap)

        data_def = _meta.DataDefinition(parts={})
        data_def.parts[part_key.opaqueKey] = part

        if schema is not None:
            data_def.schema = schema
        else:
            data_def.schema = None

        data_def.storageId = _meta.TagSelector(
            _meta.ObjectType.STORAGE, storage_id.objectId,
            objectVersion=storage_id.objectVersion, latestTag=True)

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

        storage_def = _meta.StorageDefinition(dataItems={})
        storage_def.dataItems[delta.dataItem] = storage_item

        if storage_format.lower() == "csv":
            storage_def.storageOptions["lenient_csv_parser"] = _types.MetadataCodec.encode_value(True)

        data_obj = _meta.ObjectDefinition(objectType=_meta.ObjectType.DATA, data=data_def)
        storage_obj = _meta.ObjectDefinition(objectType=_meta.ObjectType.STORAGE, storage=storage_def)

        return data_obj, storage_obj


DevModeTranslator._log = _util.logger_for_class(DevModeTranslator)
