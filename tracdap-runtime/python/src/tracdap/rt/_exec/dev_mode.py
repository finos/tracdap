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
    re.compile(r"job\.run(Model|Flow)\.parameters\.\w+"),
    re.compile(r"job\.run(Model|Flow)\.inputs\.\w+"),
    re.compile(r"job\.run(Model|Flow)\.outputs\.\w+"),
    re.compile(r"job\.run(Model|Flow)\.models\.\w+"),
    re.compile(r"job\.run(Model|Flow)\.flow+")]

DEV_MODE_SYS_CONFIG = []


class DevModeTranslator:

    _log: tp.Optional[_util.logging.Logger] = None

    @classmethod
    def translate_sys_config(cls, sys_config: _cfg.RuntimeConfig, config_dir: tp.Optional[pathlib.Path]):

        cls._log.info(f"Applying dev mode config translation to system config")

        # TODO: In code gen, default object types to a new object unless the field is marked as optional
        # This would match the general semantics of protobuf
        if sys_config.storage is None:
            sys_config.storage = _cfg.StorageConfig()

        sys_config = cls._add_integrated_repo(sys_config)
        sys_config = cls._resolve_relative_storage_root(sys_config, config_dir)

        return sys_config

    @classmethod
    def translate_job_config(
            cls,
            sys_config: _cfg.RuntimeConfig,
            job_config: _cfg.JobConfig,
            scratch_dir: pathlib.Path,
            config_dir: tp.Optional[pathlib.Path],
            model_class: tp.Optional[_api.TracModel.__class__]) \
            -> _cfg.JobConfig:

        cls._log.info(f"Applying dev mode config translation to job config")

        model_loader = _models.ModelLoader(sys_config, scratch_dir)
        model_loader.create_scope("DEV_MODE_TRANSLATION")

        if not job_config.jobId:
            job_config = cls._process_job_id(job_config)

        if job_config.job.jobType is None or job_config.job.jobType == _meta.JobType.JOB_TYPE_NOT_SET:
            job_config = cls._process_job_type(job_config)

        if model_class is not None:

            model_id, model_obj = cls._generate_model_for_class(model_loader, model_class)
            job_config = cls._add_job_resource(job_config, model_id, model_obj)
            job_config.job.runModel.model = _util.selector_for(model_id)

        if job_config.job.jobType == _meta.JobType.RUN_FLOW:

            original_models = job_config.job.runFlow.models.copy()
            for model_key, model_detail in original_models.items():
                model_id, model_obj = cls._generate_model_for_entry_point(model_loader, model_detail)
                job_config = cls._add_job_resource(job_config, model_id, model_obj)
                job_config.job.runFlow.models[model_key] = _util.selector_for(model_id)

            flow_id, flow_obj = cls._expand_flow_definition(job_config, config_dir)
            job_config = cls._add_job_resource(job_config, flow_id, flow_obj)
            job_config.job.runFlow.flow = _util.selector_for(flow_id)

        model_loader.destroy_scope("DEV_MODE_TRANSLATION")

        if job_config.job.jobType in [_meta.JobType.RUN_MODEL, _meta.JobType.RUN_FLOW]:
            job_config = cls._process_parameters(job_config)

        if job_config.job.jobType not in [_meta.JobType.RUN_MODEL, _meta.JobType.RUN_FLOW]:
            return job_config

        run_info = job_config.job.runModel \
            if job_config.job.jobType == _meta.JobType.RUN_MODEL \
            else job_config.job.runFlow

        original_inputs = run_info.inputs
        original_outputs = run_info.outputs
        original_resources = job_config.resources

        translated_inputs = copy.copy(original_inputs)
        translated_outputs = copy.copy(original_outputs)
        translated_resources = copy.copy(job_config.resources)

        def process_input_or_output(data_key, data_value, is_input: bool):

            data_id = _util.new_object_id(_meta.ObjectType.DATA)
            storage_id = _util.new_object_id(_meta.ObjectType.STORAGE)

            if is_input:
                if job_config.job.jobType == _meta.JobType.RUN_MODEL:
                    model_def = job_config.resources[_util.object_key(job_config.job.runModel.model)]
                    schema = model_def.model.inputs[data_key].schema
                else:
                    flow_def = job_config.resources[_util.object_key(job_config.job.runFlow.flow)]
                    schema = flow_def.flow.inputs[data_key].schema
            else:
                schema = None

            data_obj, storage_obj = cls._process_job_io(
                sys_config, data_key, data_value, data_id, storage_id,
                new_unique_file=not is_input, schema=schema)

            translated_resources[_util.object_key(data_id)] = data_obj
            translated_resources[_util.object_key(storage_id)] = storage_obj

            if is_input:
                translated_inputs[data_key] = _util.selector_for(data_id)
            else:
                translated_outputs[data_key] = _util.selector_for(data_id)

        for input_key, input_value in original_inputs.items():
            if not (isinstance(input_value, str) and input_value in original_resources):
                process_input_or_output(input_key, input_value, is_input=True)

        for output_key, output_value in original_outputs.items():
            if not (isinstance(output_value, str) and output_value in original_outputs):
                process_input_or_output(output_key, output_value, is_input=False)

        job_config = copy.copy(job_config)
        job_config.resources = translated_resources
        run_info.inputs = translated_inputs
        run_info.outputs = translated_outputs

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
    def _resolve_relative_storage_root(
            cls, sys_config: _cfg.RuntimeConfig,
            sys_config_path: tp.Optional[pathlib.Path]):

        storage_config = copy.deepcopy(sys_config.storage)

        for bucket_key, bucket_config in storage_config.buckets.items():

            if bucket_config.protocol != "LOCAL":
                continue

            if "rootPath" not in bucket_config.properties:
                continue

            root_path = pathlib.Path(bucket_config.properties["rootPath"])

            if root_path.is_absolute():
                continue

            cls._log.info(f"Resolving relative path for [{bucket_key}] local storage...")

            if sys_config_path is not None:
                absolute_path = sys_config_path.joinpath(root_path).resolve()
                if absolute_path.exists():
                    cls._log.info(f"Resolved [{root_path}] -> [{absolute_path}]")
                    bucket_config.properties["rootPath"] = str(absolute_path)
                    continue

            cwd = pathlib.Path.cwd()
            absolute_path = cwd.joinpath(root_path).resolve()

            if absolute_path.exists():
                cls._log.info(f"Resolved [{root_path}] -> [{absolute_path}]")
                bucket_config.properties["rootPath"] = str(absolute_path)
                continue

            msg = f"Failed to resolve relative storage path [{root_path}]"
            cls._log.error(msg)
            raise _ex.EConfigParse(msg)

        sys_config = copy.copy(sys_config)
        sys_config.storage = storage_config

        return sys_config

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
    def _expand_flow_definition(
            cls, job_config: _cfg.JobConfig, config_dir: pathlib.Path) \
            -> (_meta.TagHeader, _meta.ObjectDefinition):

        flow_details = job_config.job.runFlow.flow

        # The full specification for a flow is as a tag selector for a valid job resource
        # This is still allowed in dev mode, in which case dev mode translation is not applied
        if isinstance(flow_details, _meta.TagHeader) or isinstance(flow_details, _meta.TagSelector):
            flow_obj = _util.get_job_resource(flow_details, job_config, optional=False)
            return flow_details, flow_obj

        # Otherwise, flow is specified as the path to dev-mode flow definition
        if not isinstance(flow_details, str):
            err = f"Invalid config value for [job.runFlow.flow]: Expected path or tag selector, got [{flow_details}])"
            cls._log.error(err)
            raise _ex.EConfigParse(err)

        flow_id = _util.new_object_id(_meta.ObjectType.FLOW)
        flow_key = _util.object_key(flow_id)

        cls._log.info(f"Generating flow definition for [{flow_details}] with ID = [{flow_key}]")

        flow_path = config_dir.joinpath(flow_details) if config_dir is not None else pathlib.Path(flow_details)
        flow_parser = _cfg_p.ConfigParser(_meta.FlowDefinition)
        flow_raw_data = flow_parser.load_raw_config(flow_path, flow_path.name)
        flow_def = flow_parser.parse(flow_raw_data, flow_path.name)

        flow_def = cls._autowire_flow(flow_def)
        flow_def = cls._generate_flow_parameters(flow_def, job_config)
        flow_def = cls._generate_flow_inputs(flow_def, job_config)
        flow_def = cls._generate_flow_outputs(flow_def, job_config)

        flow_object = _meta.ObjectDefinition(
            objectType=_meta.ObjectType.FLOW,
            flow=flow_def)

        return flow_id, flow_object

    @classmethod
    def _autowire_flow(cls, flow: _meta.FlowDefinition):

        sources: tp.Dict[str, _meta.FlowSocket] = dict()
        duplicates: tp.Dict[str, tp.List[_meta.FlowSocket]] = dict()

        edges: tp.Dict[str, _meta.FlowEdge] = dict()
        errors: tp.Dict[str, str] = dict()

        def socket_key(socket: _meta.FlowSocket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        def add_source(name: str, socket: _meta.FlowSocket):
            if name in duplicates:
                duplicates[name].append(socket)
            elif name in sources:
                duplicates[name] = [sources[name], socket]
                del sources[name]
            else:
                sources[name] = socket

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
            if node.nodeType == _meta.FlowNodeType.INPUT_NODE:
                add_source(node_name, _meta.FlowSocket(node_name))
            if node.nodeType == _meta.FlowNodeType.MODEL_NODE:
                for model_output in node.outputs:
                    add_source(model_output, _meta.FlowSocket(node_name, model_output))

        # Include any edges defined explicitly in the flow
        # These take precedence over auto-wired edges
        for edge in flow.edges:
            edges[socket_key(edge.target)] = edge

        for node_name, node in flow.nodes.items():
            if node.nodeType == _meta.FlowNodeType.OUTPUT_NODE:
                add_edge(_meta.FlowSocket(node_name))
            if node.nodeType == _meta.FlowNodeType.MODEL_NODE:
                for model_input in node.inputs:
                    add_edge(_meta.FlowSocket(node_name, model_input))

        if any(errors):

            err_line_break = "\n" if len(errors) > 1 else ""
            err_details = "\n".join(errors.values())
            err = f"Flow could not be auto-wired: {err_line_break}{err_details}"

            cls._log.error(err)
            raise _ex.EConfigParse(err)

        autowired_flow = copy.copy(flow)
        autowired_flow.edges = list(edges.values())

        return autowired_flow

    @classmethod
    def _generate_flow_parameters(cls, flow: _meta.FlowDefinition, job_config: _cfg.JobConfig) -> _meta.FlowDefinition:

        params: tp.Dict[str, _meta.ModelParameter] = dict()

        for node_name, node in flow.nodes.items():

            if node.nodeType != _meta.FlowNodeType.MODEL_NODE:
                continue

            if node_name not in job_config.job.runFlow.models:
                err = f"No model supplied for flow model node [{node_name}]"
                cls._log.error(err)
                raise _ex.EConfigParse(err)

            model_selector = job_config.job.runFlow.models[node_name]
            model_obj = _util.get_job_resource(model_selector, job_config)

            for param_name, param in model_obj.model.parameters.items():

                if param_name not in params:
                    params[param_name] = param

                else:
                    existing_param = params[param_name]

                    if param.paramType != existing_param.paramType:
                        err = f"Model parameter [{param_name}] has different types in different models"
                        cls._log.error(err)
                        raise _ex.EConfigParse(err)

                    if param.defaultValue != existing_param.defaultValue:
                        if existing_param.defaultValue is None:
                            params[param_name] = param
                        elif param.defaultValue is not None:
                            warn = f"Model parameter [{param_name}] has different default values in different models" \
                                 + f" (using [{_types.MetadataCodec.decode_value(existing_param.defaultValue)}])"
                            cls._log.warning(warn)

        flow.parameters = params

        return flow

    @classmethod
    def _generate_flow_inputs(cls, flow: _meta.FlowDefinition, job_config: _cfg.JobConfig) -> _meta.FlowDefinition:

        inputs: tp.Dict[str, _meta.ModelInputSchema] = dict()

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        # Build a map of edges by source socket, mapping to all edges flowing from that source
        edges = {socket_key(edge.source): [] for edge in flow.edges}
        for edge in flow.edges:
            edges[socket_key(edge.source)].append(edge)

        for node_name, node in flow.nodes.items():

            if node.nodeType != _meta.FlowNodeType.INPUT_NODE:
                continue

            input_edges = edges.get(node_name)

            if not input_edges:
                err = f"Flow input [{node_name}] is not connected, so the input schema cannot be inferred" \
                    + f" (either remove the input or connect it to a model)"
                cls._log.error(err)
                raise _ex.EConfigParse(err)

            input_schemas = []

            for edge in input_edges:

                target_node = flow.nodes.get(edge.target.node) # or cls._report_error(cls._MISSING_FLOW_NODE, node_name)
                # cls._require(target_node.nodeType == _meta.FlowNodeType.MODEL_NODE)

                model_selector = job_config.job.runFlow.models.get(edge.target.node)
                model_obj = _util.get_job_resource(model_selector, job_config)
                model_input = model_obj.model.inputs[edge.target.socket]
                input_schemas.append(model_input)

            if len(input_schemas) == 1:
                inputs[node_name] = input_schemas[0]
            else:
                first_schema = input_schemas[0]
                if all(map(lambda s: s == first_schema, input_schemas[1:])):
                    inputs[node_name] = first_schema
                else:
                    raise _ex.EJobValidation(f"Multiple models use input [{node_name}] but expect different schemas")

        flow.inputs = inputs

        return flow

    @classmethod
    def _generate_flow_outputs(cls, flow: _meta.FlowDefinition, job_config: _cfg.JobConfig) -> _meta.FlowDefinition:

        outputs: tp.Dict[str, _meta.ModelOutputSchema] = dict()

        def socket_key(socket):
            return f"{socket.node}.{socket.socket}" if socket.socket else socket.node

        # Build a map of edges by target socket, there can only be one edge per target in a valid flow
        edges = {socket_key(edge.target): edge for edge in flow.edges}

        for node_name, node in flow.nodes.items():

            if node.nodeType != _meta.FlowNodeType.OUTPUT_NODE:
                continue

            edge = edges.get(node_name)

            if not edge:
                err = f"Flow output [{node_name}] is not connected, so the output schema cannot be inferred" \
                      + f" (either remove the output or connect it to a model)"
                cls._log.error(err)
                raise _ex.EConfigParse(err)

            source_node = flow.nodes.get(edge.source.node) # or cls._report_error(cls._MISSING_FLOW_NODE, node_name)
            # cls._require(target_node.nodeType == _meta.FlowNodeType.MODEL_NODE)

            model_selector = job_config.job.runFlow.models.get(edge.source.node)
            model_obj = _util.get_job_resource(model_selector, job_config)
            model_output = model_obj.model.outputs[edge.source.socket]

            outputs[node_name] = model_output

        flow.outputs = outputs

        return flow

    @classmethod
    def _process_parameters(cls, job_config: _cfg.JobConfig) -> _cfg.JobConfig:

        if job_config.job.jobType == _meta.JobType.RUN_MODEL:

            job_details = job_config.job.runModel
            model_key = _util.object_key(job_details.model)
            model_or_flow = job_config.resources[model_key].model

        elif job_config.job.jobType == _meta.JobType.RUN_FLOW:

            job_details = job_config.job.runFlow
            flow_key = _util.object_key(job_details.flow)
            model_or_flow = job_config.resources[flow_key].flow

        else:
            raise _ex.EUnexpected()

        param_specs = model_or_flow.parameters
        param_values = job_details.parameters

        encoded_params = cls._process_parameters_dict(param_specs, param_values)

        job_details = copy.copy(job_details)
        job_def = copy.copy(job_config.job)
        job_config = copy.copy(job_config)

        if job_config.job.jobType == _meta.JobType.RUN_MODEL:
            job_def.runModel = job_details
        else:
            job_def.runFlow = job_details

        job_details.parameters = encoded_params
        job_config.job = job_def

        return job_config

    @classmethod
    def _process_parameters_dict(
            cls, param_specs: tp.Dict[str, _meta.ModelParameter],
            param_values: tp.Dict[str, _meta.Value]) -> tp.Dict[str, _meta.Value]:

        unknown_params = list(filter(lambda p: p not in param_specs, param_values))

        if any(unknown_params):
            msg = f"Unknown parameters cannot be translated: [{', '.join(unknown_params)}]"
            cls._log.error(msg)
            raise _ex.EConfigParse(msg)

        encoded_values = dict()

        for p_name, p_value in param_values.items():

            if isinstance(p_value, _meta.Value):
                encoded_values[p_name] = p_value

            else:
                p_spec = param_specs[p_name]

                cls._log.info(f"Encoding parameter [{p_name}] as {p_spec.paramType.basicType}")

                encoded_value = _types.MetadataCodec.convert_value(p_value, p_spec.paramType)
                encoded_values[p_name] = encoded_value

        return encoded_values

    @classmethod
    def _process_job_io(
            cls, sys_config, data_key, data_value, data_id, storage_id,
            new_unique_file=False, schema: tp.Optional[_meta.SchemaDefinition] = None):

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
                existing_files = x_storage.ls(str(x_orig_path.parent))
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

        return data_obj, storage_obj

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
            data_def.schema = _meta.SchemaDefinition(schemaType=_meta.SchemaType.TABLE, table=_meta.TableSchema())

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
