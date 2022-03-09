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

import tracdap.rt.api as api
import tracdap.rt.metadata as meta
import tracdap.rt.config as cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.models as _models
import tracdap.rt.impl.storage as _storage
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.util as util


DEV_MODE_JOB_CONFIG = [
    re.compile(r"job\.run(Model|Flow)\.parameters\.[\w]+"),
    re.compile(r"job\.run(Model|Flow)\.inputs\.[\w]+"),
    re.compile(r"job\.run(Model|Flow)\.outputs\.[\w]+")]

DEV_MODE_SYS_CONFIG = []


class DevModeTranslator:

    _log: tp.Optional[util.logging.Logger] = None

    @classmethod
    def translate_dev_mode_config(
            cls,
            sys_config_dir: pathlib.Path,
            sys_config: cfg.RuntimeConfig,
            job_config: cfg.JobConfig,
            model_class: tp.Optional[api.TracModel.__class__]) \
            -> (cfg.JobConfig, cfg.RuntimeConfig):

        cls._log.info(f"Applying dev mode config translation")

        if not job_config.jobId:
            job_config = cls._process_job_id(job_config)

        if job_config.job.jobType is None or job_config.job.jobType == meta.JobType.JOB_TYPE_NOT_SET:
            job_config = cls._process_job_type(job_config)

        if model_class is not None:
            job_config, sys_config = cls._process_model_definition(model_class, job_config, sys_config)

        if job_config.job.jobType in [meta.JobType.RUN_MODEL, meta.JobType.RUN_FLOW]:
            job_config = cls._process_parameters(job_config)

        if job_config.job.jobType != meta.JobType.RUN_MODEL:
            return job_config, sys_config

        original_inputs = job_config.job.runModel.inputs
        original_outputs = job_config.job.runModel.outputs
        original_resources = job_config.resources

        translated_inputs = copy.copy(original_inputs)
        translated_outputs = copy.copy(original_outputs)
        translated_resources = copy.copy(job_config.resources)

        def process_input_or_output(data_key, data_value, is_input: bool):

            data_id = util.new_object_id(meta.ObjectType.DATA)
            storage_id = util.new_object_id(meta.ObjectType.STORAGE)

            data_obj, storage_obj = cls._process_job_io(
                sys_config, sys_config_dir,
                data_key, data_value, data_id, storage_id,
                new_unique_file=not is_input)

            translated_resources[util.object_key(data_id)] = data_obj
            translated_resources[util.object_key(storage_id)] = storage_obj

            if is_input:
                translated_inputs[data_key] = cls._selector_for(data_id)
            else:
                translated_outputs[data_key] = cls._selector_for(data_id)

        for input_key, input_value in original_inputs.items():
            if not (isinstance(input_value, str) and input_value in original_resources):
                process_input_or_output(input_key, input_value, is_input=True)

        for output_key, output_value in original_outputs.items():
            if not (isinstance(output_value, str) and output_value in original_outputs):
                process_input_or_output(output_key, output_value, is_input=False)

        job_config = copy.copy(job_config)
        job_config.resources = translated_resources
        job_config.job.runModel.inputs = translated_inputs
        job_config.job.runModel.outputs = translated_outputs

        return job_config, sys_config

    @staticmethod
    def _selector_for(object_id: meta.TagHeader):

        return meta.TagSelector(
            objectType=object_id.objectType,
            objectId=object_id.objectId,
            objectVersion=object_id.objectVersion,
            tagVersion=object_id.objectVersion)

    @classmethod
    def _process_job_id(cls, job_config: cfg.JobConfig):

        job_id = util.new_object_id(meta.ObjectType.JOB)

        cls._log.info(f"Assigning job ID = [{util.object_key(job_id)}]")

        translated_config = copy.copy(job_config)
        translated_config.jobId = job_id

        return translated_config

    @classmethod
    def _process_job_type(cls, job_config: cfg.JobConfig):

        if job_config.job.runModel is not None:
            job_type = meta.JobType.RUN_MODEL

        elif job_config.job.runFlow is not None:
            job_type = meta.JobType.RUN_FLOW

        elif job_config.job.importModel is not None:
            job_type = meta.JobType.IMPORT_MODEL

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
    def _process_parameters(cls, job_config: cfg.JobConfig) -> cfg.JobConfig:

        if job_config.job.jobType == meta.JobType.RUN_MODEL:

            job_details = job_config.job.runModel
            model_key = util.object_key(job_details.model)
            model_or_flow = job_config.resources[model_key].model

        elif job_config.job.jobType == meta.JobType.RUN_FLOW:

            job_details = job_config.job.runFlow
            flow_key = util.object_key(job_details.flow)
            model_or_flow = job_config.resources[flow_key].flow

        else:
            raise _ex.EUnexpected()

        param_specs = model_or_flow.parameters
        param_values = job_details.parameters

        encoded_params = cls._process_parameters_dict(param_specs, param_values)

        job_details = copy.copy(job_details)
        job_def = copy.copy(job_config.job)
        job_config = copy.copy(job_config)

        if job_config.job.jobType == meta.JobType.RUN_MODEL:
            job_def.runModel = job_details
        else:
            job_def.runFlow = job_details

        job_details.parameters = encoded_params
        job_config.job = job_def

        return job_config

    @classmethod
    def _process_parameters_dict(
            cls, param_specs: tp.Dict[str, meta.ModelParameter],
            param_values: tp.Dict[str, meta.Value]) -> tp.Dict[str, meta.Value]:

        unknown_params = list(filter(lambda p: p not in param_specs, param_values))

        if any(unknown_params):
            msg = f"Unknown parameters cannot be translated: [{', '.join(unknown_params)}]"
            cls._log.error(msg)
            raise _ex.EConfigParse(msg)

        encoded_values = dict()

        for p_name, p_value in param_values.items():

            if isinstance(p_value, meta.Value):
                encoded_values[p_name] = p_value

            else:
                p_spec = param_specs[p_name]

                cls._log.info(f"Encoding parameter [{p_name}] as {p_spec.paramType.basicType}")

                encoded_value = _types.convert_value(p_value, p_spec.paramType)
                encoded_values[p_name] = encoded_value

        return encoded_values

    @classmethod
    def _process_model_definition(
            cls, model_class: api.TracModel.__class__,
            job_config: cfg.JobConfig,
            sys_config: cfg.RuntimeConfig) \
            -> (cfg.JobConfig, cfg.RuntimeConfig):

        # Add the integrated model repo trac_integrated

        repos = copy.copy(sys_config.repositories)
        repos["trac_integrated"] = cfg.RepositoryConfig("integrated")
        translated_sys_config = copy.copy(sys_config)
        translated_sys_config.repositories = repos

        model_id = util.new_object_id(meta.ObjectType.MODEL)
        model_key = util.object_key(model_id)

        cls._log.info(f"Generating model definition for [{model_class.__name__}] with ID = [{model_key}]")

        skeleton_modeL_def = meta.ModelDefinition(  # noqa
            language="python",
            repository="trac_integrated",
            entryPoint=f"{model_class.__module__}.{model_class.__name__}",

            parameters={},
            inputs={},
            outputs={})

        loader = _models.ModelLoader(translated_sys_config)

        try:
            loader.create_scope("DEV_MODE_TRANSLATION")
            model_class = loader.load_model_class("DEV_MODE_TRANSLATION", skeleton_modeL_def)
            model_scan = loader.scan_model(model_class)
        finally:
            loader.destroy_scope("DEV_MODE_TRANSLATION")

        model_def = meta.ModelDefinition(  # noqa
            language="python",
            repository="trac_integrated",
            entryPoint=f"{model_class.__module__}.{model_class.__name__}",

            parameters=model_scan.parameters,
            inputs=model_scan.inputs,
            outputs=model_scan.outputs)

        model_object = meta.ObjectDefinition(
            objectType=meta.ObjectType.MODEL,
            model=model_def)

        translated_job_config = copy.copy(job_config)
        translated_job_config.job.runModel.model = model_id
        translated_job_config.resources = copy.copy(job_config.resources)
        translated_job_config.resources[model_key] = model_object

        return translated_job_config, translated_sys_config

    @classmethod
    def _process_job_io(
            cls, sys_config, sys_config_dir,
            data_key, data_value, data_id, storage_id,
            new_unique_file=False):

        if isinstance(data_value, str):
            storage_path = data_value
            storage_key = sys_config.storageSettings.defaultStorage
            storage_format = sys_config.storageSettings.defaultFormat
            snap_version = 1

        elif isinstance(data_value, dict):

            storage_path = data_value.get("path")

            if not storage_path:
                raise _ex.EConfigParse(f"Invalid configuration for input [{data_key}] (missing required value 'path'")

            storage_key = data_value.get("storageKey") or sys_config.storageSettings.defaultStorage
            storage_format = data_value.get("format") or sys_config.storageSettings.defaultFormat
            snap_version = 1

        else:
            raise _ex.EConfigParse(f"Invalid configuration for input '{data_key}'")

        cls._log.info(f"Generating data definition for [{data_key}] with ID = [{util.object_key(data_id)}]")

        # For unique outputs, increment the snap number to find a new unique snap
        # These are not incarnations, bc likely in dev mode model code and inputs are changing
        # Incarnations are for recreation of a dataset using the exact same code path and inputs

        if new_unique_file:

            x_storage_mgr = _storage.StorageManager(sys_config, sys_config_dir)
            x_storage = x_storage_mgr.get_file_storage(storage_key)
            x_orig_path = pathlib.PurePath(storage_path)

            while x_storage.exists(storage_path):

                snap_version += 1
                x_stem = f"{x_orig_path.stem}-{snap_version}"
                storage_path = str(x_orig_path.parent.joinpath(x_stem))

            cls._log.info(f"Output for [{data_key}] will be snap version {snap_version}")

        data_obj, storage_obj = cls._generate_input_definition(
            data_id, storage_id, storage_key, storage_path, storage_format,
            snap_index=snap_version, delta_index=1, incarnation_index=1)

        return data_obj, storage_obj

    @classmethod
    def _generate_input_definition(
            cls, data_id: meta.TagHeader, storage_id: meta.TagHeader,
            storage_key: str, storage_path: str, storage_format: str,
            snap_index: int, delta_index: int, incarnation_index: int) \
            -> (meta.ObjectDefinition, meta.ObjectDefinition):

        part_key = meta.PartKey(
            opaqueKey="part-root",
            partType=meta.PartType.PART_ROOT)

        data_item = f"data/table/{data_id.objectId}/{part_key.opaqueKey}/snap-{snap_index}/delta-{delta_index}-x000000"

        delta = meta.DataDefinition.Delta(
            deltaIndex=delta_index,
            dataItem=data_item)

        snap = meta.DataDefinition.Snap(
            snapIndex=snap_index,
            deltas=[delta])

        part = meta.DataDefinition.Part(
            partKey=part_key,
            snap=snap)

        data_def = meta.DataDefinition(parts={})

        data_def.storageId = meta.TagSelector(
            meta.ObjectType.STORAGE, storage_id.objectId,
            objectVersion=storage_id.objectVersion, latestTag=True)

        data_def.schema = meta.SchemaDefinition(schemaType=meta.SchemaType.TABLE, table=meta.TableSchema())
        data_def.parts[part_key.opaqueKey] = part

        storage_copy = meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=meta.CopyStatus.COPY_AVAILABLE)

        storage_incarnation = meta.StorageIncarnation(
            incarnationIndex=incarnation_index,
            incarnationTimestamp=storage_id.objectTimestamp,
            incarnationStatus=meta.IncarnationStatus.INCARNATION_AVAILABLE,
            copies=[storage_copy])

        storage_item = meta.StorageItem(
            incarnations=[storage_incarnation])

        storage_def = meta.StorageDefinition(dataItems={})
        storage_def.dataItems[delta.dataItem] = storage_item

        data_obj = meta.ObjectDefinition(objectType=meta.ObjectType.DATA, data=data_def)
        storage_obj = meta.ObjectDefinition(objectType=meta.ObjectType.STORAGE, storage=storage_def)

        return data_obj, storage_obj


DevModeTranslator._log = util.logger_for_class(DevModeTranslator)
