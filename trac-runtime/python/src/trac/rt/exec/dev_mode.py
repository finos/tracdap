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
import copy
import pathlib
import uuid

import trac.rt.api as api
import trac.rt.metadata as meta
import trac.rt.config as cfg
import trac.rt.exceptions as _ex
import trac.rt.impl.repositories as _repos
import trac.rt.impl.storage as _storage
import trac.rt.impl.util as util


class DevModeTranslator:

    _log: tp.Optional[util.logging.Logger] = None

    @classmethod
    def translate_dev_mode_config(
            cls,
            sys_config_dir: pathlib.Path,
            sys_config: cfg.SystemConfig,
            job_config: cfg.JobConfig,
            model_class: tp.Optional[api.TracModel.__class__]) \
            -> (cfg.JobConfig, cfg.SystemConfig):

        cls._log.info(f"Applying dev mode config translation")

        if not job_config.job_id:

            job_id = uuid.uuid4()
            job_config = copy.copy(job_config)
            job_config.job_id = job_id

            cls._log.info(f"Assigned dev mode job ID = {job_config.job_id}")

        else:

            cls._log.info(f"Using dev mode job ID = {job_config.job_id}")

        if model_class is not None:
            job_config, sys_config = cls._generate_integrated_model_definition(model_class, job_config, sys_config)

        original_inputs = job_config.inputs
        original_outputs = job_config.outputs
        original_objects = job_config.objects

        translated_inputs = copy.copy(original_inputs)
        translated_outputs = copy.copy(original_outputs)
        translated_objects = job_config.objects

        def process_input_or_output(data_key, data_value, is_input: bool):

            data_id = uuid.uuid4()
            storage_id = uuid.uuid4()

            data_obj, storage_obj = cls._process_job_io(
                sys_config, sys_config_dir,
                data_key, data_value, data_id, storage_id,
                new_unique_file=not is_input)

            translated_objects[str(data_id)] = data_obj
            translated_objects[str(storage_id)] = storage_obj

            if is_input:
                translated_inputs[data_key] = str(data_id)
            else:
                translated_outputs[data_key] = str(data_id)

        for input_key, input_value in original_inputs.items():
            if not (isinstance(input_value, str) and input_value in original_objects):
                process_input_or_output(input_key, input_value, is_input=True)

        for output_key, output_value in original_outputs.items():
            if not (isinstance(output_value, str) and output_value in original_outputs):
                process_input_or_output(output_key, output_value, is_input=False)

        job_config = copy.copy(job_config)
        job_config.objects = translated_objects
        job_config.inputs = translated_inputs
        job_config.outputs = translated_outputs

        return job_config, sys_config

    @classmethod
    def _process_job_io(
            cls, sys_config, sys_config_dir,
            data_key, data_value, data_id, storage_id,
            new_unique_file=False):

        if isinstance(data_value, str):
            storage_path = data_value
            storage_key = sys_config.storageSettings.defaultStorage
            storage_format = sys_config.storageSettings.defaultFormat
            snap = 1

        elif isinstance(data_value, dict):

            storage_path = data_value.get("path")

            if not storage_path:
                raise _ex.EConfigParse(f"Invalid configuration for input '{data_key}' (missing required value 'path'")

            storage_key = data_value.get("storageKey") or sys_config.storageSettings.defaultStorage
            storage_format = data_value.get("format") or sys_config.storageSettings.defaultFormat
            snap = 1

        else:
            raise _ex.EConfigParse(f"Invalid configuration for input '{data_key}'")

        cls._log.info(f"Generating data definition for '{data_key}' (assigned ID {data_id})")

        # For unique outputs, increment the snap number to find a new unique snap
        # These are not incarnations, bc likely in dev mode model code and inputs are changing
        # Incarnations are for recreation of a dataset using the exact same code path and inputs

        if new_unique_file:

            x_storage_mgr = _storage.StorageManager(sys_config, sys_config_dir)
            x_storage = x_storage_mgr.get_file_storage(storage_key)
            x_orig_path = pathlib.PurePath(storage_path)

            while x_storage.exists(storage_path):

                snap += 1
                x_stem = f"{x_orig_path.stem}-{snap}"
                storage_path = str(x_orig_path.parent.joinpath(x_stem))

            cls._log.info(f"Output for {data_key} will be snap version {snap}")

        data_obj, storage_obj = cls._generate_input_definition(
            data_id, storage_id, storage_key, storage_path, storage_format,
            snap_index=snap, delta_index=1, incarnation_index=1)

        return data_obj, storage_obj

    @classmethod
    def _generate_integrated_model_definition(
            cls, model_class: api.TracModel.__class__,
            job_config: cfg.JobConfig,
            sys_config: cfg.SystemConfig) \
            -> (cfg.JobConfig, cfg.SystemConfig):

        model_id = uuid.uuid4()

        cls._log.info(f"Generating model definition for '{model_class.__name__}' (assigned ID {model_id})")

        skeleton_modeL_def = meta.ModelDefinition(  # noqa
            language="python",
            repository="trac_integrated",
            entryPoint=f"{model_class.__module__}.{model_class.__name__}",

            parameters={},
            inputs={},
            outputs={})

        loader = _repos.IntegratedModelLoader(cfg.RepositoryConfig(repoType="INTEGRATED", repoUrl=""))
        model_class = loader.load_model(skeleton_modeL_def)
        model: api.TracModel = model_class()

        model_params = model.define_parameters()
        model_inputs = model.define_inputs()
        model_outputs = model.define_outputs()

        model_def = meta.ModelDefinition(  # noqa
            language="python",
            repository="trac_integrated",
            entryPoint=f"{model_class.__module__}.{model_class.__name__}",

            parameters=model_params,
            inputs=model_inputs,
            outputs=model_outputs)

        model_object = meta.ObjectDefinition(
            objectType=meta.ObjectType.MODEL,
            model=model_def)

        translated_job_config = copy.copy(job_config)
        translated_job_config.target = model_id
        translated_job_config.objects = copy.copy(job_config.objects)
        translated_job_config.objects[model_id] = model_object

        repos = copy.copy(sys_config.repositories)
        repos["trac_integrated"] = cfg.RepositoryConfig("integrated")

        translated_sys_config = copy.copy(sys_config)
        translated_sys_config.repositories = repos

        return translated_job_config, translated_sys_config

    @classmethod
    def _generate_input_definition(
            cls, data_id: uuid.UUID, storage_id: uuid.UUID,
            storage_key: str, storage_path: str, storage_format: str,
            snap_index: int, delta_index: int, incarnation_index: int) \
            -> (meta.ObjectDefinition, meta.ObjectDefinition):

        part_key = meta.PartKey(
            opaqueKey="part-root",
            partType=meta.PartType.PART_ROOT)

        data_item = f"DATA:{data_id}:{part_key.opaqueKey}:{snap_index}:{delta_index}"

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
            meta.ObjectType.STORAGE, str(storage_id),
            latestObject=True, latestTag=True)

        data_def.schema = meta.SchemaDefinition(schemaType=meta.SchemaType.TABLE, table=meta.TableSchema())
        data_def.parts[part_key.opaqueKey] = part

        storage_copy = meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=meta.CopyStatus.COPY_AVAILABLE)

        storage_incarnation = meta.StorageIncarnation(
            incarnationIndex=incarnation_index,
            incarnationTimestamp=meta.DatetimeValue(isoDatetime=""),  # TODO: Timestamp
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
