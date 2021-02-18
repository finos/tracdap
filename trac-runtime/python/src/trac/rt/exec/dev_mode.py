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
import trac.rt.impl.util as util


class DevModeTranslator:

    _log: tp.Optional[util.logging.Logger] = None

    @classmethod
    def translate_dev_mode_config(
            cls,
            job_config: cfg.JobConfig,
            sys_config: cfg.RuntimeConfig,
            model_class: tp.Optional[api.TracModel.__class__]) \
            -> (cfg.JobConfig, cfg.RuntimeConfig):

        cls._log.info(f"Applying dev mode config translation")

        if model_class is not None:
            job_config, sys_config = cls._generate_integrated_model_definition(model_class, job_config, sys_config)

        original_inputs = job_config.inputs
        original_objects = job_config.objects
        translated_inputs = copy.copy(original_inputs)
        translated_objects = job_config.objects

        for input_key, input_value in original_inputs.items():

            # Inputs that refer to an existing object definition do not need dev mode translation
            if isinstance(input_value, str) and input_value in original_objects:
                continue

            if isinstance(input_value, str):
                storage_path = input_value
                storage_key = sys_config.storageSettings.defaultStorage
                storage_format = sys_config.storageSettings.defaultFormat

            elif isinstance(input_value, dict):

                storage_path = input_value.get("path")

                if not storage_path:
                    raise RuntimeError(f"Invalid configuration for input '{input_key}' (missing required value 'path'")

                storage_key = input_value.get("storageKey") or sys_config.storageSettings.defaultStorage
                storage_format = input_value.get("format") or sys_config.storageSettings.defaultFormat

            else:
                raise RuntimeError(f"Invalid configuration for input '{input_key}'")

            data_id = uuid.uuid4()
            storage_id = uuid.uuid4()

            cls._log.info(f"Generating data definition for '{input_key}' (assigned ID {data_id})")

            data_obj, storage_obj = cls._generate_input_definition(
                data_id, storage_id, storage_key, storage_path, storage_format)

            translated_objects[str(data_id)] = data_obj
            translated_objects[str(storage_id)] = storage_obj
            translated_inputs[input_key] = str(data_id)

        job_config = copy.copy(job_config)
        job_config.objects = translated_objects
        job_config.inputs = translated_inputs

        return job_config, sys_config

    @classmethod
    def _generate_integrated_model_definition(
            cls, model_class: api.TracModel.__class__,
            job_config: cfg.JobConfig,
            sys_config: cfg.RuntimeConfig) \
            -> (cfg.JobConfig, cfg.RuntimeConfig):

        model_id = uuid.uuid4()

        cls._log.info(f"Generating model definition for '{model_class.__name__}' (assigned ID {model_id})")

        modeL_def = meta.ModelDefinition(  # noqa
            language="python",
            repository="trac_integrated",
            entryPoint=f"{model_class.__module__}.{model_class.__name__}",

            path="",
            repositoryVersion="",
            input={},
            output={},
            param={},
            overlay=False,
            schemaUnchanged=False)

        model_object = meta.ObjectDefinition(
            objectType=meta.ObjectType.MODEL,
            model=modeL_def)

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
            storage_key: str, storage_path: str, storage_format: str) \
            -> (meta.ObjectDefinition, meta.ObjectDefinition):

        part_key = meta.DataDefinition.PartKey(
            opaqueKey="part-root",
            partType=meta.DataDefinition.PartType.PART_ROOT)

        snap_index = 1
        delta_index = 1
        data_item_id = f"DATA:{data_id}:{part_key.opaqueKey}:{snap_index}:{delta_index}"

        delta = meta.DataDefinition.Delta(
            deltaIndex=delta_index,
            dataItemId=data_item_id)

        snap = meta.DataDefinition.Snap(
            snapIndex=snap_index,
            deltas=[delta])

        part = meta.DataDefinition.Part(
            partKey=part_key,
            snap=snap)

        data_def = meta.DataDefinition(parts={})
        data_def.storageId = str(storage_id)
        data_def.schema = meta.TableDefinition()
        data_def.parts[part_key.opaqueKey] = part

        storage_copy = meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=meta.CopyStatus.COPY_AVAILABLE)

        storage_incarnation = meta.StorageIncarnation(
            incarnationIndex=1,
            incarnationTimestamp=meta.DatetimeValue(isoDatetime=""),  # TODO: Timestamp
            incarnationStatus=meta.IncarnationStatus.INCARNATION_AVAILABLE,
            copies=[storage_copy])

        storage_item = meta.StorageItem(
            incarnations=[storage_incarnation])

        storage_def = meta.StorageDefinition(dataItems={})
        storage_def.dataItems[delta.dataItemId] = storage_item

        data_obj = meta.ObjectDefinition(objectType=meta.ObjectType.DATA, data=data_def)
        storage_obj = meta.ObjectDefinition(objectType=meta.ObjectType.STORAGE, storage=storage_def)

        return data_obj, storage_obj


DevModeTranslator._log = util.logger_for_class(DevModeTranslator)
