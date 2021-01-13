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

import trac.rt.api as api
import trac.rt.metadata as meta
import trac.rt.config as cfg
import trac.rt.impl.util as util


class DevModeTranslator:

    _log: tp.Optional[util.logging.Logger] = None

    @classmethod
    def translate_integrated_launch(
            cls, model_class: type,
            job_config: cfg.JobConfig,
            sys_config: cfg.RuntimeConfig) \
            -> (cfg.JobConfig, cfg.RuntimeConfig):

        cls._log.info(f"Applying translation for integrated model launch")

        model_id = "launch_target"  # TODO: Generate dummy ID

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


DevModeTranslator._log = util.logger_for_class(DevModeTranslator)
