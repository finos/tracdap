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

import yaml
import json
import datetime as dt
import sys
import pathlib
import typing as tp

import trac.rt.api as api
import trac.rt.config.config as config
import trac.rt.impl.config_parser as cfg
import trac.rt.impl.util as util
import trac.rt.impl.repositories as repos

import trac.rt.exec.actors as actors
import trac.rt.exec.engine as engine
import trac.rt.exec.dev_mode as dev_mode


class TracRuntime:

    def __init__(self, sys_config_path: str, job_config_path: tp.Optional[str] = None,
                 dev_mode: bool = False, model_class: api.TracModel.__class__ = None):

        python_version = sys.version.replace("\n", "")
        mode = "batch" if job_config_path else "service"

        print(f">>> TRAC Python Runtime {'DEVELOPMENT VERSION'} starting {mode} at {dt.datetime.now()}")
        print(f">>> Python installation: {python_version} ({sys.exec_prefix})")
        print(f">>> Working directory: {pathlib.Path.cwd()}")
        print(f">>> System config: {sys_config_path}")

        if job_config_path:
            print(f">>> Job config: {job_config_path}")

        if dev_mode:
            print(f">>> Development mode enabled (DO NOT USE THIS IN PRODUCTION)")

        util.configure_logging(self.__class__)
        self._log = util.logger_for_object(self)

        self._sys_config_path = sys_config_path
        self._sys_config: tp.Optional[config.RuntimeConfig] = None

        if job_config_path:
            self._batch_mode = True
            self._job_config_path = job_config_path
            self._job_config: tp.Optional[config.JobConfig] = None
        else:
            self._batch_mode = False

        self._dev_mode = dev_mode
        self._model_class = model_class

        # Top level resources
        self._repos: tp.Optional[repos.Repositories] = None

        # The execution engine
        self._engine: tp.Optional[engine.TracEngine] = None
        self._system: tp.Optional[actors.ActorSystem] = None

    # ------------------------------------------------------------------------------------------------------------------
    # Runtime service control
    # ------------------------------------------------------------------------------------------------------------------

    def __enter__(self):
        self.start(wait=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def pre_start(self):

        # Plugins will be loaded here, before config

        self._log.info("Loading system config...")
        raw_sys_config = cfg.ConfigParser.load_raw_config(self._sys_config_path)
        self._sys_config = cfg.ConfigParser(config.RuntimeConfig).parse(raw_sys_config, self._sys_config_path)

        if self._batch_mode:
            self._log.info("Loading job config...")
            raw_job_config = cfg.ConfigParser.load_raw_config(self._job_config_path)
            self._job_config = cfg.ConfigParser(config.JobConfig).parse(raw_job_config, self._job_config_path)

        if self._dev_mode:

            if self._model_class:
                job_config, sys_config = dev_mode.DevModeTranslator \
                    .translate_integrated_launch(self._model_class, self._job_config, self._sys_config)
                self._job_config = job_config
                self._sys_config = sys_config

    def start(self, wait: bool = False):

        self._log.info("Starting the engine")

        self._repos = repos.Repositories(self._sys_config)

        self._engine = engine.TracEngine(self._sys_config, self._repos, batch_mode=self._batch_mode)
        self._system = actors.ActorSystem(self._engine, system_thread="engine")

        self._system.start(wait=wait)

    def stop(self):

        self._log.info("Begin shutdown sequence")
        self._system.stop()

    def wait_for_shutdown(self):

        self._system.wait_for_shutdown()

    # ------------------------------------------------------------------------------------------------------------------
    # Job submission
    # ------------------------------------------------------------------------------------------------------------------

    def submit_job(self, job_config_path: str):

        if self._batch_mode:
            raise RuntimeError()  # TODO: Error

        self._system.send("submit_job", job_config_path)

    def submit_batch(self):

        if not self._batch_mode:
            raise RuntimeError()  # TODO Error

        self._system.send("submit_job", self._job_config)
