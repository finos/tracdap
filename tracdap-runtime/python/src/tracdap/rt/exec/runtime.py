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

import datetime as dt
import sys
import pathlib
import typing as tp

import tracdap.rt.api as _api
import tracdap.rt.config as _cfg
import tracdap.rt._version as _version
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.config_parser as _cparse
import tracdap.rt.impl.util as util
import tracdap.rt.impl.models as _models
import tracdap.rt.impl.storage as _storage

import tracdap.rt.exec.actors as _actors
import tracdap.rt.exec.engine as _engine
import tracdap.rt.exec.dev_mode as _dev_mode


class TracRuntime:

    def __init__(
            self,
            sys_config: tp.Union[str, pathlib.Path, _cfg.RuntimeConfig],
            job_config: tp.Union[str, pathlib.Path, _cfg.JobConfig, None] = None,
            job_result_dir: tp.Union[str, pathlib.Path, None] = None,
            job_result_format: tp.Optional[str] = None,
            dev_mode: bool = False,
            model_class: tp.Optional[_api.TracModel.__class__] = None):

        trac_version = _version.__version__
        python_version = sys.version.replace("\n", "")
        mode = "batch" if job_config else "service"

        sys_config_path = "[embedded]" if isinstance(sys_config, _cfg.RuntimeConfig) else sys_config
        job_config_path = "[embedded]" if isinstance(job_config, _cfg.JobConfig) else job_config

        print(f">>> TRAC Python Runtime {trac_version} starting in {mode} mode at {dt.datetime.now()}")
        print(f">>> Python installation: {python_version} ({sys.exec_prefix})")
        print(f">>> System config: {sys_config_path}")

        if job_config:
            print(f">>> Job config: {job_config_path}")

        if dev_mode:
            print(f">>> Development mode enabled (DO NOT USE THIS IN PRODUCTION)")

        util.configure_logging()
        self._log = util.logger_for_object(self)
        self._log.info(f"TRAC Python Runtime {trac_version}")

        self._sys_config_dir = pathlib.Path(sys_config_path).parent
        self._sys_config_path = sys_config_path
        self._job_config_path = job_config_path
        self._job_result_dir = job_result_dir
        self._job_result_format = job_result_format
        self._batch_mode = bool(job_config is not None)
        self._dev_mode = dev_mode
        self._model_class = model_class

        self._sys_config = sys_config if isinstance(sys_config, _cfg.RuntimeConfig) else None
        self._job_config = job_config if isinstance(job_config, _cfg.JobConfig) else None

        # Top level resources
        self._models: tp.Optional[_models.ModelLoader] = None
        self._storage: tp.Optional[_storage.StorageManager] = None

        # The execution engine
        self._engine: tp.Optional[_engine.TracEngine] = None
        self._system: tp.Optional[_actors.ActorSystem] = None

    # ------------------------------------------------------------------------------------------------------------------
    # Runtime service control
    # ------------------------------------------------------------------------------------------------------------------

    def __enter__(self):
        self.start(wait=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        # Do not call self.stop()
        # In batch mode, the engine will stop itself once the job is complete, so no need to stop twice
        # It may be necessary to call self.stop() in service mode, depending on how stop is triggered (e.g. interrupt)

        pass

    def pre_start(self):

        try:

            self._log.info(f"Beginning pre-start sequence...")

            # Plugins will be loaded here, before config

            # Load sys and job config (or use embedded)

            if self._sys_config is None:
                sys_config_dev_mode = _dev_mode.DEV_MODE_SYS_CONFIG if self._dev_mode else None
                sys_config_parser = _cparse.ConfigParser(_cfg.RuntimeConfig, sys_config_dev_mode)
                sys_config_raw = sys_config_parser.load_raw_config(self._sys_config_path, config_file_name="system")
                self._sys_config = sys_config_parser.parse(sys_config_raw, self._sys_config_path)
            else:
                self._log.info("Using embedded system config")

            if self._job_config is None and self._batch_mode:
                job_config_dev_mode = _dev_mode.DEV_MODE_JOB_CONFIG if self._dev_mode else None
                job_config_parser = _cparse.ConfigParser(_cfg.JobConfig, job_config_dev_mode)
                job_config_raw = job_config_parser.load_raw_config(self._job_config_path, config_file_name="job")
                self._job_config = job_config_parser.parse(job_config_raw, self._job_config_path)
            elif self._batch_mode:
                self._log.info("Using embedded job config")

            # Dev mode translation is controlled by the dev mode flag
            # I.e. it can be applied to embedded configs

            if self._dev_mode:

                job_config, sys_config = _dev_mode.DevModeTranslator.translate_dev_mode_config(
                    self._sys_config_dir, self._sys_config, self._job_config, self._model_class)

                self._job_config = job_config
                self._sys_config = sys_config

        except Exception as e:
            self._handle_startup_error(e)

    def start(self, wait: bool = False):

        try:

            self._log.info("Starting the engine...")

            self._models = _models.ModelLoader(self._sys_config)
            self._storage = _storage.StorageManager(self._sys_config, self._sys_config_dir)

            self._engine = _engine.TracEngine(
                self._sys_config,
                self._models, self._storage,
                batch_mode=self._batch_mode)

            self._system = _actors.ActorSystem(self._engine, system_thread="engine")
            self._system.start(wait=wait)

        except Exception as e:
            self._handle_startup_error(e)

    def stop(self):

        self._log.info("Shutting down the engine")
        self._system.stop()

    def wait_for_shutdown(self):

        self._system.wait_for_shutdown()

        if self._system.shutdown_code() == 0:
            self._log.info("TRAC runtime has gone down cleanly")
        else:
            self._log.error("TRAC runtime has gone down with errors")

            # For now, use the final error that propagated in the engine as the shutdown error
            # A more sophisticated approach would handle job-related errors by writing job output metadata
            raise self._system.shutdown_error()

    # ------------------------------------------------------------------------------------------------------------------
    # Job submission
    # ------------------------------------------------------------------------------------------------------------------

    def submit_job(self, job_config_path: str):

        if self._batch_mode:
            msg = "Additional jobs cannot be submitted in batch mode"
            self._log.error(msg)
            raise _ex.EJobValidation(msg)

        self._system.send("submit_job", job_config_path)

    def submit_batch(self):

        if not self._batch_mode:
            msg = "Batch jobs cannot be triggered in service mode"
            self._log.error(msg)
            raise _ex.EJobValidation(msg)

        self._system.send(
            "submit_job", self._job_config,
            str(self._job_result_dir) if self._job_result_dir else "",
            self._job_result_format if self._job_result_format else "")

    # ------------------------------------------------------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------------------------------------------------------

    def _handle_startup_error(self, error: Exception):

        try:
            raise error

        # Internal error - something has gone wrong with the engine
        # Propagate original exception, this is not a startup error
        except _ex.ETracInternal:
            self._log.error(f"TRAC runtime failed to start due to an internal error (this is a bug)")
            raise

        # An expected startup error - e.g. missing or bad config, properly handled in the startup code
        # Propagate as EStartup
        except _ex.EStartup as e:
            self._log.error(f"TRAC runtime failed to start: {e}")
            raise

        # An expected error, but without explict handling in te startup code
        # Wrap expected errors during the startup sequence in EStartup
        except _ex.ETrac as e:
            self._log.error(f"TRAC runtime failed to start: {e}")
            raise _ex.EStartup("TRAC runtime failed to start") from e

        # The worst case, an error that was never handled at all, e.g. an unexpected null reference
        # Wrap with EUnexpected so at least we don't exit with a raw exception from somewhere deep in the stack!
        except Exception as e:
            self._log.error(f"TRAC runtime failed to start due to an unhandled error (this is a bug)")
            raise _ex.EUnexpected("An unhandled error propagated to the top level error handler (this is a bug)") from e
