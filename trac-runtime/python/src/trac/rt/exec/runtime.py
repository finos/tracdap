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

import datetime as dt
import sys
import pathlib
import typing as tp

import trac.rt.api as api
import trac.rt.config.config as config
import trac.rt.exceptions as _ex
import trac.rt.impl.config_parser as cfg
import trac.rt.impl.util as util
import trac.rt.impl.repositories as repos
import trac.rt.impl.storage as storage

import trac.rt.exec.actors as actors
import trac.rt.exec.engine as engine
import trac.rt.exec.dev_mode as _dev_mode


class TracRuntime:

    def __init__(
            self,
            sys_config_path: tp.Union[str, pathlib.Path],
            job_config_path: tp.Optional[tp.Union[str, pathlib.Path]] = None,
            dev_mode: bool = False,
            model_class: tp.Optional[api.TracModel.__class__] = None):

        trac_version = 'DEVELOPMENT VERSION'  # TODO: Bring in version from the version script
        python_version = sys.version.replace("\n", "")
        mode = "batch" if job_config_path else "service"

        print(f">>> TRAC Python Runtime {trac_version} starting in {mode} mode at {dt.datetime.now()}")
        print(f">>> Python installation: {python_version} ({sys.exec_prefix})")
        print(f">>> System config: {sys_config_path}")

        if job_config_path:
            print(f">>> Job config: {job_config_path}")

        if dev_mode:
            print(f">>> Development mode enabled (DO NOT USE THIS IN PRODUCTION)")

        util.configure_logging()
        self._log = util.logger_for_object(self)
        self._log.info(f"TRAC Python Runtime {trac_version}")

        self._sys_config_path = sys_config_path
        self._sys_config_dir = pathlib.Path(self._sys_config_path).parent

        if job_config_path:
            self._job_config_path = job_config_path
            self._batch_mode = True
        else:
            self._job_config_path = None
            self._batch_mode = False

        self._sys_config: tp.Optional[config.SystemConfig] = None
        self._job_config: tp.Optional[config.JobConfig] = None

        self._dev_mode = dev_mode
        self._model_class = model_class

        # Top level resources
        self._repos: tp.Optional[repos.Repositories] = None
        self._storage: tp.Optional[storage.StorageManager] = None

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

        # Do not call self.stop()
        # In batch mode, the engine will stop itself once the job is complete, so no need to stop twice
        # It may be necessary to call self.stop() in service mode, depending how the stop is triggered (e.g. interrupt)

        pass

    def pre_start(self):

        try:

            self._log.info(f"Beginning pre-start sequence...")

            # Plugins will be loaded here, before config

            # self._log.info("Loading system config...")
            sys_config_parser = cfg.ConfigParser(config.SystemConfig)
            sys_config_raw = sys_config_parser.load_raw_config(self._sys_config_path, config_file_name="system")
            self._sys_config = sys_config_parser.parse(sys_config_raw, self._sys_config_path)

            if self._batch_mode:
                # self._log.info("Loading job config...")
                job_config_parser = cfg.ConfigParser(config.JobConfig)
                job_config_raw = job_config_parser.load_raw_config(self._job_config_path, config_file_name="job")
                self._job_config = job_config_parser.parse(job_config_raw, self._job_config_path)

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

            self._repos = repos.Repositories(self._sys_config)
            self._storage = storage.StorageManager(self._sys_config, self._sys_config_dir)

            self._engine = engine.TracEngine(self._sys_config, self._repos, self._storage, batch_mode=self._batch_mode)
            self._system = actors.ActorSystem(self._engine, system_thread="engine")

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

        self._system.send("submit_job", self._job_config)

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
