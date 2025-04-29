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

from __future__ import annotations

import dataclasses as dc
import datetime as dt
import signal
import threading

import sys
import pathlib
import tempfile
import typing as tp

import tracdap.rt.api as _api
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.plugins as _plugins
import tracdap.rt._impl.core.config_parser as _cparse
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.guard_rails as _guard
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.models as _models
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.util as _util
import tracdap.rt._impl.exec.actors as _actors
import tracdap.rt._impl.exec.engine as _engine
import tracdap.rt._impl.exec.dev_mode as _dev_mode
import tracdap.rt._impl.static_api as _static_api
import tracdap.rt._version as _version


@dc.dataclass
class _RuntimeJobInfo:
    done: bool = dc.field(default=False)
    result: _cfg.JobResult = dc.field(default=None)
    error: Exception = dc.field(default=None)


class TracRuntime:

    # Basic configuration of engine threads
    __THREAD_POOL_CONFIG = {"model": 1, "data": 3}
    __THREAD_POOL_MAPPING = {
        _engine.ModelNodeProcessor: "model",
        _engine.DataNodeProcessor: "data"}

    __DEFAULT_API_PORT = 9000

    def __init__(
            self,
            sys_config: tp.Union[str, pathlib.Path, _cfg.RuntimeConfig],
            job_result_dir: tp.Union[str, pathlib.Path, None] = None,
            job_result_format: tp.Optional[str] = None,
            scratch_dir: tp.Union[str, pathlib.Path, None] = None,
            scratch_dir_persist: bool = False,
            plugin_packages: tp.List[str] = None,
            dev_mode: bool = False):

        trac_version = _version.__version__
        python_version = sys.version.replace("\n", "")

        sys_config_path = "[embedded]" if isinstance(sys_config, _cfg.RuntimeConfig) else sys_config

        print(f">>> TRAC D.A.P. Python Runtime {trac_version} starting at {dt.datetime.now()}")
        print(f">>> Python installation: {python_version} ({sys.exec_prefix})")
        print(f">>> System config: {sys_config_path}")

        if dev_mode:
            print(f">>> Development mode enabled (DO NOT USE THIS IN PRODUCTION)")

        if isinstance(scratch_dir, str):
            scratch_dir = pathlib.Path(scratch_dir)

        _logging.configure_logging()
        self._log = _logging.logger_for_object(self)
        self._log.info(f"TRAC D.A.P. Python Runtime {trac_version}")

        self._sys_config = sys_config if isinstance(sys_config, _cfg.RuntimeConfig) else None
        self._sys_config_path = sys_config if not self._sys_config else None
        self._job_result_dir = job_result_dir
        self._job_result_format = job_result_format
        self._scratch_dir = scratch_dir
        self._scratch_dir_provided = True if scratch_dir is not None else False
        self._scratch_dir_persist = scratch_dir_persist
        self._plugin_packages = plugin_packages or []
        self._dev_mode = dev_mode
        self._dev_mode_translator = None

        # Runtime control
        self._runtime_lock = threading.Lock()
        self._runtime_event = threading.Condition(self._runtime_lock)
        self._pre_start_complete = False
        self._shutdown_requested = False
        self._oneshot_job = None

        # Top level resources
        self._config_mgr: tp.Optional[_cparse.ConfigManager] = None
        self._models: tp.Optional[_models.ModelLoader] = None
        self._storage: tp.Optional[_storage.StorageManager] = None

        # The execution engine
        self._system: tp.Optional[_actors.ActorSystem] = None
        self._engine: tp.Optional[_engine.TracEngine] = None

        # Runtime API server
        self._server_enabled = False
        self._server_port = 0
        self._server = None

        self._jobs: tp.Dict[str, _RuntimeJobInfo] = dict()

    # ------------------------------------------------------------------------------------------------------------------
    # Runtime service control
    # ------------------------------------------------------------------------------------------------------------------

    def __enter__(self):
        self.start(wait=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_type is not None:
            self.stop(due_to_error=True)
        else:
            self.stop()

    def pre_start(self):

        try:

            self._log.info(f"Beginning pre-start sequence...")

            # Plugin manager, static API and guard rails are singletons
            # Calling these methods multiple times is safe (e.g. for embedded or testing scenarios)
            # However, plugins are never un-registered for the lifetime of the processes

            _plugins.PluginManager.register_core_plugins()

            for plugin_package in self._plugin_packages:
                _plugins.PluginManager.register_plugin_package(plugin_package)

            _static_api.StaticApiImpl.register_impl()

            # Load sys config (or use embedded), config errors are detected before start()
            # Job config can also be checked before start() by using load_job_config()

            self._config_mgr = _cparse.ConfigManager.for_root_config(self._sys_config_path)

            if self._sys_config is None:
                sys_config_dev_mode = _dev_mode.DEV_MODE_SYS_CONFIG if self._dev_mode else None
                self._sys_config = self._config_mgr.load_root_object(
                    _cfg.RuntimeConfig, sys_config_dev_mode,
                    config_file_name="system")
            else:
                self._log.info("Using embedded system config")

            # Check whether to enable categorical processing in the data layer

            if _data.DataMapping.CATEGORICAL_CONFIG_KEY in self._sys_config.properties:
                categorical_prop = self._sys_config.properties[_data.DataMapping.CATEGORICAL_CONFIG_KEY]
                categorical_flag = categorical_prop and categorical_prop.lower() == "true"
                _data.DataMapping.enable_categorical(categorical_flag)

            # Dev mode translation is controlled by the dev mode flag
            # I.e. it can be applied to embedded configs

            if self._dev_mode:
                self._sys_config = _dev_mode.DevModeTranslator.translate_sys_config(self._sys_config, self._config_mgr)

            # Runtime API server is controlled by the sys config

            if self._sys_config.runtimeApi is not None:
                api_config = self._sys_config.runtimeApi
                if api_config.enabled:
                    self._server_enabled = True
                    self._server_port = api_config.port or self.__DEFAULT_API_PORT

            self._pre_start_complete = True

        except Exception as e:
            self._handle_startup_error(e)

    def start(self, wait: bool = False):

        try:

            # Ensure pre-start has been run
            if not self._pre_start_complete:
                self.pre_start()

            self._log.info("Starting the engine...")

            self._prepare_scratch_dir()

            self._models = _models.ModelLoader(self._sys_config, self._scratch_dir)
            self._storage = _storage.StorageManager(self._sys_config)

            if self._dev_mode:

                self._dev_mode_translator = _dev_mode.DevModeTranslator(
                    self._sys_config, self._config_mgr, self._scratch_dir,
                    model_loader=self._models, storage_manager=self._storage)

            # Enable protection after the initial setup of the runtime is complete
            # Storage plugins in particular are likely to tigger protected imports
            # Once the runtime is up, no more plugins should be loaded
            _guard.PythonGuardRails.protect_dangerous_functions()

            self._engine = _engine.TracEngine(
                self._sys_config, self._models, self._storage,
                notify_callback=self._engine_callback)

            self._system = _actors.ActorSystem(
                self._engine, system_thread="engine",
                thread_pools=self.__THREAD_POOL_CONFIG,
                thread_pool_mapping=self.__THREAD_POOL_MAPPING)

            self._system.start(wait=wait)

            # If the runtime server has been enabled, start it up
            if self._server_enabled:

                self._log.info("Starting the runtime API server...")

                # The server module pulls in all the gRPC dependencies, don't import it unless we have to
                import tracdap.rt._impl.grpc.server as _server

                self._server = _server.RuntimeApiServer(self._system, self._server_port)
                self._server.start()

        except Exception as e:
            self._handle_startup_error(e)

    def stop(self, due_to_error=False):

        if self._server is not None:
            self._log.info("Stopping the runtime API server...")
            self._server.stop()

        if due_to_error:
            self._log.info("Shutting down the engine in response to an error")
        else:
            self._log.info("Shutting down the engine")

        if not self._system:
            self._log.warning("TRAC runtime engine was never started")
            self._clean_scratch_dir()
            return

        self._system.stop()
        self._system.wait_for_shutdown()
        self._clean_scratch_dir()

        # If there are errors in the engine code or actor system, this is the most serious failure
        # Report these errors as the highest priority
        if self._system.shutdown_code() != 0:

            self._log.error("TRAC runtime has gone down with errors")
            raise self._system.shutdown_error()

        # If the engine went down cleanly, but in response to an error, this still counts as a failure
        # This will normally be because of errors in a job
        elif due_to_error:
            self._log.error("TRAC runtime has gone down, previous errors will be propagated")

        else:
            self._log.info("TRAC runtime has gone down cleanly")

    def is_oneshot(self):
        return not self._server_enabled

    def run_until_done(self):

        if self._server_enabled == False and len(self._jobs) == 0:
            self._log.error("No job config supplied, TRAC runtime will not run")
            raise _ex.EStartup("No job config supplied")

        signal.signal(signal.SIGTERM, self._request_shutdown)
        signal.signal(signal.SIGINT, self._request_shutdown)

        with self._runtime_lock:
            while not self._shutdown_requested:
                self._runtime_event.wait()

    def _request_shutdown(self, _signum = None, _frame = None):

        with self._runtime_lock:
            self._shutdown_requested = True
            self._runtime_event.notify()

    def _prepare_scratch_dir(self):

        if not self._scratch_dir_provided:
            scratch_dir = tempfile.mkdtemp(prefix="tracdap_scratch_")
            self._scratch_dir = pathlib.Path(scratch_dir)

        self._log.info(f"Using scratch directory [{self._scratch_dir}]")

        if not self._scratch_dir.exists():
            raise _ex.EStartup(f"Scratch directory [{self._scratch_dir}] does not exist")

        if not self._scratch_dir.is_dir() or any(self._scratch_dir.iterdir()):
            raise _ex.EStartup(f"Scratch directory [{self._scratch_dir}] is not an empty directory")

        try:
            write_test_path = self._scratch_dir.joinpath("trac_write_test.dat")
            with open(write_test_path, "wb") as write_test:
                write_test.write(bytes([1, 2, 3]))
            write_test_path.unlink()
        except Exception:
            raise _ex.EStartup(f"Scratch directory [{self._scratch_dir}] is not writable")

    def _clean_scratch_dir(self):

        if not self._scratch_dir_persist:
            _util.try_clean_dir(self._scratch_dir, remove=(not self._scratch_dir_provided))

    # ------------------------------------------------------------------------------------------------------------------
    # Job submission
    # ------------------------------------------------------------------------------------------------------------------

    def load_job_config(
            self, job_config: tp.Union[str, pathlib.Path, _cfg.JobConfig],
            model_class: tp.Optional[_api.TracModel.__class__] = None):

        if not self._engine or self._shutdown_requested:
            raise _ex.ETracInternal("Engine is not started or shutdown has been requested")

        if isinstance(job_config, _cfg.JobConfig):
            self._log.info("Using embedded job config")

        else:
            job_config_dev_mode = _dev_mode.DEV_MODE_JOB_CONFIG if self._dev_mode else None
            job_config = self._config_mgr.load_config_object(
                job_config, _cfg.JobConfig,
                job_config_dev_mode,
                config_file_name="job")

        if self._dev_mode:
            job_config = self._dev_mode_translator.translate_job_config(job_config, model_class)

        return job_config

    def submit_job(self, job_config: _cfg.JobConfig):

        if not self._engine or self._shutdown_requested:
            raise _ex.ETracInternal("Engine is not started or shutdown has been requested")

        job_key = _util.object_key(job_config.jobId)
        self._jobs[job_key] = _RuntimeJobInfo()

        self._system.send_main(
            "submit_job", job_config,
            str(self._job_result_dir) if self._job_result_dir else "",
            self._job_result_format if self._job_result_format else "")

    def wait_for_job(self, job_id: _api.TagHeader):

        if not self._engine or self._shutdown_requested:
            raise _ex.ETracInternal("Engine is not started or shutdown has been requested")

        job_key = _util.object_key(job_id)

        if job_key not in self._jobs:
            raise _ex.ETracInternal(f"Attempt to wait for a job that was never started")

        self._oneshot_job = job_key

        self.run_until_done()

        job_info = self._jobs[job_key]

        if job_info.error is not None:
            raise job_info.error

        elif job_info.result is not None:
            return job_info.result

        else:
            err = f"No result or error information is available for job [{job_key}]"
            self._log.error(err)
            raise _ex.ETracInternal(err)

    def _engine_callback(self, job_key, job_result, job_error):

        if job_result is not None:
            self._jobs[job_key].done = True
            self._jobs[job_key].result = job_result
        elif job_error is not None:
            self._jobs[job_key].done = True
            self._jobs[job_key].error = job_error

        if self._oneshot_job == job_key:
            self._request_shutdown()

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
