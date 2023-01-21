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

import tracdap.rt.config as _cfg
import tracdap.rt.ext._guard as _guard

# Dependency back into the main runtime implementation
import tracdap.rt._exec.runtime as _rt  # noqa


class __EmbeddedRuntime:

    def __init__(self, runtime: _rt.TracRuntime):
        _guard.run_model_guard("Using __EmbeddedRuntime")
        self.__runtime = runtime
        self.__destroyed = False

    def __enter__(self):
        _guard.run_model_guard("Using __EmbeddedRuntime")
        self.__runtime.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _guard.run_model_guard("Using __EmbeddedRuntime")
        self.__runtime.__exit__(exc_type, exc_val, exc_tb)
        self.__destroyed = True

    def __del__(self):
        _guard.run_model_guard("Using __EmbeddedRuntime")
        if not self.__destroyed:
            self.__runtime.stop()
            self.__destroyed = True

    def run_job_(self, job_config: _cfg.JobConfig):

        _guard.run_model_guard("Using __EmbeddedRuntime")

        self.__runtime.submit_job(job_config)

        return self.__runtime.wait_for_job(job_config.jobId)


def create_runtime(sys_config: _cfg.RuntimeConfig, dev_mode: bool = False) -> __EmbeddedRuntime:

    _guard.run_model_guard()

    runtime = _rt.TracRuntime(sys_config, dev_mode=dev_mode)
    runtime.pre_start()

    return __EmbeddedRuntime(runtime)


def run_job(runtime: __EmbeddedRuntime, job_config: _cfg.JobConfig) -> _cfg.JobResult:

    _guard.run_model_guard()

    return runtime.run_job_(job_config)
