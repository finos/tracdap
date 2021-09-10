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

import inspect
import pathlib
import typing as _tp

import trac.rt.api as api
import trac.rt.exec.runtime as runtime


def launch_cli():
    pass


def launch_session(sys_config: str):

    runtime_instance = runtime.TracRuntime(sys_config, dev_mode=True)

    with runtime_instance as rt:
        rt.wait_for_shutdown()


def launch_job(job_config: str, sys_config: str):

    runtime_instance = runtime.TracRuntime(sys_config, job_config, dev_mode=True)
    runtime_instance.pre_start()

    with runtime_instance as rt:
        rt.submit_batch()
        rt.wait_for_shutdown()


def launch_model(
        model_class: api.TracModel.__class__,
        job_config: _tp.Union[str, pathlib.Path],
        sys_config: _tp.Union[str, pathlib.Path]):

    model_file = inspect.getfile(model_class)
    model_dir = pathlib.Path(model_file).parent

    trac_system_dir = model_dir.joinpath(sys_config).parent.resolve()
    model_sys_cfg = model_dir.joinpath(sys_config).resolve()
    model_job_cfg = model_dir.joinpath(job_config).resolve()

    runtime_instance = runtime.TracRuntime(
        model_sys_cfg, model_job_cfg,
        trac_system_dir=trac_system_dir,
        dev_mode=True,
        model_class=model_class)

    runtime_instance.pre_start()

    with runtime_instance as rt:
        rt.submit_batch()
        rt.wait_for_shutdown()
