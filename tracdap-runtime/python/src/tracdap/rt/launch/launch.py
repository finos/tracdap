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

from __future__ import annotations

import inspect as _inspect
import pathlib as _pathlib
import typing as _tp

import tracdap.rt.api as _api
import tracdap.rt._impl.config_parser as _cparse  # noqa
import tracdap.rt._impl.util as _util  # noqa
import tracdap.rt._exec.runtime as _runtime  # noqa

from .cli import cli_args


def _resolve_config_file(
        config_path: _tp.Union[str, _pathlib.Path],
        model_dir: _tp.Optional[_pathlib.Path] = None) \
        -> _pathlib.Path:

    if _pathlib.Path(config_path).is_absolute():
        return config_path

    cwd = _pathlib.Path.cwd()
    cwd_config_path = cwd.joinpath(config_path).resolve()

    if cwd_config_path.exists():
        return cwd_config_path

    if model_dir is not None:

        model_config_path = model_dir.joinpath(config_path).resolve()

        if model_config_path.exists():
            return model_config_path

    if isinstance(config_path, _pathlib.Path):
        return config_path
    else:
        return _pathlib.Path(config_path)


def launch_model(
        model_class: _api.TracModel.__class__,
        job_config: _tp.Union[str, _pathlib.Path],
        sys_config: _tp.Union[str, _pathlib.Path]):

    """
    Launch an individual model component by class (embedded launch)

    This launch method launches the supplied model class directly, it must be called
    from the Python codebase containing the model class. The TRAC runtime will launch
    within the current Python process, job target and model repositories are configured
    automatically and dev mode will be enabled. This method is mainly useful for launching
    development and debugging runs.

    To resolve the paths of the job and system config files, paths are tried in the
    following order:

    1. If an absolute path is supplied, this takes priority
    2. Resolve relative to the current working directory
    3. Resolve relative to the directory containing the Python module of the model

    :param model_class: The model class that will be launched
    :param job_config: Path to the job configuration file
    :param sys_config: Path to the system configuration file
    """

    model_file = _inspect.getfile(model_class)
    model_dir = _pathlib.Path(model_file).parent

    _sys_config = _resolve_config_file(sys_config, model_dir)
    _job_config = _resolve_config_file(job_config, model_dir)

    runtime_instance = _runtime.TracRuntime(_sys_config, dev_mode=True)
    runtime_instance.pre_start()

    job = runtime_instance.load_job_config(_job_config, model_class=model_class)

    with runtime_instance as rt:
        rt.submit_job(job)
        rt.wait_for_job(job.jobId)


def launch_job(
        job_config: _tp.Union[str, _pathlib.Path],
        sys_config: _tp.Union[str, _pathlib.Path],
        dev_mode: bool = False):

    _sys_config = _resolve_config_file(sys_config, None)
    _job_config = _resolve_config_file(job_config, None)

    runtime_instance = _runtime.TracRuntime(_sys_config, dev_mode=dev_mode)
    runtime_instance.pre_start()

    job = runtime_instance.load_job_config(_job_config)

    with runtime_instance as rt:
        rt.submit_job(job)
        rt.wait_for_job(job.jobId)


def launch_cli():

    launch_args = cli_args()

    _sys_config = _resolve_config_file(launch_args.sys_config, None)
    _job_config = _resolve_config_file(launch_args.job_config, None)

    runtime_instance = _runtime.TracRuntime(
        _sys_config,
        dev_mode=launch_args.dev_mode,
        job_result_dir=launch_args.job_result_dir,
        job_result_format=launch_args.job_result_format,
        scratch_dir=launch_args.scratch_dir,
        scratch_dir_persist=launch_args.scratch_dir_persist)

    runtime_instance.pre_start()

    job = runtime_instance.load_job_config(_job_config)

    with runtime_instance as rt:
        rt.submit_job(job)
        rt.wait_for_job(job.jobId)
