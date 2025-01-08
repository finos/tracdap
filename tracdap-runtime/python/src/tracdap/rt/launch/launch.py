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

import inspect as _inspect
import pathlib as _pathlib
import typing as _tp

import tracdap.rt.api as _api
import tracdap.rt._impl.core.config_parser as _cparse  # noqa
import tracdap.rt._impl.core.util as _util  # noqa
import tracdap.rt._impl.runtime as _runtime  # noqa

from .cli import _cli_args


def _resolve_config_file(
        config_path: _tp.Union[str, _pathlib.Path],
        model_dir: _tp.Optional[_pathlib.Path] = None) \
        -> _tp.Union[_pathlib.Path, str]:

    # If the config path is a URL, do not convert it into a path
    if isinstance(config_path, str):
        scheme_sep = config_path.find(":")
        # Single letter scheme is a Windows file path (C:\...)
        scheme = scheme = config_path[:scheme_sep] if scheme_sep > 1 else "file"
        if scheme != "file":
            return config_path

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


def _optional_arg(launch_args: _tp.Dict[str, _tp.Any], arg_name: str) -> _tp.Any:

    return launch_args.get(arg_name, None)


def launch_model(
        model_class: _api.TracModel.__class__,
        job_config: _tp.Union[_pathlib.Path, str],
        sys_config: _tp.Union[_pathlib.Path, str],
        **launch_args):

    """
    Launch an individual model using its Python class

    This function launches the supplied model class directly, it must be called
    from the Python codebase containing the model class. The TRAC runtime will launch
    in dev mode and execute the model inside the current Python process, a minimal
    job definition and set of local resources will be configured automatically.
    This method is useful for launching models during local development
    for debugging and testing.

    To resolve the paths of the job and system config files, paths are tried in the
    following order:

    1. If an absolute path is supplied, this takes priority
    2. Resolve relative to the current working directory
    3. Resolve relative to the directory containing the Python module of the model

    :param model_class: The model class that will be launched
    :param job_config: Path to the job configuration file
    :param sys_config: Path to the system configuration file
    :param launch_args: Additional arguments to control behavior of the TRAC runtime (not normally required)

    :type model_class: :py:class:`TracModel.__class__ <tracdap.rt.api.TracModel>`
    :type job_config: :py:class:`pathlib.Path` | str
    :type sys_config: :py:class:`pathlib.Path` | str
    """

    model_file = _inspect.getfile(model_class)
    model_dir = _pathlib.Path(model_file).parent

    _sys_config = _resolve_config_file(sys_config, model_dir)
    _job_config = _resolve_config_file(job_config, model_dir)

    plugin_package = _optional_arg(launch_args, 'plugin_package')
    plugin_packages = [plugin_package] if plugin_package else None

    runtime_instance = _runtime.TracRuntime(_sys_config, dev_mode=True, plugin_packages=plugin_packages)
    runtime_instance.pre_start()

    with runtime_instance as rt:

        job = rt.load_job_config(_job_config, model_class=model_class)

        rt.submit_job(job)
        rt.wait_for_job(job.jobId)


def launch_job(
        job_config: _tp.Union[_pathlib.Path, str],
        sys_config: _tp.Union[_pathlib.Path, str],
        dev_mode: bool = False,
        **launch_args):

    """
    Launch a TRAC job using external configuration files

    This function launches the job definition supplied in the job_config file,
    which must contain enough information to describe the job along with any
    models and other resources that it needs. It allows for running more complex
    job types such as :py:class:`JobType.RUN_FLOW <tracdap.rt.metadata.JobType>`
    and can be used for local development by setting dev_mode = True. If the job
    depends on external resources, those must be specified in the sys_config file.

    To resolve the paths of the job and system config files, paths are tried in the
    following order:

    1. If an absolute path is supplied, this takes priority
    2. Resolve relative to the current working directory

    :param job_config: Path to the job configuration file
    :param sys_config: Path to the system configuration file
    :param dev_mode: Whether to launch in dev mode (applies dev mode translation to the job inputs)
    :param launch_args: Additional arguments to control behavior of the TRAC runtime (not normally required)

    :type job_config: :py:class:`pathlib.Path` | str
    :type sys_config: :py:class:`pathlib.Path` | str
    :type dev_mode: bool
    """

    _sys_config = _resolve_config_file(sys_config, None)
    _job_config = _resolve_config_file(job_config, None)

    plugin_package = _optional_arg(launch_args, 'plugin_package')
    plugin_packages = [plugin_package] if plugin_package else None

    runtime_instance = _runtime.TracRuntime(_sys_config, dev_mode=dev_mode, plugin_packages=plugin_packages)
    runtime_instance.pre_start()

    with runtime_instance as rt:

        job = rt.load_job_config(_job_config)

        rt.submit_job(job)
        rt.wait_for_job(job.jobId)


def launch_cli(programmatic_args: _tp.Optional[_tp.List[str]] = None):

    """
    Launch the TRAC runtime using the command line interface

    CLI arguments are read from the process command line by default. To pass CLI args
    explicitly, provide the list of arguments using the programmatic_args parameter.

    :param programmatic_args: Optional parameter to pass CLI args explicitly in code
    """

    if programmatic_args:
        launch_args = _cli_args(programmatic_args)
    else:
        launch_args = _cli_args()

    _sys_config = _resolve_config_file(launch_args.sys_config, None)
    _job_config = _resolve_config_file(launch_args.job_config, None) if launch_args.job_config else None

    runtime_instance = _runtime.TracRuntime(
        _sys_config,
        dev_mode=launch_args.dev_mode,
        job_result_dir=launch_args.job_result_dir,
        job_result_format=launch_args.job_result_format,
        scratch_dir=launch_args.scratch_dir,
        scratch_dir_persist=launch_args.scratch_dir_persist,
        plugin_packages=launch_args.plugin_packages)

    runtime_instance.pre_start()

    with runtime_instance as rt:

        if _job_config is not None:
            job = rt.load_job_config(_job_config)
            rt.submit_job(job)

        if rt.is_oneshot():
            rt.wait_for_job(job.jobId)
        else:
            rt.run_until_done()
