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


def _resolve_config_file(
        config_path: _tp.Union[str, pathlib.Path],
        model_dir: _tp.Optional[pathlib.Path] = None) \
        -> pathlib.Path:

    if pathlib.Path(config_path).is_absolute():
        return config_path

    cwd = pathlib.Path.cwd()
    cwd_config_path = cwd.joinpath(config_path).resolve()

    if cwd_config_path.exists():
        return cwd_config_path

    if model_dir is not None:

        model_config_path = model_dir.joinpath(config_path).resolve()

        if model_config_path.exists():
            return model_config_path

    if isinstance(config_path, pathlib.Path):
        return config_path
    else:
        return pathlib.Path(config_path)


def launch_model(
        model_class: api.TracModel.__class__,
        job_config: _tp.Union[str, pathlib.Path],
        sys_config: _tp.Union[str, pathlib.Path]):

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

    model_file = inspect.getfile(model_class)
    model_dir = pathlib.Path(model_file).parent

    _sys_config = _resolve_config_file(sys_config, model_dir)
    _job_config = _resolve_config_file(job_config, model_dir)

    runtime_instance = runtime.TracRuntime(
        _sys_config, _job_config,
        dev_mode=True,
        model_class=model_class)

    runtime_instance.pre_start()

    with runtime_instance as rt:
        rt.submit_batch()
        rt.wait_for_shutdown()
