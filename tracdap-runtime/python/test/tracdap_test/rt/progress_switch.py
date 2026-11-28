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

"""
Run jobs with progress on, whichever entry path submits them

Every job submitted inside progress_enabled() gets result.progress.path, and a result storage location if it has
none, so the launch_* functions and dev mode run with progress too. Results and snapshots go to a temporary folder.
"""

import contextlib
import pathlib
import tempfile
import typing as tp
import unittest.mock as mock

import tracdap.rt.config as cfg
import tracdap.rt.metadata as meta
import tracdap.rt._impl.runtime as runtime  # noqa
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.core.util as util  # noqa


PROGRESS_STORAGE_KEY = "trac_progress_switch"
PROGRESS_FILE_NAME = "trac_job_progress.json"

_ORIGINAL_SUBMIT = runtime.TracRuntime.submit_job


@contextlib.contextmanager
def progress_enabled(storage_dir: tp.Optional[pathlib.Path] = None) -> tp.Iterator[pathlib.Path]:

    with contextlib.ExitStack() as stack:

        if storage_dir is None:
            storage_dir = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        original_start = runtime.TracRuntime.start

        def start(rt: runtime.TracRuntime, wait: bool = False):

            if not rt._pre_start_complete:  # noqa
                rt.pre_start()

            rt._sys_config.resources[PROGRESS_STORAGE_KEY] = meta.ResourceDefinition(  # noqa
                resourceType=meta.ResourceType.INTERNAL_STORAGE, protocol="LOCAL",
                properties={"rootPath": str(storage_dir)})

            return original_start(rt, wait)

        def submit_job(rt: runtime.TracRuntime, job_config: cfg.JobConfig):

            properties = job_config.properties
            job_key = util.object_key(job_config.jobId)

            if cfg_p.ConfigKeys.RESULT_PROGRESS_PATH not in properties:

                if cfg_p.ConfigKeys.RESULT_STORAGE_LOCATION not in properties:
                    properties[cfg_p.ConfigKeys.RESULT_STORAGE_LOCATION] = PROGRESS_STORAGE_KEY
                    properties[cfg_p.ConfigKeys.RESULT_STORAGE_PATH] = f"{job_key}/trac_job_result.json"

                properties[cfg_p.ConfigKeys.RESULT_PROGRESS_PATH] = f"{job_key}/{PROGRESS_FILE_NAME}"

            return _ORIGINAL_SUBMIT(rt, job_config)

        stack.enter_context(mock.patch.object(runtime.TracRuntime, "start", start))
        stack.enter_context(mock.patch.object(runtime.TracRuntime, "submit_job", submit_job))

        yield storage_dir


@contextlib.contextmanager
def progress_disabled() -> tp.Iterator[None]:

    """Submit jobs unchanged, also inside progress_enabled()"""

    with mock.patch.object(runtime.TracRuntime, "submit_job", _ORIGINAL_SUBMIT):
        yield
