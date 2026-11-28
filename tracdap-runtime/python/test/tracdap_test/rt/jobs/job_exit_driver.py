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
Run one job as its own process, for the job exit tests

    python job_exit_driver.py <sys_config> <job_config> [--hold-progress-write]

With --hold-progress-write, each snapshot write opens the file, writes it and then holds it open, so the process
exits while a write is still in progress.
"""

import sys
import threading

import tracdap.rt.launch.launch as launch  # noqa
import tracdap.rt._impl.core.storage as storage  # noqa


PROGRESS_FILE_NAME = "trac_job_progress.json"
HELD_WRITE_MESSAGE = "Progress write held open"


def hold_progress_writes():

    write_bytes = storage.CommonFileStorage.write_bytes

    def held_write(file_storage, storage_path: str, data: bytes):

        if not storage_path.endswith(PROGRESS_FILE_NAME):
            return write_bytes(file_storage, storage_path, data)

        with file_storage.write_byte_stream(storage_path) as stream:
            stream.write(data)
            print(HELD_WRITE_MESSAGE, flush=True)
            threading.Event().wait()

    storage.CommonFileStorage.write_bytes = held_write


def main(args):

    if "--hold-progress-write" in args:
        hold_progress_writes()

    launch.launch_cli(["--sys-config", args[0], "--job-config", args[1], "--dev-mode"])


if __name__ == "__main__":
    main(sys.argv[1:])
