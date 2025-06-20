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

import argparse
import pathlib


def _cli_args(programmatic_args = None):

    parser = argparse.ArgumentParser(
        prog="python -m tracdap.rt.launch",
        description="TRAC D.A.P. Runtime for Python")

    parser.add_argument(
        "--sys-config", dest="sys_config", type=str, required=True,
        help="Path to the system configuration file for the TRAC runtime")

    parser.add_argument(
        "--job-config", dest="job_config", type=str, required=True,
        help="Path to the job configuration for the job to be executed")

    parser.add_argument(
        "--dev-mode", dest="dev_mode", default=False, action="store_true",
        help="Enable development mode config translation")

    parser.add_argument(
        "--scratch-dir", dest="scratch_dir", type=pathlib.Path, required=False,
        help="Scratch directory for working files" +
             " (if not supplied the system's temp location will be used)")

    parser.add_argument(
        "--scratch-dir-persist", dest="scratch_dir_persist", default=False, action="store_true",
        help="Do not clean up the scratch location on exit")

    parser.add_argument(
        "--plugin-package", dest="plugin_packages", type=str, action="append",
        help="Do not clean up the scratch location on exit")

    if programmatic_args:
        return parser.parse_args(programmatic_args)
    else:
        return parser.parse_args()
