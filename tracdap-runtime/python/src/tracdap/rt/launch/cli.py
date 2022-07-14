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

import argparse
import pathlib


def cli_args():

    parser = argparse.ArgumentParser(
        prog="python -m tracdap.rt.launch",
        description="TRAC D.A.P. Runtime for Python")

    parser.add_argument(
        "--sys-config", dest="sys_config", type=pathlib.Path, required=True,
        help="Path to the system configuration file for the TRAC runtime")

    parser.add_argument(
        "--job-config", dest="job_config", type=pathlib.Path, required=True,
        help="Path to the job configuration for the job to be executed")

    parser.add_argument(
        "--dev-mode", dest="dev_mode", default=False, action="store_true",
        help="Enable development mode config translation")

    parser.add_argument(
        "--job-result-dir", dest="job_result_dir", type=pathlib.Path, required=False,
        help="Output the result metadata for a batch job to the given directory")

    parser.add_argument(
        "--job-result-format", dest="job_result_format", choices=["json", "yaml", "proto"], default="json",
        help="Output format for the result metadata (only meaningful if --job-result-dir is set)")

    parser.add_argument(
        "--scratch-dir", dest="scratch_dir", type=pathlib.Path, required=False,
        help="Scratch directory for working files" +
             " (if not supplied the system's temp location will be used)"
    )

    parser.add_argument(
        "--scratch-dir-persist", dest="scratch_dir_persist", default=False, action="store_true",
        help="Do not clean up the scratch location on exit"
    )

    return parser.parse_args()
