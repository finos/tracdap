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

import pathlib
import shutil
import subprocess
import sys

ROOT_PATH = pathlib.Path(__file__) \
    .parent \
    .resolve()

BUILD_PATH = ROOT_PATH \
    .joinpath("build")

COPY_FILES = [
    "pyproject.toml",
    "setup.cfg",
    "README.md",
    "src",
    "generated"
]


def reset_build_dir():

    if BUILD_PATH.exists():
        shutil.rmtree(BUILD_PATH)

    BUILD_PATH.mkdir(parents=True, exist_ok=False)


def copy_source_files():

    for file in COPY_FILES:

        source_path = ROOT_PATH.joinpath(file)
        target_path = BUILD_PATH.joinpath(file)

        if source_path.is_dir():
            shutil.copytree(source_path, target_path)
        else:
            shutil.copy(source_path, target_path)


def run_pypa_build():

    build_exe = sys.executable
    build_args = ["python", "-m", "build"]

    build_result = subprocess.run(executable=build_exe, args=build_args, cwd=BUILD_PATH)


def main():

    reset_build_dir()
    copy_source_files()
    run_pypa_build()


main()
