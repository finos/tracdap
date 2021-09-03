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
import fileinput
import platform
import sys
import packaging.version


ROOT_PATH = pathlib.Path(__file__) \
    .parent \
    .resolve()

BUILD_PATH = ROOT_PATH \
    .joinpath("build")

COPY_FILES = [
    "pyproject.toml",
    "setup.cfg",
    "README.md",
    "src"
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


def move_generated_into_src():

    # For generated packages, the main source tree contains placeholders that import everything
    # from the generated tree. We want to remove the placeholders and put the generated code into
    # the main source tree

    src_metadata_path = BUILD_PATH.joinpath("src/trac/rt/metadata")
    generated_metadata_path = ROOT_PATH.joinpath("generated/trac/rt_gen/domain/trac/metadata")

    shutil.rmtree(src_metadata_path)
    shutil.copytree(generated_metadata_path, src_metadata_path)

    # Remove references to rt_gen package in setup.cfg, since everything is now in place under src/

    for line in fileinput.input(BUILD_PATH.joinpath("setup.cfg"), inplace=True):
        if "rt_gen" not in line:
            print(line, end='')


def set_trac_version():

    if platform.system().lower().startswith("win"):
        command = ['powershell', '-ExecutionPolicy', 'Bypass', '-File', '..\\..\\dev\\version.ps1']
    else:
        command = ['../../dev/version.sh']

    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, err = process.communicate()
    exit_code = process.wait()

    if exit_code != 0:
        raise subprocess.SubprocessError('Failed to get TRAC version')

    raw_version = output.decode('utf-8').strip()

    # Using Python's Version class normalises the version according to PEP440
    trac_version = packaging.version.Version(raw_version)

    # Set the version number used in the package metadata

    # setup.cfg has file: and attr: for reading the version in from external sources
    # attr: doesn't work with namespace packages, __version__ has to be in the root package
    # file: works for the sdist build but is throwing an error for bdist_wheel, this could be a bug
    # Writing the version directly into setup.cfg avoids both of these issues

    for line in fileinput.input(BUILD_PATH.joinpath("setup.cfg"), inplace=True):
        if line.startswith("version ="):
            print(f"version = {str(trac_version)}")
        else:
            print(line, end="")

    # Set the version number embedded into the package

    embedded_version_file = BUILD_PATH.joinpath("src/trac/rt/_version.py")
    embedded_header_copied = False

    for line in fileinput.input(embedded_version_file, inplace=True):

        if line.isspace() and not embedded_header_copied:
            embedded_header_copied = True
            print("")
            print(f'__version__ = "{str(trac_version)}"')

        if not embedded_header_copied:
            print(line, end="")


def run_pypa_build():

    build_exe = sys.executable
    build_args = ["python", "-m", "build"]

    build_result = subprocess.run(executable=build_exe, args=build_args, cwd=BUILD_PATH)

    if build_result.returncode != 0:
        raise subprocess.SubprocessError(f"PyPA Build failed with exit code {build_result.returncode}")


def main():

    reset_build_dir()
    copy_source_files()

    move_generated_into_src()
    set_trac_version()

    run_pypa_build()


main()
