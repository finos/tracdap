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

import os
import pathlib
import shutil
import subprocess
import fileinput
import platform
import sys
import packaging.version
import argparse
import unittest


SCRIPT_DIR = pathlib.Path(__file__) \
    .parent \
    .resolve()

ROOT_PATH = SCRIPT_DIR \
    .parent.parent \
    .resolve()

BUILD_PATH = SCRIPT_DIR \
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

    BUILD_PATH.mkdir(parents=False, exist_ok=False)


def copy_source_files():

    for file in COPY_FILES:

        source_path = SCRIPT_DIR.joinpath(file)
        target_path = BUILD_PATH.joinpath(file)

        if source_path.is_dir():
            shutil.copytree(source_path, target_path)
        else:
            shutil.copy(source_path, target_path)


def copy_license():

    # Copy the license file out of the project root

    shutil.copy(
        SCRIPT_DIR.joinpath("../../LICENSE"),
        BUILD_PATH.joinpath("LICENSE"))


def generate_from_proto(unpacked: bool = False):

    if unpacked:
        generated_dir = SCRIPT_DIR.joinpath("generated")
        grpc_relocate = "tracdap:tracdap/rt_gen/grpc/tracdap"
    else:
        generated_dir = BUILD_PATH.joinpath("generated")
        grpc_relocate = "tracdap:tracdap/rt/_impl/grpc/tracdap"

    if generated_dir.exists():
        shutil.rmtree(generated_dir)

    generated_dir.mkdir(parents=True, exist_ok=False)

    protoc_ctrl = ROOT_PATH.joinpath("dev/codegen/protoc-ctrl.py")

    domain_cmd = [
        str(sys.executable), str(protoc_ctrl), "python_runtime",
        "--proto_path", "tracdap-api/tracdap-metadata/src/main/proto",
        "--proto_path", "tracdap-api/tracdap-config/src/main/proto",
        "--out", str(generated_dir) + "/tracdap/rt_gen/domain"]

    proto_cmd = [
        str(sys.executable), str(protoc_ctrl), "python_proto",
        "--proto_path", "tracdap-api/tracdap-metadata/src/main/proto",
        "--proto_path", "tracdap-api/tracdap-config/src/main/proto",
        "--proto_path", "tracdap-api/tracdap-services/src/main/proto",
        "--relocate", grpc_relocate,
        "--package", "tracdap.metadata",
        "--package", "tracdap.config",
        "--package", "tracdap.api.internal",
        "--out", str(generated_dir)]

    grpc_command = [
        str(sys.executable), str(protoc_ctrl), "python_grpc",
        "--proto_path", "tracdap-api/tracdap-metadata/src/main/proto",
        "--proto_path", "tracdap-api/tracdap-config/src/main/proto",
        "--proto_path", "tracdap-api/tracdap-services/src/main/proto",
        "--relocate", grpc_relocate,
        "--package", "tracdap.api.internal",
        "--out", str(generated_dir)]

    domain_proc = subprocess.Popen(domain_cmd, stdout=subprocess.PIPE, cwd=ROOT_PATH, env=os.environ)
    domain_out, domain_err = domain_proc.communicate()
    domain_result = domain_proc.wait()

    print(domain_out.decode("utf-8"))

    if domain_result != 0:
        raise subprocess.SubprocessError("Failed to generate domain classes from definitions")

    proto_proc = subprocess.Popen(proto_cmd, stdout=subprocess.PIPE, cwd=ROOT_PATH, env=os.environ)
    proto_out, proto_err = proto_proc.communicate()
    proto_result = proto_proc.wait()

    print(proto_out.decode("utf-8"))

    if proto_result != 0:
        raise subprocess.SubprocessError("Failed to generate proto classes from definitions")

    grpc_proc = subprocess.Popen(grpc_command, stdout=subprocess.PIPE, cwd=ROOT_PATH, env=os.environ)
    grpc_out, grpc_err = grpc_proc.communicate()
    grpc_result = grpc_proc.wait()

    print(grpc_out.decode("utf-8"))

    if grpc_result != 0:
        raise subprocess.SubprocessError("Failed to generate gRPC classes from definitions")


def move_generated_into_src():

    move_generated_package_into_src("src/tracdap/rt/metadata", "generated/tracdap/rt_gen/domain/tracdap/metadata")
    move_generated_package_into_src("src/tracdap/rt/config", "generated/tracdap/rt_gen/domain/tracdap/config")
    move_generated_package_into_src("src/tracdap/rt/_impl/grpc/tracdap", "generated/tracdap/rt/_impl/grpc/tracdap")

    # Update reference to gRPC generated classes in server.py

    for line in fileinput.input(BUILD_PATH.joinpath("src/tracdap/rt/_exec/server.py"), inplace=True):
        if "rt_gen" in line:
            print(line.replace("rt_gen.grpc", "rt._impl.grpc"), end="")
        else:
            print(line, end='')

    # Remove references to rt_gen package in setup.cfg, since everything is now in place under src/

    for line in fileinput.input(BUILD_PATH.joinpath("setup.cfg"), inplace=True):
        if "rt_gen" not in line:
            print(line, end='')


def move_generated_package_into_src(src_relative_path, generate_rel_path):

    # For generated packages, the main source tree contains placeholders that import everything
    # from the generated tree. We want to remove the placeholders and put the generated code into
    # the main source tree

    src_metadata_path = BUILD_PATH.joinpath(src_relative_path)
    generated_metadata_path = BUILD_PATH.joinpath(generate_rel_path)

    if src_metadata_path.exists():
        shutil.rmtree(src_metadata_path)

    shutil.copytree(generated_metadata_path, src_metadata_path)


def set_trac_version():

    if platform.system().lower().startswith("win"):
        command = ['powershell', '-ExecutionPolicy', 'Bypass', '-File', f'{ROOT_PATH}\\dev\\version.ps1']
    else:
        command = [f'{ROOT_PATH}/dev/version.sh']

    process = subprocess.Popen(command, stdout=subprocess.PIPE, cwd=ROOT_PATH)
    output, err = process.communicate()
    exit_code = process.wait()

    if exit_code != 0:
        raise subprocess.SubprocessError('Failed to get TRAC d.a.p. version')

    raw_version = output.decode('utf-8').strip()

    # Using Python's Version class normalises the version according to PEP440
    trac_version = packaging.version.Version(raw_version)

    # Set the version number used in the package metadata

    # setup.cfg uses file: and attr: for reading the version in from external sources
    # attr: doesn't work with namespace packages, __version__ has to be in the root package
    # file: works for the sdist build but is throwing an error for bdist_wheel, this could be a bug
    # Writing the version directly into setup.cfg avoids both of these issues

    for line in fileinput.input(BUILD_PATH.joinpath("setup.cfg"), inplace=True):
        if line.startswith("version ="):
            print(f"version = {str(trac_version)}")
        else:
            print(line, end="")

    # Set the version number embedded into the package

    embedded_version_file = BUILD_PATH.joinpath("src/tracdap/rt/_version.py")
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


def cli_args():

    parser = argparse.ArgumentParser(description='TRAC/Python Runtime Builder')

    parser.add_argument(
        "--target", type=str, metavar="target",
        choices=["codegen", "test", "examples", "integration", "dist"], nargs="*", required=True,
        help="The target to build")

    parser.add_argument(
        "--pattern", type=str, metavar="pattern",
        nargs="*", required=False,
        help="Patterns for matching integration tests")

    parser.add_argument(
        "--build-dir", type=pathlib.Path, metavar="build_dir", required=False,
        help="Specify a build location (defaults to ./build in same directory as this script)")

    return parser.parse_args()


def run_tests(test_path, pattern=None):

    # Default test pattern for unit tests
    if pattern is None:
        pattern = "test*.py"

    cwd = os.getcwd()
    python_path = [*sys.path]

    try:

        os.chdir(ROOT_PATH)
        sys.path.append(str(SCRIPT_DIR.joinpath("generated")))
        sys.path.append(str(SCRIPT_DIR.joinpath("src")))
        sys.path.append(str(SCRIPT_DIR.joinpath("test")))

        runner = unittest.TextTestRunner()
        loader = unittest.TestLoader()
        suite = loader.discover(
            start_dir=str(SCRIPT_DIR.joinpath(test_path)),
            top_level_dir=str(SCRIPT_DIR.joinpath("test")),
            pattern=pattern)

        result = runner.run(suite)

        if not result.wasSuccessful():
            exit(-1)

    finally:

        os.chdir(cwd)
        sys.path = python_path


def main():

    args = cli_args()

    # Update BUILD_PATH if it is specified on the command line
    global BUILD_PATH
    if args.build_dir is not None:
        BUILD_PATH = args.build_dir

    if "codegen" in args.target:
        generate_from_proto(unpacked=True)

    if "test" in args.target:
        run_tests("test/tracdap_test")

    if "examples" in args.target:
        run_tests("test/tracdap_examples")

    if "integration" in args.target:
        for pattern in args.pattern:
            run_tests("test/tracdap_test", pattern)

    if "dist" in args.target:

        reset_build_dir()
        copy_source_files()
        copy_license()
        set_trac_version()

        generate_from_proto()
        move_generated_into_src()

        run_pypa_build()


main()
