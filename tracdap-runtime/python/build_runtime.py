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

import os
import pathlib
import re
import shutil
import subprocess
import fileinput
import platform
import sys
import packaging.version
import argparse
import unittest
import importlib.util


RUNTIME_DIR = pathlib.Path(__file__).parent.resolve()
RUNTIME_EXT_DIR = RUNTIME_DIR.parent.joinpath("python-ext").resolve()

ROOT_PATH = RUNTIME_DIR.parent.parent.resolve()
BUILD_PATH = ROOT_PATH.joinpath("build/python")
WORK_PATH = BUILD_PATH.joinpath("work")
DIST_PATH = BUILD_PATH.joinpath("dist")

COPY_PROJECT_FILES = {
    "README.md",
    "setup.cfg",
    "requirements.txt",
    "requirements_optional.txt"
}

TRAC_PYTHON_BUILD_ISOLATION = os.environ.get("TRAC_PYTHON_BUILD_ISOLATION") or "true"
BUILD_ISOLATION = TRAC_PYTHON_BUILD_ISOLATION.lower() in ["true", "yes", "1"]


def reset_build_dir():

    if BUILD_PATH.exists():
        shutil.rmtree(BUILD_PATH)

    BUILD_PATH.mkdir(parents=True, exist_ok=False)
    WORK_PATH.mkdir(parents=False, exist_ok=False)
    DIST_PATH.mkdir(parents=False, exist_ok=False)


def copy_project_files(plugin_path, plugin_package, project_root=RUNTIME_DIR):

    if project_root == RUNTIME_DIR:
        plugin_content_dir = project_root
    else:
        plugin_content_dir = project_root.joinpath(plugin_path)

    plugin_package_path = f"src/{plugin_package.replace('.', '/')}"
    plugin_package_dir = project_root.joinpath(plugin_package_path)

    project_work_dir = WORK_PATH.joinpath(plugin_path)
    project_work_dir.mkdir(parents=True, exist_ok=False)

    # Copy source code for the plugin's root package
    target_package_dir = project_work_dir.joinpath(plugin_package_path)
    shutil.copytree(plugin_package_dir, target_package_dir)

    # Copy other content files from the plugins content folder
    for file in COPY_PROJECT_FILES:
        source_path = plugin_content_dir.joinpath(file)
        target_path = project_work_dir.joinpath(file)
        if source_path.exists():
            shutil.copy(source_path, target_path)

    source_toml_file = project_root.joinpath("pyproject.toml")
    target_toml_file = project_work_dir.joinpath("pyproject.toml")
    shutil.copy(source_toml_file, target_toml_file)


def copy_license(project_work_dir="runtime"):

    # Copy the license file out of the project root

    source_license_file = ROOT_PATH.joinpath("LICENSE")
    target_license_file = WORK_PATH.joinpath(project_work_dir).joinpath("LICENSE")

    shutil.copy(source_license_file, target_license_file)


def generate_from_proto(runtime_path="runtime", unpacked: bool = False):

    work_dir = WORK_PATH.joinpath(runtime_path)

    if unpacked:
        generated_dir = RUNTIME_DIR.joinpath("generated")
        grpc_relocate = "tracdap:tracdap/rt_gen/grpc/tracdap"
    else:
        generated_dir = work_dir.joinpath("generated")
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
        "--proto_path", "tracdap-api/tracdap-services/src/main/proto",
        "--relocate", grpc_relocate,
        "--package", "tracdap.metadata",
        "--package", "tracdap.api.internal.runtime",
        "--out", str(generated_dir)]

    grpc_command = [
        str(sys.executable), str(protoc_ctrl), "python_grpc",
        "--proto_path", "tracdap-api/tracdap-metadata/src/main/proto",
        "--proto_path", "tracdap-api/tracdap-services/src/main/proto",
        "--relocate", grpc_relocate,
        "--package", "tracdap.api.internal.runtime",
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


def move_generated_into_src(runtime_path):

    work_dir = WORK_PATH.joinpath(runtime_path)

    move_generated_package_into_src(work_dir, "src/tracdap/rt/metadata", "generated/tracdap/rt_gen/domain/tracdap/metadata")
    move_generated_package_into_src(work_dir, "src/tracdap/rt/config", "generated/tracdap/rt_gen/domain/tracdap/config")
    move_generated_package_into_src(work_dir, "src/tracdap/rt/_impl/grpc/tracdap", "generated/tracdap/rt/_impl/grpc/tracdap")

    # Update reference to gRPC generated classes in server.py

    grpc_src_files = [
        "src/tracdap/rt/_impl/grpc/server.py",
        "src/tracdap/rt/_impl/grpc/codec.py"
    ]

    for src_file in grpc_src_files:
        for line in fileinput.input(work_dir.joinpath(src_file), inplace=True):
            if "rt_gen" in line:
                print(line.replace("rt_gen.grpc", "rt._impl.grpc"), end="")
            else:
                print(line, end='')

    # Remove references to rt_gen package in setup.cfg, since everything is now in place under src/

    for line in fileinput.input(work_dir.joinpath("setup.cfg"), inplace=True):
        if "rt_gen" not in line:
            print(line, end='')


def move_generated_package_into_src(work_dir, src_relative_path, generate_rel_path):

    # For generated packages, the main source tree contains placeholders that import everything
    # from the generated tree. We want to remove the placeholders and put the generated code into
    # the main source tree

    src_metadata_path = work_dir.joinpath(src_relative_path)
    generated_metadata_path = work_dir.joinpath(generate_rel_path)

    if src_metadata_path.exists():
        shutil.rmtree(src_metadata_path)

    shutil.copytree(generated_metadata_path, src_metadata_path)


def set_trac_version(project_path="runtime", project_packages=None):

    work_dir = WORK_PATH.joinpath(project_path)

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

    # Change -SNAPSHOT versions to something Python will accept
    if raw_version.endswith("-SNAPSHOT"):
        raw_version = raw_version.replace("-SNAPSHOT", "-dev")

    # Using Python's Version class normalises the version according to PEP440
    trac_version = packaging.version.Version(raw_version)

    # Set the version number embedded into the package
    # This can be done for multiple packages - e.g. tracdap.rt and tracdap.rt.api
    for project_package in project_packages or []:

        root_package_path = f"src/{project_package.replace('.', '/')}"
        version_file_path = work_dir.joinpath(root_package_path).joinpath("__init__.py")

        for line in fileinput.input(version_file_path, inplace=True):

            if "AUTOVERSION_INSERT" in line:
                print(f'__version__ = "{str(trac_version)}"')

            elif "AUTOVERSION_REMOVE" not in line and "__version__" not in line:
                print(line, end="")

    # Set the version number used in the package metadata

    # setup.cfg uses file: and attr: for reading the version in from external sources
    # attr: doesn't work with namespace packages, __version__ has to be in the root package
    # file: works for the sdist build but is throwing an error for bdist_wheel, this could be a bug
    # Writing the version directly into setup.cfg avoids both of these issues

    if work_dir.joinpath("setup.cfg").exists():
        for line in fileinput.input(work_dir.joinpath("setup.cfg"), inplace=True):
            if line.startswith("version ="):
                print(f"version = {str(trac_version)}")
            else:
                print(line, end="")

def update_toml_file(project_path="runtime", project_root=RUNTIME_DIR):

    # Load the settings file as a module
    settings_file = project_root.joinpath(project_path).joinpath("_settings.py")
    setting_spec = importlib.util.spec_from_file_location("_settings", settings_file)
    settings_module = importlib.util.module_from_spec(setting_spec)
    setting_spec.loader.exec_module(settings_module)

    settings = dict((k, v) for k, v in vars(settings_module).items() if not k.startswith('__'))

    # Substitute variables from the settings file into the project TOML file
    work_dir = WORK_PATH.joinpath(project_path)
    toml_file = work_dir.joinpath("pyproject.toml")
    variable = re.compile("\\$\\{(\\w+)}")

    with fileinput.FileInput(toml_file, inplace=True) as file:
        for line in file:
            match = variable.search(line)
            if match:
                variable_name = match.group(1)
                variable_value = settings.get(variable_name)
                if variable_value is None:
                    raise RuntimeError(f"Missing variable [{variable_name}] in _settings.py")
                else:
                    print(line.replace(match.group(0), variable_value), end='')
            else:
                print(line, end="")


def run_pypa_build(project_work_dir="runtime"):

    work_dir = WORK_PATH.joinpath(project_work_dir)

    build_exe = sys.executable
    build_args = ["python", "-m", "build", "--outdir", str(DIST_PATH)]

    if not BUILD_ISOLATION:
        build_args.append('--no-isolation')

    build_result = subprocess.run(executable=build_exe, args=build_args, cwd=work_dir)

    if build_result.returncode != 0:
        raise subprocess.SubprocessError(f"PyPA Build failed with exit code {build_result.returncode}")


def cli_args():

    parser = argparse.ArgumentParser(description='TRAC/Python Runtime Builder')

    parser.add_argument(
        "--target", type=str, metavar="target",
        choices=["codegen", "test", "examples", "integration", "dist", "ext", "clean"], nargs="*", required=True,
        help="The target to build")

    parser.add_argument(
        "--plugin", type=str, metavar="plugins", dest="plugins",
        nargs="*", required=False, default=[],
        help="Select the plugins to build (only applies if target = ext)")

    parser.add_argument(
        "--pattern", type=str, metavar="pattern",
        nargs="*", required=False,
        help="Patterns for matching integration tests")

    parser.add_argument(
        "--build-dir", type=pathlib.Path, metavar="build_dir", required=False,
        help="Specify a build location (defaults to ./build/python in the repo dir)")

    parser.add_argument(
        "--work-dir", type=pathlib.Path, metavar="work_dir", required=False,
        help="Specify a working location (defaults to ./build/python/work in the repo dir)")

    parser.add_argument(
        "--dist-dir", type=pathlib.Path, metavar="dist_dir", required=False,
        help="Specify a distribution location (defaults to ./build/python/dist in the repo dir)")

    return parser.parse_args()


def run_tests(test_path, pattern=None):

    # Default test pattern for unit tests
    if pattern is None:
        pattern = "test*.py"

    cwd = os.getcwd()
    python_path = [*sys.path]

    try:

        os.chdir(ROOT_PATH)
        sys.path.append(str(RUNTIME_DIR.joinpath("generated")))
        sys.path.append(str(RUNTIME_DIR.joinpath("src")))
        sys.path.append(str(RUNTIME_DIR.joinpath("test")))

        runner = unittest.TextTestRunner()
        loader = unittest.TestLoader()
        suite = loader.discover(
            start_dir=str(RUNTIME_DIR.joinpath(test_path)),
            top_level_dir=str(RUNTIME_DIR.joinpath("test")),
            pattern=pattern)

        result = runner.run(suite)

        if not result.wasSuccessful():
            exit(-1)

    finally:

        os.chdir(cwd)
        sys.path = python_path


def find_plugins(args) -> list[str]:

    plugins_dir = pathlib.Path(RUNTIME_EXT_DIR).joinpath("plugins")
    plugins = [plugin.name for plugin in plugins_dir.iterdir() if plugin.is_dir()]

    if args.plugins:
        selected_plugins = list(filter(lambda p: p in args.plugins, plugins))
        unknown_plugins = list(filter(lambda p: p not in plugins, args.plugins))
    else:
        selected_plugins = plugins
        unknown_plugins = []

    if any(unknown_plugins):
        raise RuntimeError(f"Unknown plugins: [{', '.join(unknown_plugins)}]")

    return selected_plugins


def build_runtime():

    runtime_path = "runtime"
    runtime_package = "tracdap.rt"
    api_package = "tracdap.rt.api"

    copy_project_files(runtime_path, runtime_package)
    copy_license(runtime_path)

    generate_from_proto(runtime_path)
    move_generated_into_src(runtime_path)

    set_trac_version(runtime_path, [runtime_package, api_package])

    run_pypa_build(runtime_path)


def build_plugin(plugin_name):

    plugin_path = f"plugins/{plugin_name}"
    plugin_package = f"tracdap.ext.{plugin_name}"

    copy_project_files(plugin_path, plugin_package, RUNTIME_EXT_DIR)
    copy_license(plugin_path)

    set_trac_version(plugin_path, [plugin_package])
    update_toml_file(plugin_path, RUNTIME_EXT_DIR)

    run_pypa_build(plugin_path)


def main():

    args = cli_args()

    # Update build paths if they are specified on the command line
    global BUILD_PATH, WORK_PATH, DIST_PATH

    if args.build_dir is not None:
        BUILD_PATH = args.build_dir.resolve()
        WORK_PATH = BUILD_PATH.joinpath("work")
        DIST_PATH = BUILD_PATH.joinpath("dist")

    if args.work_dir is not None:
        WORK_PATH = args.work_dir.resolve()

    if args.dist_dir is not None:
        DIST_PATH = args.dist_dir.resolve()

    if any(map(lambda target: target in args.target, ["clean", "dist", "ext"])):
        reset_build_dir()

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
        build_runtime()

    if "ext" in args.target:
        plugins = find_plugins(args)
        for plugin_name in plugins:
            build_plugin(plugin_name)


main()
