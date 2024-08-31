#  Copyright 2020 Accenture Global Solutions Limited
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

import fileinput
import os
import pathlib
import platform
import re
import shutil
import subprocess as sp
import argparse
import logging
import sys
import tempfile

import protoc
import google.api.http_pb2 as gapi_http_module


SCRIPT_NAME = pathlib.Path(__file__).stem

SCRIPT_DIR = pathlib.Path(__file__) \
    .parent \
    .absolute() \
    .resolve()

ROOT_DIR = SCRIPT_DIR \
    .joinpath("../..") \
    .resolve()


# Configure logging
logging_format = f"%(levelname)s %(name)s: %(message)s"
logging.basicConfig(format=logging_format, level=logging.INFO)
_log = logging.getLogger(SCRIPT_NAME)


PUBLIC_API_EXCLUSIONS = [
    re.compile(r".*[/\\]internal$"),
    re.compile(r".*_trusted\.proto$")]


def is_public_api(path: pathlib.Path):

    return not any(map(lambda excl: excl.match(str(path)), PUBLIC_API_EXCLUSIONS))

def is_in_packages(path: pathlib.Path, packages):

    unix_like_path = str(path).replace(os.sep, "/")

    return any(map(lambda pkg: pkg in unix_like_path, packages))


def _copytree(src, dst):

    _log.info(f"Copying {src} -> {dst}")

    # In shutil.copytree, dir_exists_ok is only available from Python 3.8, but we need Python 3.7
    # Codegen is part of the core build tools so needs to match supported Python versions of the TRAC runtime

    src_dir = pathlib.Path(src)
    dst_dir = pathlib.Path(dst)

    dst_dir.mkdir(parents=True, exist_ok=True)

    for src_item in src_dir.iterdir():

        rel_item = src_item.relative_to(src_dir)
        dst_item = dst_dir.joinpath(rel_item)

        if src_item.name == "__pycache__":
            continue

        if src_item.is_dir():
            _copytree(src_item, dst_item)
        else:
            if not dst_item.exists() or src_item.stat().st_mtime > dst_item.stat().st_mtime:
                shutil.copy2(src_item, dst_item)


class ProtoCtrlContext:

    # Provide some key extension protos from Google to handle web api annotations
    # The googleapis package would have the venv root as its namespace, so we need to copy to a temp dir

    def __init__(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self.proto_path = pathlib.Path()
        self.support_path = pathlib.Path()

    def __enter__(self):

        self._temp_dir.__enter__()

        temp_dir = pathlib.Path(self._temp_dir.name)
        proto_dir = temp_dir.joinpath("proto")
        proto_dir.mkdir()
        support_dir = temp_dir.joinpath("support")
        support_dir.mkdir()

        self.proto_path = proto_dir
        self.support_path = support_dir

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._temp_dir.__exit__(exc_type, exc_val, exc_tb)

    def copy_common_protos(self):

        # Core protos used by the protoc compiler itself
        protoc_inc_src = pathlib.Path(protoc.PROTOC_INCLUDE_DIR)
        protoc_inc_dst = pathlib.Path(self.support_path)

        _copytree(protoc_inc_src, protoc_inc_dst)

    def copy_google_api_protos(self):

        # Google API protos for annotating web services
        gapi_src = pathlib.Path(gapi_http_module.__file__).parent
        gapi_dst = pathlib.Path(self.support_path).joinpath("google/api")

        _copytree(gapi_src, gapi_dst)


def relocate_proto_package(proto_path: pathlib.Path, relocate):

    if isinstance(proto_path, str):
        proto_path = pathlib.Path(proto_path)

    source, target = relocate.split(":")

    # Move source -> temp -> target
    # Avoid conflicts if target is a sub-package of source

    source_pkg = proto_path.joinpath(source)
    temp_pkg = proto_path.joinpath("__temp")
    target_pkg = proto_path.joinpath(target)

    _log.info(f"Moving {source_pkg} -> {target_pkg}")

    shutil.move(source_pkg, temp_pkg)

    if not target_pkg.parent.exists():
        target_pkg.parent.mkdir(parents=True)

    shutil.move(temp_pkg, target_pkg)

    _log.info(f"Relocating imports for {source} -> {target}")

    match = re.compile(rf"import \"{source}/", re.MULTILINE)
    replace = f"import \"{target}/"

    _relocate_proto_imports(target_pkg, match, replace)


def _relocate_proto_imports(proto_path: pathlib.Path, match: re.Pattern, replace: str):

    for dir_entry in proto_path.iterdir():

        if dir_entry.name.endswith(".proto"):
            for line in fileinput.input(dir_entry, inplace=True):
                print(re.sub(match, replace, line), end="")

        elif dir_entry.is_dir():
            _relocate_proto_imports(dir_entry, match, replace)


def find_proto_files(proto_paths, packages, no_internal=False):

    proto_path_list = proto_paths if isinstance(proto_paths, list) else [proto_paths]

    for proto_path in proto_path_list:

        for entry in find_proto_files_in_dir(proto_path, proto_path, packages, no_internal):
            yield entry


def find_proto_files_in_dir(proto_path, root_proto_path, packages, no_internal):

    package_paths = list(map(lambda p: p.replace(".", "/"), packages)) if packages else None

    path_str = str(proto_path)

    if "=" in path_str:
        proto_path_ = pathlib.Path(path_str[path_str.index("=") + 1:])
    else:
        proto_path_ = pathlib.Path(path_str)

    for entry in proto_path_.iterdir():

        # Do not include internal parts of the API when generating for API docs
        if no_internal and not is_public_api(entry):
            _log.info(f"Excluding non-public API: [{entry.relative_to(root_proto_path)}]")
            continue

        if entry.is_dir():
            sub_path = proto_path_.joinpath(entry.name)
            for sub_entry in find_proto_files_in_dir(sub_path, root_proto_path, packages, no_internal):
                yield sub_entry

        elif entry.is_file() and entry.name.endswith(".proto"):
            if packages is None or is_in_packages(entry, package_paths):
                yield proto_path_.joinpath(entry.name)


def platform_args(base_args, proto_files):

    # On Linux/macOS, arg 0 is the name given to the process and is not actually passed to it!
    # Windows just passes all the arguments to the process

    if platform.system().lower().startswith("win"):
        return [protoc.PROTOC_EXE] + base_args + proto_files
    else:
        return ["protoc"] + base_args + proto_files


def build_protoc_args(generator, proto_paths, output_location, packages):

    # Protoc will call the plugin directly as an executable
    # On Windows, executing Python scripts directly requires the "py" launcher and an entry in PATHEXT
    # This might not always be available, particularly in CI
    # Using a one-line batch script avoids these complications

    if platform.system().lower().startswith("win"):
        trac_plugin = "protoc-gen-trac.bat"
    else:
        trac_plugin = "protoc-gen-trac=./protoc-gen-trac.py"

    proto_path_args = list(map(lambda pp: f"--proto_path={pp}", proto_paths))

    packages_option = "packages=" + ",".join(packages) if packages else ""

    if generator == "python_proto":

        proto_args = [
            f"--plugin=python",
            f"--python_out={output_location}",
            f"--pyi_out={output_location}"
        ]

    elif generator == "python_grpc":

        proto_args = [
            f"--grpc_python_out={output_location}"
        ]

    else:

        if generator == "python_runtime":

            options = "--trac_opt=target_package=tracdap.rt"

        elif generator == "python_doc":

            options = "--trac_opt=target_package=tracdap.rt;doc_format"

        elif generator == "api_doc":

            options = "--trac_opt=flat_pack;doc_format"

        else:

            raise ValueError(f"Unknown generator [{generator}]")

        if packages_option:
            options += f";{packages_option}"

        proto_args = [
            f"--plugin={trac_plugin}",
            f"--trac_out={output_location}",
            options
        ]

    return proto_path_args + proto_args


def cli_args():

    parser = argparse.ArgumentParser(description='TRAC code generator')

    parser.add_argument(
        "generator", type=str, metavar="generator",
        choices=["python_proto", "python_grpc", "python_runtime", "python_doc", "api_doc"],
        help="The documentation targets to build")

    parser.add_argument(
        "--proto_path", type=pathlib.Path, required=True, action="append", dest="proto_paths",
        help="Location of proto source files, relative to the repository root")

    parser.add_argument(
        "--out", type=pathlib.Path, required=True,
        help="Location where output files will be generated, relative to the repository root")

    parser.add_argument(
        "--package", type=str, action="append", dest="packages",
        help="Filter packages to include in generated output (TRAC generator only, default = generate all packages)")

    parser.add_argument(
        "--relocate", type=str, required=False, dest="relocate",
        help="Relocate packages in the generated code (source:dest e.g. tracdap:tracdap.rt._grpc)")

    parser.add_argument(
        "--no-internal", default=False, action="store_true", dest="no_internal",
        help="Ignore internal messages and APIs (for producing public-facing APIs and documentation)")

    return parser.parse_args()


def main():

    script_args = cli_args()
    output_dir = ROOT_DIR.joinpath(script_args.out)
    packages = script_args.packages

    # Provide some key extension protos from Google to handle web api annotations
    with ProtoCtrlContext() as context:

        context.copy_common_protos()
        context.copy_google_api_protos()

        if script_args.relocate:
            for proto_path in script_args.proto_paths:
                _copytree(proto_path, context.proto_path)
            relocate_proto_package(context.proto_path, script_args.relocate)
            proto_paths = [context.proto_path]
        else:
            proto_paths = [ROOT_DIR.joinpath(pp) for pp in script_args.proto_paths]

        # Only look for files to generate that were explicitly specified
        protoc_files = list(find_proto_files(proto_paths, script_args.packages, script_args.no_internal))

        # Now add supporting proto paths (needed during generation)
        proto_paths.append(context.support_path)

        protoc_args = build_protoc_args(script_args.generator, proto_paths, output_dir, packages)
        protoc_argv = platform_args(protoc_args, protoc_files)

        if script_args.generator == "python_grpc":
            protoc_executable = sys.executable
            protoc_argv = [sys.executable, "-m", "grpc_tools.protoc"] + protoc_argv[1:]
        else:
            protoc_executable = protoc.PROTOC_EXE

        newline = "\n"
        _log.info(f"Running protoc: {newline.join(map(str, protoc_argv))}")

        # Make sure the output dir exists before running protoc
        pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Always run protoc from the codegen folder
        # This makes finding the TRAC protoc plugin much easier
        result = sp.run(executable=protoc_executable, args=protoc_argv, cwd=SCRIPT_DIR, stdout=sp.PIPE)

        # We are not piping stdout/stderr
        # Logs and errors  will show up as protoc is running
        # No need to report again here

        if result.returncode == 0:
            _log.info("Protoc succeeded")
            exit(0)

        else:
            _log.error(f"Protoc failed with code {result.returncode}")
            exit(result.returncode)


if __name__ == "__main__":
    main()
