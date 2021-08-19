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

import pathlib
import platform
import shutil
import subprocess as sp
import argparse
import logging
import tempfile

import protoc
import google.api  # noqa


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


class ProtoApiExtensions:

    # Provide some key extension protos from Google to handle web api annotations
    # The googleapis package would have the venv root as its namespace, so we need to copy to a temp dir

    def __init__(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_name = ""

    def __enter__(self):

        self.temp_dir_name = self._temp_dir.__enter__()

        # Core protos used by the protoc compiler itself
        protoc_inc_src = pathlib.Path(protoc.PROTOC_INCLUDE_DIR)
        protoc_inc_dst = pathlib.Path(self.temp_dir_name)

        _log.info(f"Copying {protoc_inc_src} -> {protoc_inc_dst}")
        shutil.copytree(protoc_inc_src, protoc_inc_dst, dirs_exist_ok=True)

        # Google API protos for annotating web services
        gapi_src = pathlib.Path(google.api.__file__).parent
        gapi_dst = pathlib.Path(self.temp_dir_name).joinpath("google/api")

        _log.info(f"Copying {gapi_src} -> {gapi_dst}")
        shutil.copytree(gapi_src, gapi_dst, dirs_exist_ok=True)

        return self.temp_dir_name

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._temp_dir.__exit__(exc_type, exc_val, exc_tb)


def find_proto_files(proto_paths):

    proto_path_list = proto_paths if isinstance(proto_paths, list) else [proto_paths]

    for proto_path in proto_path_list:

        for entry in proto_path.iterdir():

            if entry.is_file() and entry.name.endswith(".proto"):
                yield proto_path.joinpath(entry.name)

            elif entry.is_dir():
                for sub_entry in find_proto_files(proto_path.joinpath(entry.name)):
                    yield sub_entry


def platform_args(base_args, proto_files):

    # On Linux/macOS, arg 0 is the name given to the process and is not actually passed to it!
    # Windows just passes all the arguments to the process

    if platform.system().lower().startswith("win"):
        return [protoc.PROTOC_EXE] + base_args + proto_files
    else:
        return ["protoc"] + base_args + proto_files


def build_protoc_args(generator, proto_paths, output_location, packages):

    if platform.system().lower().startswith("win"):
        trac_plugin = "protoc-gen-trac.py"
    else:
        trac_plugin = "protoc-gen-trac=./protoc-gen-trac.py"

    proto_path_args = list(map(lambda pp: f"--proto_path={pp}", proto_paths))

    packages_option = "packages=" + ",".join(map(str, packages)) if packages else ""

    if generator == "python_proto":

        proto_args = [
            f"--plugin=python",
            f"--python_out={output_location}"
        ]

    elif generator == "python_runtime":

        proto_args = [
            f"--plugin={trac_plugin}",
            f"--trac_out={output_location}",
        ]

        if packages_option:
            proto_args.append(f"--trac_opt={packages_option}")

    elif generator == "api_doc":

        options = "--trac_opt=flat_pack"

        if packages_option:
            options += f";{packages_option}"

        proto_args = [
            f"--plugin={trac_plugin}",
            f"--trac_out={output_location}",
            options
        ]

    else:

        raise ValueError(f"Unknown generator [{generator}]")

    return proto_path_args + proto_args


def cli_args():

    parser = argparse.ArgumentParser(description='TRAC code generator')

    parser.add_argument(
        "generator", type=str, metavar="generator", choices=["python_proto", "python_runtime", "api_doc"],
        help="The documentation targets to build")

    parser.add_argument(
        "--proto_path", type=pathlib.Path, required=True, action="append", dest="proto_paths",
        help="Location of proto source files, relative to the repository root")

    parser.add_argument(
        "--out", type=pathlib.Path, required=True,
        help="Location where output files will be generated, relative to the repository root")

    parser.add_argument(
        "--package", type=pathlib.Path, action="append", dest="packages",
        help="Filter packages to include in generated output (TRAC generator only, default = generate all packages)")

    return parser.parse_args()


def main():

    script_args = cli_args()
    proto_paths = list(map(lambda pp: ROOT_DIR.joinpath(pp), script_args.proto_paths))
    output_dir = ROOT_DIR.joinpath(script_args.out)
    packages = script_args.packages

    # Provide some key extension protos from Google to handle web api annotations
    with ProtoApiExtensions() as proto_ext_path:

        # Include all available proto paths when generating proto args, so they're available to protoc if referenced
        all_proto_paths = proto_paths + [proto_ext_path]
        protoc_args = build_protoc_args(script_args.generator, all_proto_paths, output_dir, packages)

        # Only look for files to generate that were explicitly specified
        protoc_files = list(find_proto_files(proto_paths))

        protoc_argv = platform_args(protoc_args, protoc_files)

        newline = "\n"
        _log.info(f"Running protoc: {newline.join(map(str, protoc_argv))}")

        # Make sure the output dir exists before running protoc
        pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Always run protoc from the codegen folder
        # This makes finding the TRAC protoc plugin much easier
        result = sp.run(executable=protoc.PROTOC_EXE, args=protoc_argv, cwd=SCRIPT_DIR)

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
