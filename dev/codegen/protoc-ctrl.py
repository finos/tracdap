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
import subprocess as sp
import argparse
import logging

import protoc


SCRIPT_NAME = pathlib.Path(__file__).stem

SCRIPT_DIR = pathlib.Path(__file__) \
    .parent \
    .absolute() \
    .resolve()

ROOT_DIR = SCRIPT_DIR \
    .joinpath("../..") \
    .resolve()

METADATA_PROTO_DIR = ROOT_DIR \
    .joinpath("trac-api/trac-metadata/src/main/proto")


# Configure logging
logging_format = f"%(levelname)s %(name)s: %(message)s"
logging.basicConfig(format=logging_format, level=logging.INFO)
_log = logging.getLogger(SCRIPT_NAME)


def find_proto_files(path):

    base_path = pathlib.Path(path)

    for entry in base_path.iterdir():

        if entry.is_file() and entry.name.endswith(".proto"):
            yield base_path.joinpath(entry.name)

        elif entry.is_dir():
            for sub_entry in find_proto_files(base_path.joinpath(entry.name)):
                yield sub_entry


def platform_args(base_args, proto_files):

    # On Linux/macOS, arg 0 is the name given to the process and is not actually passed to it!
    # Windows just passes all the arguments to the process

    if platform.system().lower().startswith("win"):
        return [protoc.PROTOC_EXE] + base_args + proto_files
    else:
        return ["protoc"] + base_args + proto_files


def build_protoc_args(generator, output_location):

    if platform.system().lower().startswith("win"):
        trac_plugin = "protoc-gen-trac.py"
    else:
        trac_plugin = "protoc-gen-trac=./protoc-gen-trac.py"

    if generator == "python_proto":

        proto_args = [
            f"--proto_path={METADATA_PROTO_DIR.as_posix() +'/'}",
            f"--plugin=python",
            f"--python_out={output_location}"
        ]

    elif generator == "python_runtime":

        proto_args = [
            f"--proto_path={METADATA_PROTO_DIR.as_posix() +'/'}",
            f"--plugin={trac_plugin}",
            f"--trac_out={output_location}",
            # f"--trac_opt=flat_pack"
        ]

    elif generator == "api_doc":

        proto_args = [
            f"--proto_path={METADATA_PROTO_DIR}",
            f"--plugin={trac_plugin}",
            f"--trac_out={output_location}",
            f"--trac_opt=flat_pack"
        ]

    else:

        raise ValueError(f"Unknown generator [{generator}]")

    return proto_args


def cli_args():

    parser = argparse.ArgumentParser(description='TRAC code generator')

    parser.add_argument(
        "generator", type=str, metavar="generator", choices=["python_proto", "python_runtime", "api_doc"],
        help="The documentation targets to build")

    parser.add_argument(
        "--out", type=pathlib.Path,
        help="Location where output files will be generated")

    return parser.parse_args()


def main():

    script_args = cli_args()
    output_dir = ROOT_DIR.joinpath(script_args.out)

    proto_args = build_protoc_args(script_args.generator, output_dir)

    proto_files = list(find_proto_files(METADATA_PROTO_DIR))
    argv = platform_args(proto_args, proto_files)

    # Make sure the output dir exists before running protoc
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Always run protoc from the codegen folder
    # This makes finding the TRAC protoc plugin much easier
    result = sp.run(executable=protoc.PROTOC_EXE, args=argv, cwd=SCRIPT_DIR)

    # We are not piping stdout/stderr
    # Logs and errors  will show up as protoc is running
    # No need to report again here

    if result.returncode == 0:
        _log.info("Codegen succeeded")
        exit(0)

    else:
        _log.error(f"Codegen failed with code {result.returncode}")
        exit(result.returncode)


if __name__ == "__main__":
    main()
