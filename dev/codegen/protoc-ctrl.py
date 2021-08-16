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
import sys
import os
import subprocess as sp

import protoc

# Paths are relative to the codegen folder
proto_location = "../../trac-api/trac-metadata/src/main/proto"
output_location = "../../trac-runtime/python/generated"


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
        return base_args + proto_files
    else:
        return ["protoc"] + base_args + proto_files


# Context class to change directory for the lifetime of the codegen process
class cd:

    def __init__(self, new_path):
        self.new_path = os.path.expanduser(new_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.saved_path)


def main(argv):

    gen_proto_args = [

        "--plugin=python",
        "--python_out={}/trac_gen/proto".format(output_location),
        "--proto_path={}".format(proto_location)
    ]

    if platform.system().lower().startswith("win"):
        protoc_plugin = "--plugin=protoc-gen-trac.py"
    else:
        protoc_plugin = "--plugin=protoc-gen-trac=./protoc-gen-trac.py"

    gen_trac_args = [

        protoc_plugin,
        "--trac_out={}/trac_gen/domain".format(output_location),
        "--proto_path={}".format(proto_location)
    ]

    # Always run codegen from the codegen folder
    # This makes finding the TRAC protoc plugin much easier
    codegen_path = str(pathlib.Path(__file__).parent)
    with cd(codegen_path):

        if len(argv) > 1 and argv[1] == "--domain":

            # TRAC domain classes generator adds init scripts for its own package hierarchy
            # Since we nesting inside trac_gen/domain, add init files for those packages here

            pathlib.Path(output_location).joinpath("trac_gen/domain").mkdir(parents=True, exist_ok=True)
            pathlib.Path(output_location).joinpath("trac_gen/domain/__init__.py").touch(exist_ok=True)
            pathlib.Path(output_location).joinpath("trac_gen/__init__.py").touch(exist_ok=True)

            proto_files = list(find_proto_files(proto_location))
            argv = platform_args(gen_trac_args, proto_files)
            codegen_result = sp.run(executable=protoc.PROTOC_EXE, args=argv)

        else:

            # Native Python plugin does not create init scripts for its own package hierarchy
            # Add them here instead

            pathlib.Path(output_location).joinpath("trac_gen/proto/trac/metadata").mkdir(parents=True, exist_ok=True)
            pathlib.Path(output_location).joinpath("trac_gen/proto/trac/metadata/__init__.py").touch(exist_ok=True)
            pathlib.Path(output_location).joinpath("trac_gen/proto/trac/__init__.py").touch(exist_ok=True)
            pathlib.Path(output_location).joinpath("trac_gen/proto/__init__.py").touch(exist_ok=True)
            pathlib.Path(output_location).joinpath("trac_gen/__init__.py").touch(exist_ok=True)

            proto_files = list(find_proto_files(proto_location))
            argv = platform_args(gen_proto_args, proto_files)
            codegen_result = sp.run(executable=protoc.PROTOC_EXE, args=argv)

    # We are not piping stdout/stderr
    # Errors will show up as protoc runs instead
    # No need to report again here

    if codegen_result.returncode == 0:
        print("Python codegen succeeded")
        exit(0)

    else:
        print("Python codegen failed")
        exit(codegen_result.returncode)


if __name__ == "__main__":
    main(sys.argv)
