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

import protoc


def platform_args(raw_args):

    # On Linux/macOS, arg 0 is the name given to the process and is not actually passed to it!
    # Windows just passes all the arguments to the process

    if platform.system().lower().startswith("win"):
        return raw_args
    else:
        return ["protoc"] + raw_args


def main(argv):

    proto_location = "../../../trac-api/trac-metadata/src/main/proto"
    output_location = "../../../build/modules/trac-executors/python/generated"

    protoc_args = [

        "--python_out={}/trac_gen/protoc".format(output_location),
        "--proto_path={}".format(proto_location),

        "@metadata_inputs.txt"
    ]

    pythonic_args = [

        "--plugin=protoc-gen-pythonic.py",
        "--pythonic_out={}/trac_gen/pythonic".format(output_location),
        "--proto_path={}".format(proto_location),

        "@metadata_inputs.txt"
    ]

    if len(argv) > 1 and argv[1] == "--pythonic":

        pathlib.Path(output_location).joinpath("trac_gen/pythonic").mkdir(parents=True, exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/pythonic/__init__.py").touch(exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/__init__.py").touch(exist_ok=True)

        protoc.exec_protoc(platform_args(pythonic_args))

    else:

        pathlib.Path(output_location).joinpath("trac_gen/protoc").mkdir(parents=True, exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/protoc/__init__.py").touch(exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/__init__.py").touch(exist_ok=True)

        protoc.exec_protoc(platform_args(protoc_args))


if __name__ == "__main__":
    main(sys.argv)
