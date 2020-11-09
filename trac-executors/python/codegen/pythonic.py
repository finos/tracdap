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
import sys

import protoc


def main(argv):

    protoc_args = [

        # Arg 0 is the name given to the process, not actually passed to it!
        # (This is on macOS, may behave differently on other platforms, especially Windows)
        "protoc",

        "--python_out=../../build/modules/trac-executors/python/generated/protoc",
        "--proto_path=../../trac-api/trac-metadata/src/main/proto",

        "@codegen/metadata_inputs.txt"
    ]

    pythonic_args = [

        # Arg 0 is the name given to the process, not actually passed to it!
        # (This is on macOS, may behave differently on other platforms, especially Windows)
        "protoc",

        "--plugin=protoc-gen-pythonic=./codegen/pythonic_plugin.py",
        "--pythonic_out=../../build/modules/trac-executors/python/generated/pythonic",
        "--proto_path=../../trac-api/trac-metadata/src/main/proto",

        "@codegen/metadata_inputs.txt"
    ]

    pathlib.Path("../../build/modules/trac-executors/python/generated/protoc").mkdir(parents=True, exist_ok=True)
    pathlib.Path("../../build/modules/trac-executors/python/generated/pythonic").mkdir(parents=True, exist_ok=True)

    if len(argv) > 1 and argv[1] == "--pythonic":
        protoc.exec_protoc(pythonic_args)
    else:
        protoc.exec_protoc(protoc_args)


if __name__ == "__main__":
    main(sys.argv)
