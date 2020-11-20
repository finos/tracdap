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
    output_location = "../../../build/modules/trac-runtime/python/generated"

    gen_proto_args = [

        "--plugin=python",
        "--python_out={}/trac_gen/proto".format(output_location),
        "--proto_path={}".format(proto_location),

        "@metadata_inputs.txt"
    ]

    if platform.system().lower().startswith("win"):
        protoc_plugin = "--plugin=protoc-gen-trac.py"
    else:
        protoc_plugin = "--plugin=protoc-gen-trac=./protoc-gen-trac.py"

    gen_trac_args = [

        protoc_plugin,
        "--trac_out={}/trac_gen/domain".format(output_location),
        "--proto_path={}".format(proto_location),

        "@metadata_inputs.txt"
    ]

    if len(argv) > 1 and argv[1] == "--domain":

        # TRAC domain classes generator adds init scripts for its own package hierarchy
        # Since we nesting inside trac_gen/domain, add init files for those packages here

        pathlib.Path(output_location).joinpath("trac_gen/domain").mkdir(parents=True, exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/domain/__init__.py").touch(exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/__init__.py").touch(exist_ok=True)

        protoc.exec_protoc(platform_args(gen_trac_args))

    else:

        # Native Python plugin does not create init scripts for its own package hierarchy
        # Add them here instead

        pathlib.Path(output_location).joinpath("trac_gen/proto/trac/metadata").mkdir(parents=True, exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/proto/trac/metadata/__init__.py").touch(exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/proto/trac/__init__.py").touch(exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/proto/__init__.py").touch(exist_ok=True)
        pathlib.Path(output_location).joinpath("trac_gen/__init__.py").touch(exist_ok=True)

        protoc.exec_protoc(platform_args(gen_proto_args))


if __name__ == "__main__":
    main(sys.argv)
