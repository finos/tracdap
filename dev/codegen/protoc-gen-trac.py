#!/usr/bin/env python

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

import sys
import logging
import itertools as it

import google.protobuf.compiler.plugin_pb2 as pb_plugin  # noqa

import generator as gen


class TracPlugin:

    def __init__(self, pb_request: pb_plugin.CodeGeneratorRequest):

        logging_format = f"%(levelname)s %(name)s: %(message)s"
        logging.basicConfig(format=logging_format, level=logging.INFO)
        self._log = logging.getLogger(TracPlugin.__name__)

        self._request = pb_request

        options_str = self._request.parameter.split(";")
        options_kv = map(lambda opt: opt.split("=", 1), options_str)
        self._options = {opt[0]: opt[1] if len(opt) > 1 else True for opt in options_kv}

        for k, v in self._options.items():
            self._log.info(f"Option {k} = {v}")

    def generate(self):

        try:

            generator = gen.TracGenerator(self._options)

            # Build a static type map in a separate first pass
            type_map = generator.build_type_map(self._request.proto_file)

            generated_response = pb_plugin.CodeGeneratorResponse()

            input_files = self._request.proto_file
            input_files = filter(lambda f: f.name in self._request.file_to_generate, input_files)
            input_files = filter(lambda f: f.source_code_info.ByteSize() > 0, input_files)

            sorted_files = input_files  # sorted(input_files, key=lambda f: f.package)
            packages = it.groupby(sorted_files, lambda f: f.package)

            for package, files in packages:

                # Take files out of iter group, they may be used multiple times
                files = list(files)

                package_files = generator.generate_package(package, files, type_map)
                generated_response.file.extend(package_files)

            # Generate package-level files for empty packages
            # (this creates a valid package tree from the root package)

            proto_packages = set(map(lambda f: f.package, self._request.proto_file))
            all_packages = self.expand_parent_packages(proto_packages)

            for package in all_packages:
                if package not in proto_packages:
                    package_files = generator.generate_package(package, [], type_map)
                    generated_response.file.extend(package_files)

            return generated_response

        except gen.ECodeGeneration as e:

            error_response = pb_plugin.CodeGeneratorResponse()
            error_response.error = str(e)
            return error_response

    @staticmethod
    def expand_parent_packages(packages):

        all_packages = set()

        for package in packages:

            package_path = package.split(".")

            for i in range(len(package_path)):
                parent_package = ".".join(package_path[:i+1])
                all_packages.add(parent_package)

        return all_packages


if __name__ == "__main__":

    data = sys.stdin.buffer.read()

    request = pb_plugin.CodeGeneratorRequest()
    request.ParseFromString(data)

    plugin = TracPlugin(request)

    response = plugin.generate()
    output = response.SerializeToString()

    sys.stdout.buffer.write(output)
