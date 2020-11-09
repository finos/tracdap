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

import google.protobuf.compiler.plugin_pb2 as pb_plugin

import codegen.generator as gen


class PythonicPlugin:

    def __init__(self, pb_request: pb_plugin.CodeGeneratorRequest):

        logging.basicConfig(level=logging.DEBUG)
        self._log = logging.getLogger(PythonicPlugin.__name__)

        self._request = pb_request

    def generate(self):

        generator = gen.PythonicGenerator()
        generated_response = pb_plugin.CodeGeneratorResponse()

        for file_descriptor in self._request.proto_file:

            if file_descriptor.name not in self._request.file_to_generate:
                continue

            if not file_descriptor.source_code_info.ByteSize():
                continue

            file_code = generator.generate_file(0, file_descriptor)

            self._log.debug(file_descriptor.name + "\n" + file_code)

            file_response = pb_plugin.CodeGeneratorResponse.File()
            file_response.name = file_descriptor.name.replace(".proto", ".py")
            file_response.content = file_code

            generated_response.file.append(file_response)

        return generated_response


if __name__ == "__main__":

    data = sys.stdin.buffer.read()

    request = pb_plugin.CodeGeneratorRequest()
    request.ParseFromString(data)

    plugin = PythonicPlugin(request)

    response = plugin.generate()
    output = response.SerializeToString()

    sys.stdout.buffer.write(output)
