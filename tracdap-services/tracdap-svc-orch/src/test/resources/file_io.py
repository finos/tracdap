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

import typing as tp

import tracdap.rt.api as trac


class FileIOModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("n_copies", trac.BasicType.INTEGER, label="Number of times to copy the input data"),
            trac.P("use_streams", trac.BasicType.BOOLEAN, default_value=False, label="Flag to enable streams for file IO"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        file_input = trac.define_input(trac.CommonFileTypes.TXT, label="Quarterly sales report")

        return {"file_input": file_input}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        file_output = trac.define_output(trac.CommonFileTypes.TXT, label="Quarterly sales report")

        return {"file_output": file_output}

    def run_model(self, ctx: trac.TracContext):

        n_copies = ctx.get_parameter("n_copies")
        use_streams = ctx.get_parameter("use_streams")

        if use_streams:

            with ctx.get_file_stream("file_input") as in_stream:
                in_data = in_stream.read()

            out_data = in_data * n_copies

            with ctx.put_file_stream("file_output") as out_stream:
                out_stream.write(out_data)

        else:

            in_data = ctx.get_file("file_input")
            out_data = in_data * n_copies
            ctx.put_file("file_output", out_data)
