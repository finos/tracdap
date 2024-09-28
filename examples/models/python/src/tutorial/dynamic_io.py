#  Copyright 2024 Accenture Global Solutions Limited
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

import typing as tp
import tracdap.rt.api as trac


class DynamicIOModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("filter_column", trac.STRING, label="Filter colum"),
            trac.P("filter_value", trac.STRING, label="Filter value"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        return { "original_data": trac.define_input_table(dynamic=True, label="Original (unfiltered) data") }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        filtered_schema = trac.SchemaDefinition(
            schemaType=trac.SchemaType.TABLE)

        filtered_output = trac.ModelOutputSchema(
            schema=filtered_schema, dynamic=True,
            label="Filtered (output) data")

        return { "filtered_data": filtered_output }

    def run_model(self, ctx: trac.TracContext):

        original_schema = ctx.get_schema("original_data")
        original_data = ctx.get_pandas_table("original_data")

        filter_column = ctx.get_parameter("filter_column")
        filter_value = ctx.get_parameter("filter_value")

        filtered_data = original_data[original_data[filter_column] != filter_value]

        ctx.put_schema("filtered_data", original_schema)
        ctx.put_pandas_table("filtered_data", filtered_data)
