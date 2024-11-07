# Licensed to the Fintech Open Source Foundation (FINOS) under one or
# more contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright ownership.
# FINOS licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
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


class DataRoundTripModel(trac.TracModel):

    ROUND_TRIP_FIELDS = [
        trac.F("boolean_field", trac.BasicType.BOOLEAN, label="BOOLEAN field"),
        trac.F("integer_field", trac.BasicType.INTEGER, label="INTEGER field"),
        trac.F("float_field", trac.BasicType.FLOAT, label="FLOAT field"),
        trac.F("decimal_field", trac.BasicType.DECIMAL, label="DECIMAL field"),
        trac.F("string_field", trac.BasicType.STRING, label="STRING field"),
        trac.F("date_field", trac.BasicType.DATE, label="DATE field"),
        trac.F("datetime_field", trac.BasicType.DATETIME, label="DATETIME field")]

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("data_framework", trac.BasicType.STRING, default_value="pandas", label="The data framework to test with"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        round_trip_input = trac.define_input_table(self.ROUND_TRIP_FIELDS)

        return {"round_trip_input": round_trip_input}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        round_trip_output = trac.define_output_table(self.ROUND_TRIP_FIELDS)

        return {"round_trip_output": round_trip_output}

    def run_model(self, ctx: trac.TracContext):

        data_framework = ctx.get_parameter("data_framework")

        if data_framework == "pandas":
            round_trip_input = ctx.get_pandas_table("round_trip_input")
            ctx.put_pandas_table("round_trip_output", round_trip_input)

        elif data_framework == "polars":
            round_trip_input = ctx.get_polars_table("round_trip_input")
            ctx.put_polars_table("round_trip_output", round_trip_input)

        elif data_framework == "spark":
            round_trip_input = ctx.get_spark_table("round_trip_input")
            ctx.put_spark_table("round_trip_output", round_trip_input)

        else:
            raise RuntimeError(f"Unsupported data framework: [{data_framework}]")
