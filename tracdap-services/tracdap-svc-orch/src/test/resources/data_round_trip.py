#  Copyright 2022 Accenture Global Solutions Limited
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


class DataRoundTripModel(trac.TracModel):

    ROUND_TRIP_FIELDS = [
        trac.F("boolean_field", trac.BasicType.BOOLEAN, label="BOOLEAN field"),
        trac.F("integer_field", trac.BasicType.INTEGER, label="INTEGER field"),
        trac.F("float_field", trac.BasicType.FLOAT, label="FLOAT field"),
        trac.F("decimal_field", trac.BasicType.DECIMAL, label="DECIMAL field"),
        trac.F("string_field", trac.BasicType.STRING, label="STRING field"),
        trac.F("date_field", trac.BasicType.DATE, label="DATE field"),
        trac.F("datetime_field", trac.BasicType.DATETIME, label="DATETIME field"),
        trac.F("categorical_field", trac.BasicType.STRING, categorical=True, label="CATEGORICAL field")]

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(
            trac.P("use_spark", trac.BasicType.BOOLEAN, default_value=False, label="Use Spark for round trip testing"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        round_trip_input = trac.declare_input_table(self.ROUND_TRIP_FIELDS)

        return {"round_trip_input": round_trip_input}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        round_trip_output = trac.declare_input_table(self.ROUND_TRIP_FIELDS)

        return {"round_trip_output": round_trip_output}

    def run_model(self, ctx: trac.TracContext):

        use_spark = ctx.get_parameter("use_spark")

        if use_spark:
            round_trip_input = ctx.get_spark_table("round_trip_input")
            ctx.put_spark_table("round_trip_output", round_trip_input)

        else:
            round_trip_input = ctx.get_pandas_table("round_trip_input")
            ctx.put_pandas_table("round_trip_output", round_trip_input)
