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

import datetime as dt
import decimal
import typing as tp

import pandas as pd

import tracdap.rt.api as trac


class SecondModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("param_2", trac.DATE, "A data parameter", default_value=dt.date(2000, 1, 1)),
            trac.P("param_3", trac.FLOAT, "A float parameter"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        preprocessed = trac.define_input_table(
            trac.F("id", trac.BasicType.STRING, label="Customer account ID", business_key=True),
            trac.F("some_quantity_x", trac.BasicType.DECIMAL, label="Some quantity X", format_code="CCY:EUR"))

        return {"preprocessed_data": preprocessed}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        profit_by_region = trac.define_output_table(
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("gross_profit", trac.BasicType.DECIMAL, label="Total gross profit", format_code="CCY:USD"))

        return {"profit_by_region": profit_by_region}

    def run_model(self, ctx: trac.TracContext):

        preproc = ctx.get_pandas_table("preprocessed_data")

        profit_by_region = pd.DataFrame(data={
            "region": ["uk", "us"],
            "gross_profit": [decimal.Decimal(24000000), decimal.Decimal(13000000)]})

        ctx.put_pandas_table("profit_by_region", profit_by_region)
