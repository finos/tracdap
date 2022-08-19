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

import tutorial.using_data as using_data
import tutorial.schemas as schemas


class SchemaFilesModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(

            trac.P("eur_usd_rate", trac.BasicType.FLOAT,
                   label="EUR/USD spot rate for reporting"),

            trac.P("default_weighting", trac.BasicType.FLOAT,
                   label="Weighting factor applied to the profit/loss of a defaulted loan"),

            trac.P("filter_defaults", trac.BasicType.BOOLEAN,
                   label="Exclude defaulted loans from the calculation",
                   default_value=False))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        customer_loans = trac.load_schema(schemas, "customer_loans.csv")

        return {"customer_loans": trac.ModelInputSchema(customer_loans)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        profit_by_region = trac.load_schema(schemas, "profit_by_region.csv")

        return {"profit_by_region": trac.ModelOutputSchema(profit_by_region)}

    def run_model(self, ctx: trac.TracContext):

        eur_usd_rate = ctx.get_parameter("eur_usd_rate")
        default_weighting = ctx.get_parameter("default_weighting")
        filter_defaults = ctx.get_parameter("filter_defaults")

        customer_loans = ctx.get_pandas_table("customer_loans")

        profit_by_region = using_data.calculate_profit_by_region(
            customer_loans, filter_defaults,
            default_weighting, eur_usd_rate)

        ctx.put_pandas_table("profit_by_region", profit_by_region)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(SchemaFilesModel, "config/using_data.yaml", "config/sys_config.yaml")
