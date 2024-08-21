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

import tutorial.using_data as using_data
import tutorial.schemas as schemas


class OptionalIOModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(

            trac.P("eur_usd_rate", trac.FLOAT,
                   label="EUR/USD spot rate for reporting"),

            trac.P("default_weighting", trac.FLOAT,
                   label="Weighting factor applied to the profit/loss of a defaulted loan"),

            trac.P("filter_defaults", trac.BOOLEAN,
                   label="Exclude defaulted loans from the calculation",
                   default_value=False))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        # Define an optional account filter input, using external schema files

        customer_loans = trac.load_schema(schemas, "customer_loans.csv")
        account_filter = trac.load_schema(schemas, "account_filter.csv")

        return {
            "customer_loans": trac.ModelInputSchema(customer_loans),
            "account_filter": trac.ModelInputSchema(account_filter, optional=True)
        }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        # Define an optional output for stats on excluded accounts, using schema definitions in code

        profit_by_region = trac.define_output_table(
            trac.F("region", trac.STRING, label="Customer home region", categorical=True),
            trac.F("gross_profit", trac.DECIMAL, label="Total gross profit"))

        exclusions = trac.define_output_table(
            trac.F("reason", trac.STRING, "Reason for exclusion"),
            trac.F("count", trac.INTEGER, "Number of accounts"),
            optional=True)

        return {
            "profit_by_region": profit_by_region,
            "exclusions": exclusions
        }

    def run_model(self, ctx: trac.TracContext):

        eur_usd_rate = ctx.get_parameter("eur_usd_rate")
        default_weighting = ctx.get_parameter("default_weighting")
        filter_defaults = ctx.get_parameter("filter_defaults")

        customer_loans = ctx.get_pandas_table("customer_loans")

        if ctx.has_dataset("account_filter"):

            # Filter out customer accounts with IDs in the filter set
            account_filter = ctx.get_pandas_table("account_filter")
            account_mask = customer_loans['id'].isin(account_filter["account_id"])
            customer_loans = customer_loans.loc[~account_mask]

            # Create an optional output with some stats about the excluded accounts
            exclusions = account_filter.groupby(["reason"]).size().to_frame(name="count").reset_index()
            ctx.put_pandas_table("exclusions", exclusions)

        profit_by_region = using_data.calculate_profit_by_region(
            customer_loans, eur_usd_rate,
            default_weighting, filter_defaults)

        ctx.put_pandas_table("profit_by_region", profit_by_region)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(OptionalIOModel, "config/optional_io.yaml", "config/sys_config.yaml")
