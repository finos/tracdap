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

import decimal
import typing as tp

import tracdap.rt.api as trac


class UsingDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(

            trac.P("eur_usd_rate", trac.BasicType.FLOAT,
                   label="EUR/USD spot rate for reporting"),

            trac.P("default_weighting", trac.BasicType.FLOAT,
                   label="Weighting factor applied to the profit/loss of a defaulted loan"),

            trac.P("filter_defaults", trac.BasicType.BOOLEAN,
                   label="Exclude defaulted loans from the calculation",
                   default_value=False))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        customer_loans = trac.declare_input_table(
            trac.F("id", trac.BasicType.STRING, label="Customer account ID", business_key=True),
            trac.F("loan_amount", trac.BasicType.DECIMAL, label="Principal loan amount", format_code="CCY:EUR"),
            trac.F("total_pymnt", trac.BasicType.DECIMAL, label="Total amount repaid", format_code="CCY:EUR"),
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("loan_condition_cat", trac.BasicType.INTEGER, label="Loan condition category", categorical=True))

        return {"customer_loans": customer_loans}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        profit_by_region = trac.declare_output_table(
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("gross_profit", trac.BasicType.DECIMAL, label="Total gross profit", format_code="CCY:USD"))

        return {"profit_by_region": profit_by_region}

    def run_model(self, ctx: trac.TracContext):

        eur_usd_rate = ctx.get_parameter("eur_usd_rate")
        default_weighting = ctx.get_parameter("default_weighting")
        filter_defaults = ctx.get_parameter("filter_defaults")

        customer_loans = ctx.get_pandas_table("customer_loans")

        if filter_defaults:
            customer_loans = customer_loans[customer_loans["loan_condition_cat"] == 0]

        customer_loans.loc[:, "gross_profit_unweighted"] = \
            customer_loans["total_pymnt"] - \
            customer_loans["loan_amount"]

        condition_weighting = customer_loans["loan_condition_cat"] \
            .apply(lambda c: decimal.Decimal(default_weighting) if c > 0 else decimal.Decimal(1))

        customer_loans.loc[:, "gross_profit_weighted"] = \
            customer_loans["gross_profit_unweighted"] * condition_weighting

        customer_loans.loc[:, "gross_profit"] = \
            customer_loans["gross_profit_weighted"] \
            .apply(lambda x: x * decimal.Decimal.from_float(eur_usd_rate))

        profit_by_region = customer_loans \
            .groupby("region", as_index=False) \
            .aggregate({"gross_profit": "sum"})

        ctx.put_pandas_table("profit_by_region", profit_by_region)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(UsingDataModel, "using_data.yaml", "../sys_config.yaml")
