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


class QuickStartModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("eur_usd_rate", trac.FLOAT, label="EUR/USD spot rate for reporting"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        customer_loans = trac.define_input_table(
            trac.F("id", trac.STRING, label="Customer account ID", business_key=True),
            trac.F("loan_amount", trac.FLOAT, label="Principal loan amount"),
            trac.F("total_pymnt", trac.FLOAT, label="Total amount repaid"),
            trac.F("region", trac.STRING, label="Customer home region"),
            trac.F("loan_condition_cat", trac.INTEGER, label="Loan condition category"))

        return {"customer_loans": customer_loans}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        profit_by_region = trac.define_output_table(
            trac.F("region", trac.STRING, label="Customer home region"),
            trac.F("gross_profit", trac.FLOAT, label="Total gross profit"))

        return {"profit_by_region": profit_by_region}

    def run_model(self, ctx: trac.TracContext):

        eur_usd_rate = ctx.get_parameter("eur_usd_rate")
        customer_loans = ctx.get_pandas_table("customer_loans")

        customer_loans["gross_profit"] = \
            (customer_loans["total_pymnt"] - customer_loans["loan_amount"]) \
            * eur_usd_rate

        profit_by_region = customer_loans \
            .groupby("region", as_index=False) \
            .aggregate({"gross_profit": "sum"})

        ctx.put_pandas_table("profit_by_region", profit_by_region)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(QuickStartModel, "config/quick_start.yaml", "config/sys_config.yaml")
