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
import typing as tp

import tracdap.rt.api as trac


class FirstModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("param_1", trac.INTEGER, "First parameter"),
            trac.P("param_2", trac.DATE, "Second parameter", default_value=dt.date(2001, 1, 1)))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        customer_loans = trac.define_input_table(
            trac.F("id", trac.STRING, label="Customer account ID", business_key=True),
            trac.F("loan_amount", trac.DECIMAL, label="Principal loan amount"),
            trac.F("total_pymnt", trac.DECIMAL, label="Total amount repaid"),
            trac.F("region", trac.STRING, label="Customer home region", categorical=True),
            trac.F("loan_condition_cat", trac.INTEGER, label="Loan condition category"),
            label="Basic loan parameters")

        currency_data = trac.define_input_table(
            trac.F("ccy_code", trac.STRING, label="Currency code", categorical=True),
            trac.F("spot_date", trac.DATE, label="Spot date for FX rate"),
            trac.F("dollar_rate", trac.DECIMAL, label="Dollar FX rate"))

        return {"customer_loans": customer_loans, "currency_data": currency_data}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        preprocessed = trac.define_output_table(
            trac.F("id", trac.STRING, label="Customer account ID", business_key=True),
            trac.F("some_quantity_x", trac.DECIMAL, label="Some quantity X"),
            label="Generic output")

        return {"preprocessed_data": preprocessed}

    def run_model(self, ctx: trac.TracContext):

        loans = ctx.get_pandas_table("customer_loans")
        currencies = ctx.get_pandas_table("currency_data")

        loans["some_quantity_x"] = loans["loan_amount"] - loans["total_pymnt"]

        preproc = loans[["id", "some_quantity_x"]]

        ctx.put_pandas_table("preprocessed_data", preproc)
