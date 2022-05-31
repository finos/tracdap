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

        return trac.declare_parameters(
            trac.P("param_1", trac.INTEGER, "First parameter"),
            trac.P("param_2", trac.DATE, "Second parameter", default_value=dt.date(2001, 1, 1)))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        customer_loans = trac.declare_input_table(
            trac.F("id", trac.BasicType.STRING, label="Customer account ID", business_key=True),
            trac.F("loan_amount", trac.BasicType.DECIMAL, label="Principal loan amount", format_code="CCY:EUR"),
            trac.F("total_pymnt", trac.BasicType.DECIMAL, label="Total amount repaid", format_code="CCY:EUR"),
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("loan_condition_cat", trac.BasicType.INTEGER, label="Loan condition category", categorical=True))

        currency_data = trac.declare_input_table(
            trac.F("ccy_code", trac.BasicType.STRING, label="Currency code", categorical=True),
            trac.F("spot_date", trac.BasicType.DATE, label="Spot date for FX rate"),
            trac.F("dollar_rate", trac.BasicType.DECIMAL, label="Dollar FX rate", format_code="CCY:USD"))

        return {"customer_loans": customer_loans, "currency_data": currency_data}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        preprocessed = trac.declare_output_table(
            trac.F("id", trac.BasicType.STRING, label="Customer account ID", business_key=True),
            trac.F("some_quantity_x", trac.BasicType.DECIMAL, label="Some quantity X", format_code="CCY:EUR"))

        return {"preprocessed_data": preprocessed}

    def run_model(self, ctx: trac.TracContext):

        loans = ctx.get_pandas_table("customer_loans")
        currencies = ctx.get_pandas_table("currency_data")

        loans["some_quantity_x"] = loans["loan_amount"] - loans["total_pymnt"]

        preproc = loans[["id", "some_quantity_x"]]

        ctx.put_pandas_table("preprocessed_data", preproc)
