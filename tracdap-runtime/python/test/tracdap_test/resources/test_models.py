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
import typing as _tp
import typing as tp
import datetime as dt

import tracdap.rt.api as trac
import tracdap.rt.api.experimental as eapi


class TestModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(

            trac.P("boolean_param", trac.BasicType.BOOLEAN,
                   label="A boolean param",
                   default_value=False),

            trac.P("integer_param", trac.BasicType.INTEGER,
                   label="An integer param",
                   default_value=1),

            trac.P("float_param", trac.BasicType.FLOAT,
                   label="A float param",
                   default_value=1.0),

            trac.P("decimal_param", trac.BasicType.DECIMAL,
                   label="A decimal param",
                   default_value=1.0),

            trac.P("string_param", trac.BasicType.STRING,
                   label="A string param",
                   default_value="hello"),

            trac.P("date_param", trac.BasicType.DATE,
                   label="A date param",
                   default_value="2000-01-01"),  # type coercion string -> date

            trac.P("datetime_param", trac.BasicType.DATETIME,
                   label="A datetime param",
                   default_value=dt.datetime.now()))  # Using Python datetime values also works

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        customer_loans = trac.define_input_table(
            trac.F("id", trac.BasicType.STRING, label="Customer account ID", business_key=True),
            trac.F("loan_amount", trac.BasicType.DECIMAL, label="Principal loan amount", format_code="CCY:EUR"),
            trac.F("total_pymnt", trac.BasicType.DECIMAL, label="Total amount repaid", format_code="CCY:EUR"),
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("loan_condition_cat", trac.BasicType.INTEGER, label="Loan condition category", categorical=True))

        return {"customer_loans": customer_loans}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        profit_by_region = trac.define_output_table(
            trac.F("region", trac.BasicType.STRING, label="Customer home region", categorical=True),
            trac.F("gross_profit", trac.BasicType.DECIMAL, label="Total gross profit", format_code="CCY:USD"))

        return {"profit_by_region": profit_by_region}

    def run_model(self, ctx: trac.TracContext):
        pass


class TestImportModel(eapi.TracDataImport):

    def define_parameters(self) -> _tp.Dict[str, trac.ModelParameter]:
        return dict()

    def define_outputs(self) -> _tp.Dict[str, trac.ModelOutputSchema]:
        return dict()

    def run_model(self, ctx: eapi.TracDataContext):
        pass


class TestExportModel(eapi.TracDataExport):

    def define_parameters(self) -> _tp.Dict[str, trac.ModelParameter]:
        return dict()

    def define_inputs(self) -> _tp.Dict[str, trac.ModelInputSchema]:
        return dict()

    def run_model(self, ctx: eapi.TracDataContext):
        pass
