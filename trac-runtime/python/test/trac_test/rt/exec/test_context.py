#  Copyright 2021 Accenture Global Solutions Limited
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
import unittest

import trac.rt.api as _api
import trac.rt.exceptions as _ex

from trac.rt.exec.context import TracContextImpl


class _TestModel(_api.TracModel):

    def define_parameters(self) -> tp.Dict[str, _api.ModelParameter]:

        return _api.define_parameters(

            _api.P("eur_usd_rate", _api.BasicType.FLOAT,
                   label="EUR/USD spot rate for reporting"),

            _api.P("default_weighting", _api.BasicType.FLOAT,
                   label="Weighting factor applied to the profit/loss of a defaulted loan"),

            _api.P("filter_defaults", _api.BasicType.BOOLEAN,
                   label="Exclude defaulted loans from the calculation",
                   default_value=False))

    def define_inputs(self) -> tp.Dict[str, _api.TableDefinition]:

        customer_loans = _api.define_table(
            _api.F("id", _api.BasicType.STRING, label="Customer account ID", business_key=True),
            _api.F("loan_amount", _api.BasicType.DECIMAL, label="Principal loan amount", format_code="CCY:EUR"),
            _api.F("total_pymnt", _api.BasicType.DECIMAL, label="Total amount repaid", format_code="CCY:EUR"),
            _api.F("region", _api.BasicType.STRING, label="Customer home region", categorical=True),
            _api.F("loan_condition_cat", _api.BasicType.INTEGER, label="Loan condition category", categorical=True))

        return {"customer_loans": customer_loans}

    def define_outputs(self) -> tp.Dict[str, _api.TableDefinition]:

        profit_by_region = _api.define_table(
            _api.F("region", _api.BasicType.STRING, label="Customer home region", categorical=True),
            _api.F("gross_profit", _api.BasicType.DECIMAL, label="Total gross profit", format_code="CCY:USD"))

        return {"profit_by_region": profit_by_region}

    def run_model(self, ctx: _api.TracContext):
        pass


_test_model_def = _api.ModelDefinition(  # noqa
    language="python",
    repository="trac_integrated",
    entryPoint=f"{_TestModel.__module__}.{_TestModel.__name__}",

    path="",
    repositoryVersion="",
    input=_TestModel().define_inputs(),
    output=_TestModel().define_outputs(),
    param=_TestModel().define_parameters(),
    overlay=False,
    schemaUnchanged=False)


class TracContextTest(unittest.TestCase):

    """
    Test core functionality and error handling in the main TracContext class
    This test does not cover data conformity, which is covered by a separate test case
    TracContext is a fairly thin layer over a set of get/put operations,
    so the tests mainly cover runtime validation of parameters to those operations
    """

    def setUp(self):
        self.ctx = TracContextImpl(_test_model_def, _TestModel, {}, {})  # todo data and params

    # Getting params

    def test_get_parameter_ok(self):
        pass

    def test_get_parameter_types(self):
        pass

    def test_get_parameter_name_is_null(self):

        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter(None))  # noqa

    def test_get_parameter_name_invalid(self):

        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter(""))
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter("test:var"))
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter("test-var"))
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter("$xyz"))
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter("{xyz}"))
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter("xyz abc"))
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter("中文"))

    def test_get_parameter_name_reserved(self):
        pass

    def test_get_parameter_unknown(self):
        pass

    # Getting tables

    def test_get_schema_ok(self):
        pass

    def test_get_schema_dynamic(self):
        pass

    def test_get_table_pandas_ok(self):
        pass

    def test_get_table_dynamic_schema(self):
        pass

    def test_get_table_pandas_conversion(self):
        pass

    def test_get_table_output_before_put(self):
        pass

    def test_get_table_name_is_null(self):
        pass

    def test_get_table_name_invalid(self):
        pass

    def test_get_table_name_reserved(self):
        pass

    def test_get_table_unknown(self):
        pass

    # Putting tables

    def test_put_schema_ok(self):
        pass

    def test_put_schema_not_dynamic(self):
        pass

    def test_put_schema_for_input(self):
        pass

    def put_table_pandas_ok(self):
        pass

    def put_table_pandas_dynamic_schema(self):
        pass

    def put_table_pandas_null(self):
        pass

    def put_table_pandas_not_a_dataframe(self):
        pass

    def put_table_pandas_no_rows(self):
        pass

    def test_put_table_name_is_null(self):
        pass

    def test_put_table_name_invalid(self):
        pass

    def test_put_table_name_reserved(self):
        pass

    def test_put_table_unknown(self):
        pass

    # Misc extra tests

    def test_get_log(self):
        pass
