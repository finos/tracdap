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
import logging
import typing as tp
import unittest
import decimal
import datetime
import pandas as pd

import trac.rt.api as _api
import trac.rt.exceptions as _ex
import trac.rt.impl.data as _data

from trac.rt.exec.context import TracContextImpl


class _TestModel(_api.TracModel):

    def define_parameters(self) -> tp.Dict[str, _api.ModelParameter]:

        return _api.declare_parameters(

            _api.P("boolean_param", _api.BasicType.BOOLEAN,
                   label="A boolean param",
                   default_value=False),

            _api.P("integer_param", _api.BasicType.INTEGER,
                   label="An integer param",
                   default_value=False),

            _api.P("float_param", _api.BasicType.FLOAT,
                   label="A float param",
                   default_value=False),

            _api.P("decimal_param", _api.BasicType.DECIMAL,
                   label="A decimal param",
                   default_value=False),

            _api.P("string_param", _api.BasicType.STRING,
                   label="A string param",
                   default_value=False),

            _api.P("date_param", _api.BasicType.DATE,
                   label="A date param",
                   default_value=False),

            _api.P("datetime_param", _api.BasicType.DATETIME,
                   label="A datetime param",
                   default_value=False))

    def define_inputs(self) -> tp.Dict[str, _api.ModelInputSchema]:

        customer_loans = _api.declare_input_table(
            _api.F("id", _api.BasicType.STRING, label="Customer account ID", business_key=True),
            _api.F("loan_amount", _api.BasicType.DECIMAL, label="Principal loan amount", format_code="CCY:EUR"),
            _api.F("total_pymnt", _api.BasicType.DECIMAL, label="Total amount repaid", format_code="CCY:EUR"),
            _api.F("region", _api.BasicType.STRING, label="Customer home region", categorical=True),
            _api.F("loan_condition_cat", _api.BasicType.INTEGER, label="Loan condition category", categorical=True))

        return {"customer_loans": customer_loans}

    def define_outputs(self) -> tp.Dict[str, _api.ModelOutputSchema]:

        profit_by_region = _api.declare_output_table(
            _api.F("region", _api.BasicType.STRING, label="Customer home region", categorical=True),
            _api.F("gross_profit", _api.BasicType.DECIMAL, label="Total gross profit", format_code="CCY:USD"))

        return {"profit_by_region": profit_by_region}

    def run_model(self, ctx: _api.TracContext):
        pass


_test_model_def = _api.ModelDefinition(  # noqa

    language="python",
    repository="trac_integrated",
    entryPoint=f"{_TestModel.__module__}.{_TestModel.__name__}",

    parameters=_TestModel().define_parameters(),
    inputs=_TestModel().define_inputs(),
    outputs=_TestModel().define_outputs())


class TracContextTest(unittest.TestCase):

    """
    Test core functionality and error handling in the main TracContext class
    This test does not cover data conformity, which is covered by a separate test case
    TracContext is a fairly thin layer over a set of get/put operations,
    so the tests mainly cover runtime validation of parameters to those operations
    """

    BOOLEAN_PARAM_VALUE = True
    INTEGER_PARAM_VALUE = 42
    FLOAT_PARAM_VALUE = 3.14159265358979
    DECIMAL_PARAM_VALUE = decimal.Decimal("3.141592653589793238462643383279")
    STRING_PARAM_VALUE = "Slartibartfast"
    DATE_PARAM_VALUE = datetime.date(2021, 6, 21)
    DATETIME_PARAM_VALUE = datetime.datetime(2021, 6, 21, 13, 0, 0)

    INVALID_IDENTIFIERS = ["", "test:var", "test-var", "$xyz", "{xyz}", "xyz abc", "中文"]

    LOANS_DATA = pd.DataFrame({
        "id": ["acc001", "acc002"],
        "loan_amount": [decimal.Decimal("1500000.00"), decimal.Decimal("1349374.83")],
        "total_pymnt": [decimal.Decimal("1650000.00"), decimal.Decimal("1563864.37")],
        "region": ["LONDON", "SOUTH_WEST"],
        "loan_condition_cat": [1, 4]
    })

    RESULT_DATA = pd.DataFrame({
        "region": ["LONDON", "SOUTH_WEST"],
        "gross_profit": [decimal.Decimal("150000.00"), decimal.Decimal("214489.54")]
    })

    def setUp(self):

        params = {
            "boolean_param": self.BOOLEAN_PARAM_VALUE,
            "integer_param": self.INTEGER_PARAM_VALUE,
            "float_param": self.FLOAT_PARAM_VALUE,
            "decimal_param": self.DECIMAL_PARAM_VALUE,
            "string_param": self.STRING_PARAM_VALUE,
            "date_param": self.DATE_PARAM_VALUE,
            "datetime_param": self.DATETIME_PARAM_VALUE
        }

        customer_loans_schema = _test_model_def.inputs.get("customer_loans").schema
        customer_loans_delta0 = _data.DataItem(pandas=self.LOANS_DATA)
        customer_loans_parts = {_data.DataPartKey.for_root(): [customer_loans_delta0]}
        customer_loans_view = _data.DataView(customer_loans_schema, customer_loans_parts)

        profit_by_region_schema = _test_model_def.outputs.get("profit_by_region").schema
        profit_by_region_view = _data.DataView(profit_by_region_schema, {})

        data_ctx = {
            "customer_loans": customer_loans_view,
            "profit_by_region": profit_by_region_view
        }

        self.ctx = TracContextImpl(_test_model_def, _TestModel, params, data_ctx)

    # Getting params

    def test_get_parameter_ok(self):

        string_param = self.ctx.get_parameter("string_param")
        self.assertEqual(string_param, self.STRING_PARAM_VALUE)

    def test_get_parameter_types(self):
        
        types_to_check = [
            ("boolean_param", self.BOOLEAN_PARAM_VALUE, bool),
            ("integer_param", self.INTEGER_PARAM_VALUE, int),
            ("float_param", self.FLOAT_PARAM_VALUE, float),
            ("decimal_param", self.DECIMAL_PARAM_VALUE, decimal.Decimal),
            ("string_param", self.STRING_PARAM_VALUE, str),
            ("date_param", self.DATE_PARAM_VALUE, datetime.date),
            ("datetime_param", self.DATETIME_PARAM_VALUE, datetime.datetime)]

        for param_name, expected_value, expected_type in types_to_check:

            param_value = self.ctx.get_parameter(param_name)
            self.assertIsInstance(param_value, expected_type)
            self.assertEqual(param_value, expected_value)

    def test_get_parameter_name_is_null(self):

        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter(None))  # noqa

    def test_get_parameter_name_invalid(self):

        for identifier in self.INVALID_IDENTIFIERS:
            self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter(identifier))

    def test_get_parameter_unknown(self):

        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_parameter("unknown_param"))

    # Getting tables

    def test_get_schema_ok(self):

        schema = self.ctx.get_schema("customer_loans")

        self.assertIsInstance(schema, _api.SchemaDefinition)
        self.assertEqual(schema.schemaType, _api.SchemaType.TABLE)
        self.assertIsInstance(schema.table, _api.TableSchema)
        self.assertEqual(len(schema.table.fields), 5)

    def test_get_schema_for_output(self):

        schema = self.ctx.get_schema("profit_by_region")

        self.assertIsInstance(schema, _api.SchemaDefinition)
        self.assertEqual(schema.schemaType, _api.SchemaType.TABLE)
        self.assertIsInstance(schema.table, _api.TableSchema)
        self.assertEqual(len(schema.table.fields), 2)

    def test_get_pandas_table_ok(self):

        customer_loans = self.ctx.get_pandas_table("customer_loans")

        self.assertIsInstance(customer_loans, pd.DataFrame)
        self.assertEqual(len(customer_loans.columns), 5)
        self.assertEqual(len(customer_loans), 2)

    def test_get_pandas_table_for_output(self):

        # Trying to get an output dataset before it has been put is a validation error
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_pandas_table("profit_by_region"))

        self.ctx.put_pandas_table("profit_by_region", self.RESULT_DATA)
        result_data = self.ctx.get_pandas_table("profit_by_region")

        self.assertIsInstance(result_data, pd.DataFrame)
        self.assertEqual(len(result_data.columns), 2)
        self.assertEqual(len(result_data), 2)

    def test_get_dataset_name_is_null(self):

        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_schema(None))  # noqa
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_pandas_table(None))  # noqa

        # Add other get methods here as they are implemented, e.g. get_pyspark_table

    def test_get_dataset_name_invalid(self):

        for identifier in self.INVALID_IDENTIFIERS:

            self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_schema(identifier))
            self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_pandas_table(identifier))

            # Add other get methods here as they are implemented, e.g. get_pyspark_table

    def test_get_dataset_unknown(self):

        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_schema("unknown_dataset"))
        self.assertRaises(_ex.ERuntimeValidation, lambda: self.ctx.get_pandas_table("unknown_dataset"))

        # Add other get methods here as they are implemented, e.g. get_pyspark_table

    # Putting tables

    def test_put_pandas_table_ok(self):

        self.ctx.put_pandas_table("profit_by_region", self.RESULT_DATA)

        result_data = self.ctx.get_pandas_table("profit_by_region")
        self.assertIsInstance(result_data, pd.DataFrame)
        self.assertEqual(len(result_data.columns), 2)
        self.assertEqual(len(result_data), 2)

    def test_put_pandas_table_empty(self):

        # Corner case: Empty result sets are allowed

        empty_results = pd.DataFrame({"region": [], "gross_profit": []})
        self.ctx.put_pandas_table("profit_by_region", empty_results)

        result_data = self.ctx.get_pandas_table("profit_by_region")
        self.assertIsInstance(result_data, pd.DataFrame)
        self.assertEqual(len(result_data.columns), 2)
        self.assertEqual(len(result_data), 0)

    def test_put_pandas_table_twice(self):

        self.ctx.put_pandas_table("profit_by_region", self.RESULT_DATA)

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.put_pandas_table("profit_by_region", self.RESULT_DATA))

    def test_put_pandas_table_for_input(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.put_pandas_table("customer_loans", self.RESULT_DATA))

    def test_put_pandas_table_null(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.put_pandas_table("profit_by_region", None))  # noqa

    def test_put_pandas_table_not_a_dataframe(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.put_pandas_table("profit_by_region", object()))  # noqa

    def test_put_dataset_name_is_null(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.put_pandas_table(None, self.RESULT_DATA))  # noqa

        # Add other put methods here as they are implemented, e.g. put_pyspark_table

    def test_put_dataset_name_invalid(self):

        for identifier in self.INVALID_IDENTIFIERS:

            self.assertRaises(
                _ex.ERuntimeValidation,
                lambda: self.ctx.put_pandas_table(identifier, self.RESULT_DATA))

            # Add other put methods here as they are implemented, e.g. put_pyspark_table

    def test_put_dataset_unknown(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.put_pandas_table("unknown_output", self.RESULT_DATA))

        # Add other put methods here as they are implemented, e.g. put_pyspark_table

    # Misc extra tests

    def test_get_log(self):

        log = self.ctx.log()
        self.assertIsInstance(log, logging.Logger)

        with self.assertLogs(log.name, logging.INFO):
            log.info("Model logger test")

    """
    Functionality not available yet:
    
    def test_get_schema_dynamic(self):
    def test_get_table_dynamic_schema(self):
    def test_get_table_pandas_conversion(self):
    
    def test_put_schema_ok(self):
    def test_put_schema_for_input(self):
    def test_put_schema_not_dynamic(self):
    def put_table_pandas_dynamic_schema(self):
    """
