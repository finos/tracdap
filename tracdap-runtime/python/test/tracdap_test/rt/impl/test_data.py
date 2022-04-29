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
import unittest
import sys
import math

import pyarrow as pa

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.data as _data


class DataMappingTest(unittest.TestCase):

    @staticmethod
    def sample_schema():

        trac_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema("boolean_field", fieldType=_meta.BasicType.BOOLEAN),
                _meta.FieldSchema("integer_field", fieldType=_meta.BasicType.INTEGER),
                _meta.FieldSchema("float_field", fieldType=_meta.BasicType.FLOAT),
                _meta.FieldSchema("decimal_field", fieldType=_meta.BasicType.DECIMAL),
                _meta.FieldSchema("string_field", fieldType=_meta.BasicType.STRING),
                _meta.FieldSchema("date_field", fieldType=_meta.BasicType.DATE),
                _meta.FieldSchema("datetime_field", fieldType=_meta.BasicType.DATETIME),
            ]))

        return _types.trac_to_arrow_schema(trac_schema)

    @staticmethod
    def sample_data():

        return {
            "boolean_field": [True, False, True, False],
            "integer_field": [1, 2, 3, 4],
            "float_field": [1.0, 2.0, 3.0, 4.0],
            "decimal_field": [decimal.Decimal(1.0), decimal.Decimal(2.0), decimal.Decimal(3.0), decimal.Decimal(4.0)],
            "string_field": ["hello", "world", "what's", "up"],
            "date_field": [dt.date(2000, 1, 1), dt.date(2000, 1, 2), dt.date(2000, 1, 3), dt.date(2000, 1, 4)],
            "datetime_field": [
                dt.datetime(2000, 1, 1, 0, 0, 0), dt.datetime(2000, 1, 2, 1, 1, 1),
                dt.datetime(2000, 1, 3, 2, 2, 2), dt.datetime(2000, 1, 4, 3, 3, 3)]
        }

    @staticmethod
    def one_field_schema(field_type: _meta.BasicType):

        field_name = f"{field_type.name.lower()}_field"

        trac_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema(field_name, fieldType=field_type)]))

        return _types.trac_to_arrow_schema(trac_schema)

    def test_round_trip_basic(self):

        sample_schema = self.sample_schema()
        sample_data = self.sample_data()

        table = pa.Table.from_pydict(sample_data, sample_schema)  # noqa
        df = _data.DataMapping.arrow_to_pandas(table)

        # TODO: Check schema on DF, i.e. at midway point?

        rt = _data.DataMapping.pandas_to_arrow(df, self.sample_schema())

        self.assertEqual(sample_schema, rt.schema)
        self.assertEqual(table, rt)

    def test_round_trip_nulls(self):

        sample_schema = self.sample_schema()
        sample_data = self.sample_data()

        for col, values in sample_data.items():
            values[0] = None

        table = pa.Table.from_pydict(sample_data, sample_schema)  # noqa
        df = _data.DataMapping.arrow_to_pandas(table)

        # TODO: Check schema on DF, i.e. at midway point?

        rt = _data.DataMapping.pandas_to_arrow(df, self.sample_schema())

        self.assertEqual(sample_schema, rt.schema)
        self.assertEqual(table, rt)

    def test_edge_cases_integer(self):

        schema = self.one_field_schema(_meta.BasicType.INTEGER)
        table = pa.Table.from_pydict({"integer_field": [  # noqa
            0,
            sys.maxsize,
            -sys.maxsize - 1
        ]}, schema)

        df = _data.DataMapping.arrow_to_pandas(table)
        rt = _data.DataMapping.pandas_to_arrow(df, schema)

        self.assertEqual(schema, rt.schema)
        self.assertEqual(table, rt)

    def test_edge_cases_float(self):

        # It may be helpful to check for / prohibit inf and -inf in some places, e.g. model outputs
        # But still the data mapping should handle these values correctly if they are present

        schema = self.one_field_schema(_meta.BasicType.FLOAT)
        table = pa.Table.from_pydict({"float_field": [  # noqa
            0.0,
            sys.float_info.min,
            sys.float_info.max,
            sys.float_info.epsilon,
            -sys.float_info.epsilon,
            math.inf,
            -math.inf
        ]}, schema)

        df = _data.DataMapping.arrow_to_pandas(table)
        rt = _data.DataMapping.pandas_to_arrow(df, schema)

        self.assertEqual(schema, rt.schema)
        self.assertEqual(table, rt)

    def test_edge_cases_float_nan(self):

        # NaN needs special consideration
        # First, math.nan != math.nan, instead NaN must be checked with math.isnan
        # Also, in Pandas NaN and None values are treated the same (by .isna(), .fillna() etc
        # NaNs are converted to None in Pandas -> Arrow conversion
        # Overriding this behaviour would be expensive
        # So for now, just check that the round-trip yields either a NaN or a null

        schema = self.one_field_schema(_meta.BasicType.FLOAT)
        table = pa.Table.from_pydict({"float_field": [math.nan]}, schema)  # noqa

        df = _data.DataMapping.arrow_to_pandas(table)
        rt = _data.DataMapping.pandas_to_arrow(df, schema)

        self.assertEqual(schema, rt.schema)

        nan_value = rt.column(0)[0].as_py()

        self.assertTrue(nan_value is None or math.isnan(nan_value))

    def test_edge_cases_decimal(self):

        # TRAC basic decimal has precision 38, scale 12
        # Should allow for 26 places before the decimal place and 12 after

        schema = self.one_field_schema(_meta.BasicType.DECIMAL)
        table = pa.Table.from_pydict({"decimal_field": [  # noqa
            decimal.Decimal(0.0),
            decimal.Decimal(1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(1.0) / decimal.Decimal(1.0).shift(12),
            decimal.Decimal(-1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(-1.0) / decimal.Decimal(1.0).shift(12)
        ]}, schema)

        df = _data.DataMapping.arrow_to_pandas(table)
        rt = _data.DataMapping.pandas_to_arrow(df, schema)

        self.assertEqual(schema, rt.schema)
        self.assertEqual(table, rt)

    def test_edge_cases_string(self):

        schema = self.one_field_schema(_meta.BasicType.STRING)
        table = pa.Table.from_pydict({"string_field": [  # noqa
            "", " ", "  ", "\t", "\r\n", "  \r\n   ",
            "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@",
            "Olá Mundo", "你好，世界", "Привет, мир", "नमस्ते दुनिया",
            "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |"
        ]}, schema)

        df = _data.DataMapping.arrow_to_pandas(table)
        rt = _data.DataMapping.pandas_to_arrow(df, schema)

        self.assertEqual(schema, rt.schema)
        self.assertEqual(table, rt)

    def test_edge_cases_date(self):

        schema = self.one_field_schema(_meta.BasicType.DATE)
        table = pa.Table.from_pydict({"date_field": [  # noqa
            dt.date(1970, 1, 1),
            dt.date(2000, 1, 1),
            dt.date(2038, 1, 20),
            dt.date.max,
            dt.date.min
        ]}, schema)

        df = _data.DataMapping.arrow_to_pandas(table)
        rt = _data.DataMapping.pandas_to_arrow(df, schema)

        self.assertEqual(schema, rt.schema)
        self.assertEqual(table, rt)

    def test_edge_cases_datetime(self):

        schema = self.one_field_schema(_meta.BasicType.DATETIME)
        table = pa.Table.from_pydict({"datetime_field": [  # noqa
            dt.datetime(1970, 1, 1, 0, 0, 0),
            dt.datetime(2000, 1, 1, 0, 0, 0),
            dt.datetime(2038, 1, 19, 3, 14, 8),
            # Fractional seconds before and after the epoch
            # Test fractions for both positive and negative encoded values
            dt.datetime(1972, 1, 1, 0, 0, 0, 500000),
            dt.datetime(1968, 1, 1, 23, 59, 59, 500000),
            dt.datetime.max,
            dt.datetime.min
        ]}, schema)

        df = _data.DataMapping.arrow_to_pandas(table)
        rt = _data.DataMapping.pandas_to_arrow(df, schema)

        self.assertEqual(schema, rt.schema)
        self.assertEqual(table, rt)


class DataConformanceTest(unittest.TestCase):

    def test_boolean_same_type(self):

        schema = DataMappingTest.one_field_schema(_meta.BasicType.BOOLEAN)
        table = pa.Table.from_pydict({"boolean_field": [True, False, True, False]})  # noqa

        conformed = _data.DataConformance.conform_to_schema(table, schema)

        self.assertEqual(table.column(0), conformed.column(0))

    def test_boolean_wrong_type(self):

        schema = DataMappingTest.one_field_schema(_meta.BasicType.BOOLEAN)

        table = pa.Table.from_pydict({"boolean_field": [1.0, 2.0, 3.0, 4.0]})  # noqa
        self.assertRaises(
            _ex.EDataConformance, lambda:
            _data.DataConformance.conform_to_schema(table, schema))

        # Coercion does not include parsing string values
        table = pa.Table.from_pydict({"boolean_field": ["True", "False", "True", "False"]})  # noqa
        self.assertRaises(
            _ex.EDataConformance, lambda:
            _data.DataConformance.conform_to_schema(table, schema))

    def test_integer_same_type(self):

        schema = DataMappingTest.one_field_schema(_meta.BasicType.INTEGER)
        table = pa.Table.from_pydict({"integer_field": [1, 2, 3, 4]})  # noqa

        conformed = _data.DataConformance.conform_to_schema(table, schema)

        self.assertEqual(table.column(0), conformed.column(0))

    def test_integer_width(self):
        pass

    def test_integer_signedness(self):
        pass

    def test_integer_wrong_type(self):
        pass

    def test_float_same_type(self):
        pass

    def test_float_width(self):
        pass

    def test_float_from_integer_types(self):
        pass

    def test_float_wrong_type(self):
        pass

    def test_decimal_same_type(self):
        pass

    def test_decimal_precision_and_scale(self):
        pass

    def test_decimal_128_256(self):
        pass

    def test_decimal_from_numeric_types(self):
        pass

    def test_decimal_wrong_type(self):
        pass

    def test_string_same_type(self):
        pass

    def test_string_wrong_type(self):
        pass

    def test_date_same_type(self):
        pass

    def test_date_wrong_type(self):
        pass

    def test_timestamp_same_type(self):
        pass

    def test_timestamp_wrong_type(self):
        pass
