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
import pandas as pd

import pyarrow as pa

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.data as _data


class DataMappingTest(unittest.TestCase):

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
        table = pa.Table.from_pydict({"boolean_field": [True, False, True, False]}, pa.schema([("boolean_field", pa.bool_())]))  # noqa

        conformed = _data.DataConformance.conform_to_schema(table, schema)

        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

    def test_boolean_wrong_type(self):

        schema = DataMappingTest.one_field_schema(_meta.BasicType.BOOLEAN)

        table = pa.Table.from_pydict({"boolean_field": [1.0, 2.0, 3.0, 4.0]})  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        # Coercion does not include parsing string values
        table = pa.Table.from_pydict({"boolean_field": ["True", "False", "True", "False"]})  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_integer_same_type(self):

        schema = DataMappingTest.one_field_schema(_meta.BasicType.INTEGER)
        table = pa.Table.from_pydict({"integer_field": [1, 2, 3, 4]})  # noqa

        conformed = _data.DataConformance.conform_to_schema(table, schema)

        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

    def test_integer_width(self):

        s16 = ("f", pa.int16())
        s32 = ("f", pa.int32())
        s64 = ("f", pa.int64())
        u16 = ("f", pa.uint16())
        u32 = ("f", pa.uint32())
        u64 = ("f", pa.uint64())

        schema = pa.schema([s64])

        table = pa.Table.from_pydict({"f": [2 ** 31 - 1]}, pa.schema([s32]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 15 - 1]}, pa.schema([s16]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([s32])

        table = pa.Table.from_pydict({"f": [2 ** 31]}, pa.schema([s64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [2 ** 31 - 1]}, pa.schema([s64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 15 - 1]}, pa.schema([s16]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([s16])

        table = pa.Table.from_pydict({"f": [2 ** 15]}, pa.schema([s64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [2 ** 31 - 1]}, pa.schema([s32]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [2 ** 15 - 1]}, pa.schema([s64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 15 - 1]}, pa.schema([s32]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([u32])

        table = pa.Table.from_pydict({"f": [2 ** 32]}, pa.schema([u64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [2 ** 32 - 1]}, pa.schema([u64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 16 - 1]}, pa.schema([u16]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

    def test_integer_signedness(self):

        s32 = ("f", pa.int32())
        s64 = ("f", pa.int64())
        u32 = ("f", pa.uint32())
        u64 = ("f", pa.uint64())

        schema = pa.schema([s64])

        table = pa.Table.from_pydict({"f": [2 ** 32 - 1]}, pa.schema([u32]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 63 - 1]}, pa.schema([u64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 63 + 1]}, pa.schema([u64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([u64])

        table = pa.Table.from_pydict({"f": [-1]}, pa.schema([s32]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [-1]}, pa.schema([s64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_integer_wrong_type(self):

        s64 = ("f", pa.int64())
        u32 = ("f", pa.uint32())
        f64 = ("f", pa.float64())
        utf8 = ("f", pa.utf8())

        schema = pa.schema([s64])

        table = pa.Table.from_pydict({"f": [1.0, 2.0, 3.0, 4.9]}, pa.schema([f64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": ["1", "2", "3", "4"]}, pa.schema([utf8]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([u32])

        table = pa.Table.from_pydict({"f": [1.0, 2.0, 3.0, 4.9]}, pa.schema([f64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": ["1", "2", "3", "4"]}, pa.schema([utf8]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_float_same_type(self):

        f32 = ("f", pa.float32())
        f64 = ("f", pa.float64())

        schema = pa.schema([f32])
        table = pa.Table.from_pydict({"f": [1.0, 2.0, 3.0, 4.0]}, pa.schema([f32]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

        schema = pa.schema([f64])
        table = pa.Table.from_pydict({"f": [1.0, 2.0, 3.0, 4.0]}, pa.schema([f64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

    def test_float_width(self):

        f32 = ("f", pa.float32())
        f64 = ("f", pa.float64())

        schema = pa.schema([f64])

        table = pa.Table.from_pydict({"f": [1.0, 2.0, 3.0, 4.0]}, pa.schema([f32]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([f32])

        table = pa.Table.from_pydict({"f": [2.0 ** 100]}, pa.schema([f64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_float_from_integer_types(self):

        f32 = ("f", pa.float32())
        f64 = ("f", pa.float64())

        s64 = ("f", pa.int64())
        u64 = ("f", pa.uint64())
        s8 = ("f", pa.int8())
        u8 = ("f", pa.uint8())

        schema = pa.schema([f64])

        table = pa.Table.from_pydict({"f": [2 ** 53]}, pa.schema([s64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [255]}, pa.schema([u8]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([f32])

        table = pa.Table.from_pydict({"f": [2 ** 24]}, pa.schema([u64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [-128]}, pa.schema([s8]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

    def test_float_wrong_type(self):

        f32 = ("f", pa.float32())
        f64 = ("f", pa.float64())
        dec = ("f", pa.decimal128(6, 3))
        utf8 = ("f", pa.utf8())

        schema = pa.schema([f32])

        table = pa.Table.from_pydict({"f": [decimal.Decimal(1.0)]}, pa.schema([dec]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": ["1.0"]}, pa.schema([utf8]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([f64])

        table = pa.Table.from_pydict({"f": [decimal.Decimal(1.0)]}, pa.schema([dec]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": ["1.0"]}, pa.schema([utf8]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_decimal_same_type(self):

        dec1 = ("f", pa.decimal128(6, 3))
        dec2 = ("f", pa.decimal128(38, 12))
        dec3 = ("f", pa.decimal256(50, 15))

        schema = pa.schema([dec1])
        table = pa.Table.from_pydict({"f": [decimal.Decimal(1.0)]}, pa.schema([dec1]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

        schema = pa.schema([dec2])
        table = pa.Table.from_pydict({"f": [decimal.Decimal(1.0)]}, pa.schema([dec2]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

        schema = pa.schema([dec3])
        table = pa.Table.from_pydict({"f": [decimal.Decimal(1.0)]}, pa.schema([dec3]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

    def test_decimal_precision_and_scale_128(self):

        dec1 = ("f", pa.decimal128(8, 2))
        dec2 = ("f", pa.decimal128(10, 4))
        dec3 = ("f", pa.decimal128(8, 4))

        self._test_decimal_precision_and_scale(dec1, dec2, dec3)

    def test_decimal_precision_and_scale_256(self):

        dec1 = ("f", pa.decimal256(8, 2))
        dec2 = ("f", pa.decimal256(10, 4))
        dec3 = ("f", pa.decimal256(8, 4))

        self._test_decimal_precision_and_scale(dec1, dec2, dec3)

    def _test_decimal_precision_and_scale(self, dec1, dec2, dec3):

        schema = pa.schema([dec1])

        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.0001")]}, pa.schema([dec2]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [decimal.Decimal("1000.0001")]}, pa.schema([dec3]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([dec2])

        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.01")]}, pa.schema([dec1]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [decimal.Decimal("1000.0001")]}, pa.schema([dec3]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([dec3])

        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.01")]}, pa.schema([dec1]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.0001")]}, pa.schema([dec2]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_decimal_128_256(self):

        d128_1 = ("f", pa.decimal128(8, 2))
        d128_2 = ("f", pa.decimal128(8, 4))
        d256_1 = ("f", pa.decimal256(8, 2))
        d256_2 = ("f", pa.decimal256(8, 4))

        # Allow conversion 128 <-> 256 if precision and scale are the same

        schema = pa.schema([d128_1])
        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.01")]}, pa.schema([d256_1]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([d256_1])
        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.01")]}, pa.schema([d128_1]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        # Allow conversion 128 <-> 256 if the target can hold larger numbers (max_exp = precision - scale - 1)

        schema = pa.schema([d128_1])
        table = pa.Table.from_pydict({"f": [decimal.Decimal("1000.01")]}, pa.schema([d256_2]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([d256_1])
        table = pa.Table.from_pydict({"f": [decimal.Decimal("1000.01")]}, pa.schema([d128_2]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        # Do not allow conversion in either direction if the source can hold larger numbers

        schema = pa.schema([d128_2])
        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.01")]}, pa.schema([d256_1]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([d256_2])
        table = pa.Table.from_pydict({"f": [decimal.Decimal("100000.01")]}, pa.schema([d128_1]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_decimal_from_numeric_types(self):

        dec1 = ("f", pa.decimal128(38, 12))
        dec2 = ("f", pa.decimal256(50, 15))

        dec3 = ("f", pa.decimal128(6, 2))
        dec4 = ("f", pa.decimal256(6, 2))

        schema = pa.schema([dec1])

        table = pa.Table.from_pydict({"f": [2.0 ** 32, 1.0 + 1 * 2.0 ** -15]}, pa.schema([("f", pa.float64())]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 32, -1 * 2 ** 32]}, pa.schema([("f", pa.int64())]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([dec2])

        table = pa.Table.from_pydict({"f": [2.0 ** 32, 1.0 + 1 * 2.0 ** -15]}, pa.schema([("f", pa.float64())]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        table = pa.Table.from_pydict({"f": [2 ** 32, -1 * 2 ** 32]}, pa.schema([("f", pa.int64())]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([dec3])

        table = pa.Table.from_pydict({"f": [2.0 ** 32, -1 * 2.0 ** 32]}, pa.schema([("f", pa.float64())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [2 ** 32, -1 * 2 ** 32]}, pa.schema([("f", pa.int64())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([dec4])

        table = pa.Table.from_pydict({"f": [2.0 ** 32, -1 * 2.0 ** 32]}, pa.schema([("f", pa.float64())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [2 ** 32, -1 * 2 ** 32]}, pa.schema([("f", pa.int64())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_decimal_wrong_type(self):

        dec1 = ("f", pa.decimal128(38, 12))
        dec2 = ("f", pa.decimal256(50, 15))

        schema = pa.schema([dec1])

        table = pa.Table.from_pydict({"f": ["1.0", "2.0"]}, pa.schema([("f", pa.utf8())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([("f", pa.date32())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([dec2])

        table = pa.Table.from_pydict({"f": ["1.0", "2.0"]}, pa.schema([("f", pa.utf8())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([("f", pa.date32())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_string_same_type(self):

        str1 = ("f", pa.utf8())
        str2 = ("f", pa.large_utf8())

        schema = pa.schema([str1])
        table = pa.Table.from_pydict({"f": ["hello", "world"]}, pa.schema([str1]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

        schema = pa.schema([str2])
        table = pa.Table.from_pydict({"f": ["hello", "world"]}, pa.schema([str2]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

    def test_string_small_to_large(self):

        str1 = ("f", pa.utf8())
        str2 = ("f", pa.large_utf8())

        # utf8 -> large utf8 is allowed
        schema = pa.schema([str2])
        table = pa.Table.from_pydict({"f": ["hello", "world"]}, pa.schema([str1]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        # large utf8 -> utf8 is not allowed
        schema = pa.schema([str1])
        table = pa.Table.from_pydict({"f": ["hello", "world"]}, pa.schema([str2]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_string_wrong_type(self):

        str1 = ("f", pa.utf8())
        str2 = ("f", pa.large_utf8())

        schema = pa.schema([str1])

        table = pa.Table.from_pydict({"f": [1, 2, 3, 4]}, pa.schema([("f", pa.int64())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([("f", pa.date32())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([str2])

        table = pa.Table.from_pydict({"f": [1, 2, 3, 4]}, pa.schema([("f", pa.int64())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([("f", pa.date32())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_date_same_type(self):

        d32 = ("f", pa.date32())
        d64 = ("f", pa.date64())

        schema = pa.schema([d32])
        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([d32]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

        schema = pa.schema([d64])
        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([d64]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)
        self.assertEqual(table.column(0), conformed.column(0))

    def test_date_32_64(self):

        d32 = ("f", pa.date32())
        d64 = ("f", pa.date64())

        # Up-cast 32-bit day to 64-bit date (millisecond) is allowed, no loss of data or precision
        schema = pa.schema([d64])
        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([d32]))  # noqa
        conformed = _data.DataConformance.conform_to_schema(table, schema)
        self.assertEqual(schema, conformed.schema)

        # Down-cast is not allowed, could lose data and/or precision
        schema = pa.schema([d32])
        table = pa.Table.from_pydict({"f": [dt.date(2000, 1, 1)]}, pa.schema([d64]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_date_from_pandas(self):

        d32 = ("f", pa.date32())
        d64 = ("f", pa.date64())

        df = pd.DataFrame({"f": pd.to_datetime([dt.date(2001, 1, 1)])})

        # Conform should be applied inside DataMapping, allowing for conversion of NumPy native dates datetime64[ns]

        schema = pa.schema([d32])
        conformed = _data.DataMapping.pandas_to_arrow(df, schema)
        self.assertEqual(schema, conformed.schema)

        schema = pa.schema([d64])
        conformed = _data.DataMapping.pandas_to_arrow(df, schema)
        self.assertEqual(schema, conformed.schema)

    def test_date_wrong_type(self):

        d32 = ("f", pa.date32())
        d64 = ("f", pa.date64())

        schema = pa.schema([d32])

        table = pa.Table.from_pydict({"f": ["2000-01-01"]}, pa.schema([("f", pa.utf8())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [1, 2, 3, 4]}, pa.schema([("f", pa.int32())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        # By default, do not down-cast timestamps as dates (only allowed as a special case for Pandas data)
        table = pa.Table.from_pydict({"f": [dt.datetime(2001, 1, 1)]}, pa.schema([("f", pa.timestamp("ns"))]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        schema = pa.schema([d64])

        table = pa.Table.from_pydict({"f": ["2000-01-01"]}, pa.schema([("f", pa.utf8())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        table = pa.Table.from_pydict({"f": [1, 2, 3, 4]}, pa.schema([("f", pa.int64())]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

        # By default, do not down-cast timestamps as dates (only allowed as a special case for Pandas data)
        table = pa.Table.from_pydict({"f": [dt.datetime(2001, 1, 1)]}, pa.schema([("f", pa.timestamp("ns"))]))  # noqa
        self.assertRaises(_ex.EDataConformance, lambda: _data.DataConformance.conform_to_schema(table, schema))

    def test_timestamp_same_type(self):
        self.fail()

    def test_timestamp_wrong_type(self):
        self.fail()
