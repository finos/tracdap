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

import unittest
import sys
import math
import decimal
import datetime as dt

import tracdap.rt.api as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.data as _data  # noqa

from tracdap.rt._impl.ext.storage import *  # noqa

import pyarrow as pa


class DataStorageSuite2:

    assertTrue = unittest.TestCase.assertTrue
    assertFalse = unittest.TestCase.assertFalse
    assertEqual = unittest.TestCase.assertEqual
    assertListEqual = unittest.TestCase.assertListEqual
    assertIn = unittest.TestCase.assertIn
    assertNotIn = unittest.TestCase.assertNotIn
    assertRaises = unittest.TestCase.assertRaises
    skipTest = unittest.TestCase.skipTest

    storage: IDataStorageBase[pa.Table, pa.Schema]
    backend: str

    @classmethod
    def sample_data(cls, null_row = None) -> pa.Table:

        raw_data = {
            "integer_field": [1, 2, 3, 4],
            "float_field": [1.0, 2.0, 3.0, 4.0],
            "decimal_field": [decimal.Decimal(1.0), decimal.Decimal(2.0), decimal.Decimal(3.0), decimal.Decimal(4.0)],
            "string_field": ["hello", "world", "what's", "up"],
            "date_field": [dt.date(2000, 1, 1), dt.date(2000, 1, 2), dt.date(2000, 1, 3), dt.date(2000, 1, 4)],
            "datetime_field": [
                dt.datetime(2000, 1, 1, 0, 0, 0), dt.datetime(2000, 1, 2, 1, 1, 1),
                dt.datetime(2000, 1, 3, 2, 2, 2), dt.datetime(2000, 1, 4, 3, 3, 3)]
        }

        if null_row is not None:
            for col, data in raw_data.items():
                data[null_row] = None

        return pa.Table.from_pydict(raw_data, cls.sample_schema())

    @classmethod
    def sample_schema(cls) -> pa.Schema:

        trac_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema("integer_field", fieldType=_meta.BasicType.INTEGER),
                _meta.FieldSchema("float_field", fieldType=_meta.BasicType.FLOAT),
                _meta.FieldSchema("decimal_field", fieldType=_meta.BasicType.DECIMAL),
                _meta.FieldSchema("string_field", fieldType=_meta.BasicType.STRING),
                _meta.FieldSchema("date_field", fieldType=_meta.BasicType.DATE),
                _meta.FieldSchema("datetime_field", fieldType=_meta.BasicType.DATETIME),
            ]))

        return _data.DataMapping.trac_to_arrow_schema(trac_schema)

    @classmethod
    def one_field_schema(cls, field_type: _meta.BasicType) -> pa.Schema:

        field_name = f"{field_type.name.lower()}_field"

        trac_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema(field_name, fieldType=field_type)]))

        return _data.DataMapping.trac_to_arrow_schema(trac_schema)

    def test_has_table(self):

        self.assertFalse(self.storage.has_table("test_has_table"))

        self.storage.create_table("test_has_table", self.sample_schema())

        self.assertTrue(self.storage.has_table("test_has_table"))

    def test_list_tables(self):

        table_names = self.storage.list_tables()
        self.assertNotIn("test_list_tables", table_names)

        self.storage.create_table("test_list_tables", self.sample_schema())
        
        table_names = self.storage.list_tables()
        self.assertIn("test_list_tables", table_names)

    def test_create_table_ok(self):
        
        self.assertFalse(self.storage.has_table("test_create_table_ok"))

        self.storage.create_table("test_create_table_ok", self.sample_schema())

        self.assertTrue(self.storage.has_table("test_create_table_ok"))

    def test_create_table_bad_name(self):

        self.assertRaises(
            _ex.EStorageRequest, lambda: 
            self.storage.create_table("$$$-not-allowed", self.sample_schema()))

    def test_create_table_bad_schema(self):

        bad_raw_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema("integer_field", fieldType=_meta.BasicType.INTEGER),
                _meta.FieldSchema("integer_field", fieldType=_meta.BasicType.INTEGER),
                _meta.FieldSchema("integer_field", fieldType=_meta.BasicType.INTEGER)
            ]))

        bad_schema = _data.DataMapping.trac_to_arrow_schema(bad_raw_schema)

        self.assertRaises(
            _ex.EStorageRequest, lambda: 
            self.storage.create_table("test_create_table_bad_schema", bad_schema))

    def test_create_table_already_exists(self):

        self.storage.create_table("test_create_table_already_exists", self.sample_schema())

        self.assertRaises(
            _ex.EStorageRequest, lambda: 
            self.storage.create_table("test_create_table_already_exists", self.sample_schema()))

    def _do_round_trip(self, table_name, schema, original_data):

        self.storage.create_table(table_name, schema)

        self.storage.write_table(table_name, original_data)
        return self.storage.read_table(table_name)

    def test_round_trip_basic(self):

        original_schema = self.sample_schema()
        original_data = self.sample_data()

        rt_data = self._do_round_trip("test_round_trip_basic", original_schema, original_data)
        
        self.assertEqual(original_data, rt_data)

    def test_round_trip_nulls(self):

        original_schema = self.sample_schema()
        original_data = self.sample_data(null_row = 1)

        rt_data = self._do_round_trip("test_round_trip_nulls", original_schema, original_data)

        self.assertEqual(original_data, rt_data)

    def test_round_trip_no_data(self):

        empty_batches = list()

        original_schema = self.sample_schema()
        original_data = pa.Table.from_batches(empty_batches, original_schema)  # noqa

        rt_data = self._do_round_trip("test_round_trip_no_data", original_schema, original_data)

        self.assertEqual(original_data.column_names, rt_data.column_names)
        self.assertEqual(0, len(original_data))
        self.assertEqual(0, len(rt_data))

    def test_round_trip_boolean(self):

        # Boolean is not handled well in several SQL databases

        original_schema = self.one_field_schema(_meta.BOOLEAN)
        original_data = pa.Table.from_pydict({"boolean_field": [
            True,
            False,
            True,
            False
        ]}, original_schema)

        rt_data = self._do_round_trip("test_round_trip_boolean", original_schema, original_data)

        if pa.types.is_boolean(rt_data.field(0).type):
            self.assertEqual(original_data, rt_data)

        else:
            expected_field = pa.field("boolean_field", pa.int64())
            expected_schema = pa.schema([expected_field])
            expected_data = pa.Table.from_pydict({"boolean_field": [1, 0, 1, 0]}, expected_schema)
            self.assertEqual(expected_data, rt_data)

    def test_edge_cases_integer(self):

        original_schema = self.one_field_schema(_meta.INTEGER)
        original_data = pa.Table.from_pydict({"integer_field": [
            0,
            sys.maxsize,
            -sys.maxsize - 1
        ]}, original_schema)

        rt_data = self._do_round_trip("test_edge_cases_integer", original_schema, original_data)

        self.assertEqual(original_data, rt_data)

    def test_edge_cases_float(self):

        # It may be helpful to check for / prohibit inf and -inf in some places, e.g. model outputs
        # But still the storage layer should handle these values correctly if they are present

        edge_case_values = [
            0.0,
            sys.float_info.min,
            sys.float_info.max,
            sys.float_info.epsilon,
            -sys.float_info.epsilon,
            math.inf,
            -math.inf
        ]

        # Make allowances for backends that don't support some of the edge cases (still test everything else)
        if self.backend in ["mysql", "mariadb", "sqlserver"]:
            edge_case_values.remove(math.inf)
            edge_case_values.remove(-math.inf)

        original_schema = self.one_field_schema(_meta.FLOAT)
        original_data = pa.Table.from_pydict({"float_field": edge_case_values}, original_schema)

        rt_data = self._do_round_trip("test_edge_cases_float", original_schema, original_data)

        self.assertEqual(original_data, rt_data)

    def test_edge_cases_float_nan(self):

        if self.backend in ["mysql", "mariadb", "postgresql", "sqlserver"]:
            self.skipTest(f"Nan is not supported with backend [{self.backend}]")

        # For NaN, a special test that checks math.isnan on the round-trip result
        # Because math.nan != math.nan
        # Also, make sure to keep the distinction between NaN and None

        original_schema = self.one_field_schema(_meta.FLOAT)
        original_data = pa.Table.from_pydict({"float_field": [math.nan]}, original_schema)

        rt_data = self._do_round_trip("test_edge_cases_float_nan", original_schema, original_data)

        self.assertTrue(math.isnan(rt_data.columns[0][0]))

    def test_edge_cases_decimal(self):

        # TRAC basic decimal has precision 38, scale 12
        # Should allow for 26 places before the decimal place and 12 after

        original_schema = self.one_field_schema(_meta.DECIMAL)
        original_data = pa.Table.from_pydict({"decimal_field": [
            decimal.Decimal(0.0),
            decimal.Decimal(1.0) * decimal.Decimal(1.0).shift(20),
            decimal.Decimal(1.0) / decimal.Decimal(1.0).shift(10),
            decimal.Decimal(-1.0) * decimal.Decimal(1.0).shift(20),
            decimal.Decimal(-1.0) / decimal.Decimal(1.0).shift(10)
        ]}, original_schema)

        rt_data = self._do_round_trip("test_edge_cases_decimal", original_schema, original_data)

        self.assertEqual(original_data, rt_data)

    def test_edge_cases_string(self):

        edge_case_values = [
            "", " ", "  ", "\t", "\r\n", "  \r\n   ",
            "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@", "Olá Mundo",
            "你好，世界", "Привет, мир", "नमस्ते दुनिया", "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |"
        ]

        # UTF-8 encoding not working correctly with MariaDB, SQL Server
        if self.backend in ["mariadb", "sqlserver"]:
            edge_case_values = edge_case_values[:-4]

        original_schema = self.one_field_schema(_meta.STRING)
        original_data = pa.Table.from_pydict({"string_field": edge_case_values}, original_schema)

        rt_data = self._do_round_trip("test_edge_cases_string", original_schema, original_data)

        self.assertEqual(original_data, rt_data)

    def test_edge_cases_date(self):

        original_schema = self.one_field_schema(_meta.DATE)
        original_data = pa.Table.from_pydict({"date_field": [
            dt.date(1970, 1, 1),
            dt.date(2000, 1, 1),
            dt.date(2038, 1, 20),
            dt.date.max,
            dt.date.min
        ]}, original_schema)

        rt_data = self._do_round_trip("test_edge_cases_date", original_schema, original_data)

        self.assertEqual(original_data, rt_data)

    # @unittest.skip("Doesn't work on SQLite")
    def test_edge_cases_datetime(self):

        if self.backend in ["mysql", "mariadb"]:
            self.skipTest("MySQL / MariaDB Alchemy backend only supports timestamps between the Unix epoch and the 2038 rollover")

        original_schema = self.one_field_schema(_meta.DATETIME)
        original_data = pa.Table.from_pydict({"datetime_field": [
            dt.datetime(1970, 1, 1, 0, 0, 0),
            dt.datetime(2000, 1, 1, 0, 0, 0),
            dt.datetime(2038, 1, 19, 3, 14, 8),
            # Fractional seconds before and after the epoch
            # Test fractions for both positive and negative encoded values
            dt.datetime(1972, 1, 1, 0, 0, 0, 500000),
            dt.datetime(1968, 1, 1, 23, 59, 59, 500000),
            # dt.datetime.max - dt.timedelta(seconds = 1),
            # dt.datetime.min + dt.timedelta(seconds = 1),
            ]}, original_schema)

        rt_data = self._do_round_trip("test_edge_cases_datetime", original_schema, original_data)

        self.assertEqual(original_data, rt_data)

    def test_read_missing_table(self):

        self.assertRaises(
            _ex.EStorageRequest, lambda:
            self.storage.read_table("test_read_missing_table"))

    def test_write_missing_table(self):

        original_schema = self.sample_schema()
        original_data = pa.Table.from_pydict(self.sample_data(), original_schema)

        self.assertRaises(
            _ex.EStorageRequest, lambda:
            self.storage.write_table("test_write_missing_table", original_data))

    def test_write_wrong_schema(self):

        original_schema = self.sample_schema()
        original_data = pa.Table.from_pydict(self.sample_data(), original_schema)

        missing_field_schema = self.sample_schema().remove(3)
        self.storage.create_table("test_write_wrong_schema", missing_field_schema)

        self.assertRaises(
            _ex.EStorageRequest, lambda:
            self.storage.write_table("test_write_wrong_schema", original_data))

        # MySQL / Alchemy applies forced type-coercion when schema types do not match
        if self.backend not in ["mysql"]:

            wrong_type_schema = self.sample_schema()
            field_3 = wrong_type_schema.field(5)
            wrong_type_schema = wrong_type_schema.remove(5)
            wrong_type_schema = wrong_type_schema.insert(5, pa.field(field_3.name, pa.decimal128(38, 12)))

            self.storage.create_table("test_write_wrong_schema_2", wrong_type_schema)

            self.assertRaises(
                _ex.EStorageRequest, lambda:
                self.storage.write_table("test_write_wrong_schema_2", original_data))

    def test_write_append_ok(self):

        original_schema = self.sample_schema()
        original_data = pa.Table.from_pydict(self.sample_data(), original_schema)

        self.storage.create_table("test_write_append_ok", original_schema)
        self.storage.write_table("test_write_append_ok", original_data)
        self.storage.write_table("test_write_append_ok", original_data)
        rt_data = self.storage.read_table("test_write_append_ok")

        original_data_x2 = pa.concat_tables([original_data, original_data])

        self.assertEqual(original_data_x2, rt_data)

    def test_write_append_key_conflict(self):

        self.skipTest("Primary key mapping is not supported yet")

    def test_native_read_query_ok(self):

        original_schema = self.sample_schema()
        original_data = self.sample_data()

        self.storage.create_table("test_native_read_query_ok", original_schema)
        self.storage.write_table("test_native_read_query_ok", original_data)

        query = "select * from test_native_read_query_ok where integer_field >= :cutoff"  # noqa
        result = self.storage.native_read_query(query, cutoff=3)

        self.assertEqual(original_data.schema, result.schema)
        self.assertEqual(2, len(result))

    def test_native_read_query_missing_table(self):

        query = "select * from test_native_read_query_missing_table where integer_field >= :cutoff"  # noqa

        self.assertRaises(
            _ex.EStorageRequest, lambda:
            self.storage.native_read_query(query, cutoff=3))

    def test_native_read_query_bad_query(self):

        original_schema = self.sample_schema()
        original_data = self.sample_data()

        self.storage.create_table("test_native_read_query_bad_query", original_schema)
        self.storage.write_table("test_native_read_query_bad_query", original_data)

        query = "select * wombat from test_native_read_query_bad_query where integer_field >= :cutoff"  # noqa

        self.assertRaises(
            _ex.EStorageRequest, lambda:
            self.storage.native_read_query(query, cutoff=3))

    def test_native_read_query_bad_parameters(self):

        original_schema = self.sample_schema()
        original_data = self.sample_data()

        self.storage.create_table("test_native_read_query_bad_parameters", original_schema)
        self.storage.write_table("test_native_read_query_bad_parameters", original_data)

        query = "select * from test_native_read_query_bad_parameters where integer_field >= :cutoff"  # noqa

        self.assertRaises(
            _ex.EStorageRequest, lambda:
            self.storage.native_read_query(query, different_parameter=3))

        # MySQL / Alchemy applies forced type-coercion when parameter types do not match
        if self.backend not in ["mysql", "mariadb"]:

            self.assertRaises(
                _ex.EStorageRequest, lambda:
                self.storage.native_read_query(query, cutoff="wombat"))

    def test_native_read_query_no_data(self):

        original_schema = self.sample_schema()
        original_data = self.sample_data()

        self.storage.create_table("test_native_read_query_no_data", original_schema)
        self.storage.write_table("test_native_read_query_no_data", original_data)

        query = "select * from test_native_read_query_no_data where integer_field >= :cutoff"  # noqa
        result = self.storage.native_read_query(query, cutoff=100)

        self.assertEqual(original_data.column_names, result.column_names)
        self.assertEqual(0, len(result))

    def test_native_read_query_write_not_allowed(self):

        table_name = "test_native_read_query_write_not_allowed"

        original_schema = self.one_field_schema(_meta.INTEGER)
        original_data = pa.Table.from_pydict({"integer_field": [1, 2, 3, 4, 5]}, original_schema)

        self.storage.create_table(table_name, original_schema)
        self.storage.write_table(table_name, original_data)

        write_queries = [
            f"insert into {table_name} (integer_field) values (6)",  # noqa
            f"insert into {table_name} (integer_field) select integer_field from {table_name}",  # noqa
            f"update {table_name} set integer_field = 7 where integer_field == 1",  # noqa
            f"delete from {table_name} where integer_field == 2",  # noqa

            f"merge {table_name} as target\n" +
            f"   using {table_name} as source\n" +
            f"   on (target.integer_field = source.integer_field)\n" +
            f"   when matched then update\n" +
            f"   set target.integer_field = source.integer_field + 1"
        ]

        for query in write_queries:
            self.assertRaises(_ex.EStorageRequest, lambda: self.storage.native_read_query(query))

    def test_native_read_query_ddl_not_allowed(self):

        table_name = "test_native_read_query_ddl_not_allowed"

        original_schema = self.one_field_schema(_meta.INTEGER)
        original_data = pa.Table.from_pydict({"integer_field": [1, 2, 3, 4, 5]}, original_schema)

        self.storage.create_table(table_name, original_schema)
        self.storage.write_table(table_name, original_data)

        write_queries = [
            f"create table dodgy_table (integer_field integer)",  # noqa
            f"create table dodgy_table_2 (integer_field integer) as select * from {table_name}",  # noqa
            f"alter table {table_name} add column integer_field_2 integer",  # noqa
            f"drop table {table_name}",
            f"grant all on {table_name} to joe"
        ]

        for query in write_queries:
            self.assertRaises(_ex.EStorageRequest, lambda: self.storage.native_read_query(query))
