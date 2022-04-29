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
import math
import pathlib
import tempfile
import unittest
import sys

import pyarrow as pa

import tracdap.rt.config as _cfg
import tracdap.rt.metadata as _meta
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.storage as _storage
import tracdap.rt.impl.util as _util


_ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()

_TEST_DATA_DIR = _ROOT_DIR \
    .joinpath("tracdap-libs/tracdap-lib-test/src/main/resources/sample_data")

_util.configure_logging()


class DataStorageTestSuite:

    storage: _storage.IDataStorage
    storage_format: str

    assertEqual = unittest.TestCase.assertEqual
    assertTrue = unittest.TestCase.assertTrue

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

        table = pa.Table.from_pydict(self.sample_data(), self.sample_schema())  # noqa

        self.storage.write_table("round_trip_basic", self.storage_format, table)
        rt_table = self.storage.read_table("round_trip_basic", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_round_trip_nulls(self):

        sample_data = self.sample_data()

        for col, values in sample_data.items():
            values[0] = None

        table = pa.Table.from_pydict(sample_data, self.sample_schema())  # noqa

        self.storage.write_table("round_trip_nulls", self.storage_format, table)
        rt_table = self.storage.read_table("round_trip_nulls", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_integer(self):

        schema = self.one_field_schema(_meta.BasicType.INTEGER)
        table = pa.Table.from_pydict({"integer_field": [  # noqa
            0,
            sys.maxsize,
            -sys.maxsize - 1
        ]}, schema)

        self.storage.write_table("edge_cases_integer", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_integer", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_float(self):

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

        self.storage.write_table("edge_cases_float", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_float", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_float_nan(self):

        # For NaN, a special test that checks math.isnan on the round-trip result
        # Because math.nan != math.nan

        schema = self.one_field_schema(_meta.BasicType.FLOAT)
        table = pa.Table.from_pydict({"float_field": [math.nan]}, schema)  # noqa

        self.storage.write_table("edge_cases_float_nan", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_float_nan", self.storage_format, table.schema)

        self.assertTrue(math.isnan(rt_table.column(0)[0].as_py()))

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

        self.storage.write_table("edge_cases_decimal", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_decimal", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_string(self):

        schema = self.one_field_schema(_meta.BasicType.STRING)
        table = pa.Table.from_pydict({"string_field": [  # noqa
            "", " ", "  ", "\t", "\r\n", "  \r\n   ",
            "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@",
            "Olá Mundo", "你好，世界", "Привет, мир", "नमस्ते दुनिया",
            "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |"
        ]}, schema)

        self.storage.write_table("edge_cases_string", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_string", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_date(self):

        schema = self.one_field_schema(_meta.BasicType.DATE)
        table = pa.Table.from_pydict({"date_field": [  # noqa
            dt.date(1970, 1, 1),
            dt.date(2000, 1, 1),
            dt.date(2038, 1, 20),
            dt.date.max,
            dt.date.min
        ]}, schema)

        self.storage.write_table("edge_cases_date", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_date", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

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

        self.storage.write_table("edge_cases_datetime", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_datetime", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)


class LocalStorageTest(DataStorageTestSuite):

    storage_root: tempfile.TemporaryDirectory

    @classmethod
    def make_storage(cls):

        cls.storage_root = tempfile.TemporaryDirectory()

        storage_instance = _cfg.StorageInstance(
            storageType="LOCAL",
            storageProps={"rootPath": cls.storage_root.name})
        storage_config = _cfg.StorageConfig([storage_instance])

        file_storage = _storage.LocalFileStorage(storage_instance)
        data_storage = _storage.CommonDataStorage(storage_config, file_storage)

        return data_storage


class LocalCsvStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "CSV"

        test_lib_storage_instance = _cfg.StorageInstance(
            storageType="LOCAL",
            storageProps={"rootPath": str(_TEST_DATA_DIR)})

        test_lib_storage_config = _cfg.StorageConfig([test_lib_storage_instance])
        test_lib_file_storage = _storage.LocalFileStorage(test_lib_storage_instance)
        test_lib_data_storage = _storage.CommonDataStorage(test_lib_storage_config, test_lib_file_storage)

        cls.test_lib_storage = test_lib_data_storage

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()

    def test_csv_basic(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_basic.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)

    def test_csv_edge_cases(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_edge_cases.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)


class LocalArrowStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "ARROW_FILE"

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()


class LocalParquetStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "PARQUET"

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()
