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
import pathlib
import tempfile
import unittest
import sys

import pandas as pd

import tracdap.rt.config as _cfg
import tracdap.rt.metadata as _meta
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.data as _data
import tracdap.rt.impl.storage as _storage
import tracdap.rt.impl.util as _util

ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()

TEST_DATA_DIR = ROOT_DIR \
    .joinpath("tracdap-libs/tracdap-lib-test/src/main/resources/sample_data")


_util.configure_logging()


class DataStorageTestSuite:

    storage: _storage.IDataStorage
    storage_format: str

    assertEqual = unittest.TestCase.assertEqual
    assertTrue = unittest.TestCase.assertTrue

    @staticmethod
    def sample_schema():

        return _meta.SchemaDefinition(
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

    @staticmethod
    def sample_data():

        return pd.DataFrame({
            "boolean_field": [True, False, True, False],
            "integer_field": [1, 2, 3, 4],
            "float_field": [1.0, 2.0, 3.0, 4.0],
            "decimal_field": [decimal.Decimal(1.0), decimal.Decimal(2.0), decimal.Decimal(3.0), decimal.Decimal(4.0)],
            "string_field": ["hello", "world", "what's", "up"],
            "date_field": [dt.date(2000, 1, 1), dt.date(2000, 1, 2), dt.date(2000, 1, 3), dt.date(2000, 1, 4)],
            "datetime_field": [
                dt.datetime(2000, 1, 1, 0, 0, 0), dt.datetime(2000, 1, 2, 1, 1, 1),
                dt.datetime(2000, 1, 3, 2, 2, 2), dt.datetime(2000, 1, 4, 3, 3, 3)]
        })

    def test_round_trip_basic(self):

        # Use the schema to create a table using standard TRAC data types

        trac_schema = self.sample_schema()
        df = self.sample_data()

        arrow_schema = _types.trac_to_arrow_schema(trac_schema)
        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)

        self.storage.write_table("round_trip_basic", self.storage_format, table)
        rt_table = self.storage.read_table("round_trip_basic", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_round_trip_nulls(self):

        trac_schema = self.sample_schema()
        df = self.sample_data()

        for col in df.columns:
            df[col, 2] = None
            self.assertTrue(df[col, 2].isnull)

        arrow_schema = _types.trac_to_arrow_schema(trac_schema)
        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)

        self.storage.write_table("round_trip_nulls", self.storage_format, table)
        rt_table = self.storage.read_table("round_trip_nulls", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_integer(self):

        trac_schema = self.one_field_schema(_meta.BasicType.INTEGER)
        arrow_schema = _types.trac_to_arrow_schema(trac_schema)

        df = pd.DataFrame({"integer_field": [
            0,
            sys.maxsize,
            -sys.maxsize - 1
        ]})

        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)
        self.storage.write_table("edge_cases_integer", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_integer", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_float(self):

        trac_schema = self.one_field_schema(_meta.BasicType.FLOAT)
        arrow_schema = _types.trac_to_arrow_schema(trac_schema)

        df = pd.DataFrame({"float_field": [
            0.0,
            sys.float_info.min,
            sys.float_info.max,
            sys.float_info.epsilon,
            -sys.float_info.epsilon
        ]})

        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)
        self.storage.write_table("edge_cases_float", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_float", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_decimal(self):

        # TRAC basic decimal has precision 38, scale 12
        # Should allow for 26 places before the decimal place and 12 after

        trac_schema = self.one_field_schema(_meta.BasicType.DECIMAL)
        arrow_schema = _types.trac_to_arrow_schema(trac_schema)

        df = pd.DataFrame({"decimal_field": [
            decimal.Decimal(0.0),
            decimal.Decimal(1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(1.0) / decimal.Decimal(1.0).shift(12),
            decimal.Decimal(-1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(-1.0) / decimal.Decimal(1.0).shift(12)
        ]})

        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)
        self.storage.write_table("edge_cases_decimal", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_decimal", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_string(self):

        trac_schema = self.one_field_schema(_meta.BasicType.STRING)
        arrow_schema = _types.trac_to_arrow_schema(trac_schema)

        df = pd.DataFrame({"string_field": [
            "", " ", "  ", "\t", "\r\n", "  \r\n   ",
            "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@",
            "Olá Mundo", "你好，世界", "Привет, мир", "नमस्ते दुनिया",
            "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |"
        ]})

        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)
        self.storage.write_table("edge_cases_string", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_string", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_date(self):

        trac_schema = self.one_field_schema(_meta.BasicType.DATE)
        arrow_schema = _types.trac_to_arrow_schema(trac_schema)

        df = pd.DataFrame({"date_field": [
            dt.date(1970, 1, 1),
            dt.date(2000, 1, 1),
            dt.date(2038, 1, 20),
            dt.date.max,
            dt.date.min
        ]})

        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)
        self.storage.write_table("edge_cases_date", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_date", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_datetime(self):

        trac_schema = self.one_field_schema(_meta.BasicType.DATETIME)
        arrow_schema = _types.trac_to_arrow_schema(trac_schema)

        df = pd.DataFrame({"datetime_field": [
            dt.datetime(1970, 1, 1),
            dt.datetime(2000, 1, 1),
            dt.datetime(2038, 1, 20),
            dt.datetime.max,
            dt.datetime.min
        ]})

        table = _data.DataMapping.pandas_to_arrow(df, arrow_schema)
        self.storage.write_table("edge_cases_datetime", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_datetime", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    @staticmethod
    def one_field_schema(field_type: _meta.BasicType):

        field_name = f"{field_type.name.lower()}_field"

        return _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema(field_name, fieldType=field_type)]))


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
            storageProps={"rootPath": str(TEST_DATA_DIR)})

        test_lib_storage_config = _cfg.StorageConfig([test_lib_storage_instance])
        test_lib_file_storage = _storage.LocalFileStorage(test_lib_storage_instance)
        test_lib_data_storage = _storage.CommonDataStorage(test_lib_storage_config, test_lib_file_storage)

        cls.test_lib_storage = test_lib_data_storage

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()

    def test_csv_basic(self):

        storage_options = {"lenient_csv_parser": True}

        schema = _types.trac_to_arrow_schema(self.sample_schema())
        table = self.test_lib_storage.read_table("csv_basic.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)

    def test_csv_edge_cases(self):

        storage_options = {"lenient_csv_parser": True}

        schema = _types.trac_to_arrow_schema(self.sample_schema())
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
