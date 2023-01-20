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
import random
import copy

import pyarrow as pa

import tracdap.rt.config as _cfg
import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.util as _util  # noqa


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
    assertIsNotNone = unittest.TestCase.assertIsNotNone
    assertRaises = unittest.TestCase.assertRaises

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

        return _data.DataMapping.trac_to_arrow_schema(trac_schema)

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

        return _data.DataMapping.trac_to_arrow_schema(trac_schema)

    @staticmethod
    def random_bytes(n_bytes: int) -> bytes:

        bs = bytearray(n_bytes)

        for i in range(n_bytes):
            b = random.randint(0, 255)
            bs[i] = b

        return bytes(bs)

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

        # It may be helpful to check for / prohibit inf and -inf in some places, e.g. model outputs
        # But still the storage layer should handle these values correctly if they are present

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
        # Also, make sure to keep the distinction between NaN and None

        schema = self.one_field_schema(_meta.BasicType.FLOAT)
        table = pa.Table.from_pydict({"float_field": [math.nan]}, schema)  # noqa

        self.storage.write_table("edge_cases_float_nan", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_float_nan", self.storage_format, table.schema)

        nan_value = rt_table.column(0)[0].as_py()

        self.assertIsNotNone(nan_value)
        self.assertTrue(math.isnan(nan_value))

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
    file_storage: _storage.IFileStorage

    @classmethod
    def make_storage(cls):

        cls.storage_root = tempfile.TemporaryDirectory()

        bucket_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={"rootPath": cls.storage_root.name})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_bucket"] = bucket_config

        manager = _storage.StorageManager(sys_config)
        file_storage = manager.get_file_storage("test_bucket")
        data_storage = manager.get_data_storage("test_bucket")

        cls.file_storage = file_storage

        return data_storage

    # For file-based storage, test reading garbled data

    def test_read_garbled_data(self):

        garbage = self.random_bytes(256)
        schema = self.sample_schema()

        self.file_storage.write_bytes(f"garbled_data.{self.storage_format}", garbage)

        self.assertRaises(
            _ex.EDataCorruption,
            lambda: self.storage.read_table(
                f"garbled_data.{self.storage_format}",
                self.storage_format, schema))


class LocalCsvStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "CSV"

        test_lib_storage_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={"rootPath": str(_TEST_DATA_DIR)})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_csv_bucket"] = test_lib_storage_config

        manager = _storage.StorageManager(sys_config)
        test_lib_data_storage = manager.get_data_storage("test_csv_bucket")

        cls.test_lib_storage_instance_cfg = test_lib_storage_config
        cls.test_lib_storage = test_lib_data_storage

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()

    @unittest.skip("CSV read hangs with the strict (Arrow) CSV implementation for garbled data")
    def test_read_garbled_data(self):
        super().test_read_garbled_data()

    def test_csv_basic(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_basic.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)

    def test_lenient_edge_cases(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_edge_cases.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)

    def test_lenient_nulls(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_nulls.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(7, table.num_rows)

        # Nulls test dataset has nulls in the diagonals, i.e. row 0 col 0, row 1 col 1 etc.

        for i in range(7):
            column: pa.Array = table.column(i)
            column_name = table.column_names[i]
            value = column[i].as_py()

            # The lenient CSV parser does not know the difference between empty string and null

            if column_name == "string_field":
                self.assertEqual(value, "")

            else:
                self.assertIsNone(value, msg=f"Found non-null value in row [{i}], column [{column_name}]")

    def test_lenient_read_garbled_data(self):

        # Try reading garbage data with the lenient CSV parser

        storage_options = {"lenient_csv_parser": True}

        garbage = self.random_bytes(256)
        schema = self.sample_schema()

        self.file_storage.write_bytes(f"csv_garbled_data.{self.storage_format}", garbage)

        self.assertRaises(
            _ex.EDataCorruption,
            lambda: self.storage.read_table(
                f"csv_garbled_data.{self.storage_format}",
                self.storage_format, schema, storage_options))

    def test_lenient_read_garbled_text(self):

        # Try reading garbage textual data with the lenient CSV parser
        # Because CSV is such a loose format, the parser will assemble rows and columns
        # However, some form of EData exception should still be raised
        # Since reading CSV requires a schema and the schema will not match, normally this will be EDataConformance

        storage_options = {"lenient_csv_parser": True}

        garbage = "£$%£$%£$%£$%'#[]09h8\t{}}},,,,AS£F".encode("utf-8")
        schema = self.sample_schema()

        self.file_storage.write_bytes(f"csv_garbled_data_2.{self.storage_format}", garbage)

        self.assertRaises(
            _ex.EData,
            lambda: self.storage.read_table(
                f"csv_garbled_data_2.{self.storage_format}",
                self.storage_format, schema, storage_options))

    def test_csv_nan(self):

        # Test reading in CSV NaN with the strict (Apache Arrow) CSV parser

        schema = pa.schema([("float_field", pa.float64())])
        table = self.test_lib_storage.read_table("csv_nan.csv", "CSV", schema)

        self.assertEqual(1, table.num_columns)
        self.assertEqual(2, table.num_rows)

        for row, value in enumerate(table.column(0)):
            self.assertIsNotNone(value.as_py())
            self.assertTrue(math.isnan(value.as_py()))

    def test_date_format_props(self):

        test_lib_storage_instance = copy.deepcopy(self.test_lib_storage_instance_cfg)
        test_lib_storage_instance.properties["csv.lenient_csv_parser"] = "true"
        test_lib_storage_instance.properties["csv.date_format"] = "%d/%m/%Y"
        test_lib_storage_instance.properties["csv.datetime_format"] = "%d/%m/%Y %H:%M:%S"

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_csv_bucket"] = test_lib_storage_instance

        manager = _storage.StorageManager(sys_config)
        test_lib_data_storage = manager.get_data_storage("test_csv_bucket")

        schema = self.sample_schema()
        table = test_lib_data_storage.read_table("csv_basic_uk_dates.csv", "CSV", schema)

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
