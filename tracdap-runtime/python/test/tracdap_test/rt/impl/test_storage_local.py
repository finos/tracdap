#  Copyright 2023 Accenture Global Solutions Limited
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

import tempfile
import copy

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.plugins as _plugins
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.util as _util  # noqa

from tracdap_test.rt.suites.file_storage_suite import *
from tracdap_test.rt.suites.data_storage_suite import *
from tracdap_test.rt.suites.data_storage_suite import _TEST_DATA_DIR  # noqa

_plugins.PluginManager.register_core_plugins()


# ----------------------------------------------------------------------------------------------------------------------
# FILE STORAGE
# ----------------------------------------------------------------------------------------------------------------------


class LocalArrowImplStorageTest(unittest.TestCase, FileOperationsTestSuite, FileReadWriteTestSuite):

    storage_root: tempfile.TemporaryDirectory
    test_number: int

    @classmethod
    def setUpClass(cls) -> None:

        cls.storage_root = tempfile.TemporaryDirectory()
        cls.test_number = 0

    def setUp(self):

        test_dir = pathlib.Path(self.storage_root.name).joinpath(f"test_{self.test_number}")
        test_dir.mkdir()

        self.__class__.test_number += 1

        test_storage_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={
                "rootPath": str(test_dir),
                "runtimeFs": "arrow"})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_bucket"] = test_storage_config

        manager = _storage.StorageManager(sys_config)
        self.storage = manager.get_file_storage("test_bucket")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.storage_root.cleanup()


class LocalPythonImplStorageTest(unittest.TestCase, FileOperationsTestSuite, FileReadWriteTestSuite):

    storage_root: tempfile.TemporaryDirectory
    test_number: int

    @classmethod
    def setUpClass(cls) -> None:

        cls.storage_root = tempfile.TemporaryDirectory()
        cls.test_number = 0

    def setUp(self):

        test_dir = pathlib.Path(self.storage_root.name).joinpath(f"test_{self.test_number}")
        test_dir.mkdir()

        self.__class__.test_number += 1

        test_storage_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={
                "rootPath": str(test_dir),
                "runtimeFs": "python"})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_bucket"] = test_storage_config

        manager = _storage.StorageManager(sys_config)
        self.storage = manager.get_file_storage("test_bucket")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.storage_root.cleanup()


# ----------------------------------------------------------------------------------------------------------------------
# DATA STORAGE
# ----------------------------------------------------------------------------------------------------------------------


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


class LocalCsvFormatStorageTest(unittest.TestCase, LocalStorageTest):

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


class LocalArrowFormatStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "ARROW_FILE"

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()


class LocalParquetFormatStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "PARQUET"

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()
