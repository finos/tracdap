#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import unittest
import decimal
import datetime as dt
import tempfile
import pathlib
import sys
import math

import pandas as pd
import pandas.testing as pd_test

import tracdap.rt.api.experimental as trac
import tracdap.rt.ext.plugins as plugins

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.data as _data  # noqa
import tracdap.rt._impl.core.logging as _log # noqa
import tracdap.rt._impl.core.storage as _storage  # noqa
import tracdap.rt._impl.core.util as _util  # noqa
import tracdap.rt._impl.exec.context as _ctx  # noqa

import tracdap_test.resources.test_models as test_models


_test_import_def = trac.ModelDefinition(

    language="python",
    repository="trac_integrated",
    entryPoint=f"{test_models.TestImportModel.__module__}.{test_models.TestImportModel.__name__}",

    parameters=test_models.TestImportModel().define_parameters(),
    inputs=test_models.TestImportModel().define_inputs(),
    outputs=test_models.TestImportModel().define_outputs())


@unittest.skip("No backend available for unit testing, integration tests use the storage test suite")
class TestDataImportExport(unittest.TestCase):

    TEST_STORAGE_KEY = "test_data_storage"

    ctx: trac.TracDataContext
    storage_key: str
    framework = trac.PANDAS

    test_dir: tempfile.TemporaryDirectory
    storage: trac.TracDataStorage[trac.PANDAS.api_type]

    log: logging.Logger

    @staticmethod
    def sample_schema():

        return trac.SchemaDefinition(
            trac.SchemaType.TABLE,
            trac.PartType.PART_ROOT,
            trac.TableSchema(fields=[
                trac.FieldSchema("boolean_field", fieldType=trac.BasicType.BOOLEAN),
                trac.FieldSchema("integer_field", fieldType=trac.BasicType.INTEGER),
                trac.FieldSchema("float_field", fieldType=trac.BasicType.FLOAT),
                trac.FieldSchema("decimal_field", fieldType=trac.BasicType.DECIMAL),
                trac.FieldSchema("string_field", fieldType=trac.BasicType.STRING),
                trac.FieldSchema("date_field", fieldType=trac.BasicType.DATE),
                trac.FieldSchema("datetime_field", fieldType=trac.BasicType.DATETIME),
            ]))

    @staticmethod
    def one_field_schema(field_type: trac.BasicType):

        field_name = f"{field_type.name.lower()}_field"

        return trac.SchemaDefinition(
            trac.SchemaType.TABLE,
            trac.PartType.PART_ROOT,
            trac.TableSchema(fields=[
                trac.FieldSchema(field_name, fieldType=field_type)]))

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
                dt.datetime(2000, 1, 5, 0, 0, 0), dt.datetime(2000, 1, 6, 1, 1, 1),
                dt.datetime(2000, 1, 7, 2, 2, 2), dt.datetime(2000, 1, 8, 3, 3, 3)]
        }

    @classmethod
    def setUpClass(cls) -> None:

        plugins.PluginManager.register_core_plugins()

        cls.test_dir = tempfile.TemporaryDirectory()
        cls.test_number = 0

        _log.configure_logging()
        cls.log = _log.logger_for_class(cls)

    @classmethod
    def tearDownClass(cls) -> None:

        _util.try_clean_dir(pathlib.Path(cls.test_dir.name))
        cls.test_dir.cleanup()

    def setUp(self):

        test_dir = pathlib.Path(self.test_dir.name).joinpath(f"test_{self.test_number}")
        test_dir.mkdir()

        self.__class__.test_number += 1

        db_url = f"file:{test_dir}/{self.TEST_STORAGE_KEY}.sqlite?mode=rwc"

        self.log.info(f"Opening DB URL: {db_url}")

        test_storage_config = _cfg.PluginConfig(
            protocol="SQL",
            properties={
                "dialect": "sqlite",
                "sqlite.database": db_url,
                "sqlite.uri": "true"
            })

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.external[self.TEST_STORAGE_KEY] = test_storage_config

        manager = _storage.StorageManager(sys_config)

        local_ctx = dict()
        dynamic_outputs = list()

        storage_impl = manager.get_data_storage(self.TEST_STORAGE_KEY, external=True)
        converter = _data.DataConverter.for_framework(trac.PANDAS)

        storage_wrapper = _ctx.TracDataStorageImpl(
            self.TEST_STORAGE_KEY, storage_impl, converter,
            write_access=True, checkout_directory=None)

        storage_map = {self.TEST_STORAGE_KEY: storage_wrapper}

        self.ctx = _ctx.TracDataContextImpl(
            _test_import_def, test_models.TestImportModel,
            local_ctx, dynamic_outputs, storage_map)

        self.storage_key = self.TEST_STORAGE_KEY

        self.storage = self.ctx.get_data_storage(self.storage_key, trac.PANDAS)

    def test_has_table(self):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)

        self.assertFalse(storage.has_table("test_has_table"))

        storage.create_table("test_has_table", self.sample_schema())

        self.assertTrue(storage.has_table("test_has_table"))

    def test_list_tables(self):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)
        table_names = storage.list_tables()

        self.assertNotIn("test_list_tables", table_names)

        storage.create_table("test_list_tables", self.sample_schema())
        table_names = storage.list_tables()

        self.assertIn("test_list_tables", table_names)

    def test_create_table_ok(self):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)

        self.assertFalse(storage.has_table("test_create_table_ok"))

        storage.create_table("test_create_table_ok", self.sample_schema())

        self.assertTrue(storage.has_table("test_create_table_ok"))

    def test_create_table_bad_name(self):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)

        self.assertRaises(_ex.ERuntimeValidation, lambda: storage.create_table("$$$-not-allowed", self.sample_schema()))

    def test_create_table_bad_schema(self):

        bad_schema = trac.SchemaDefinition(
            trac.SchemaType.TABLE,
            trac.PartType.PART_ROOT,
            trac.TableSchema(fields=[
                trac.FieldSchema("boolean_field", fieldType=trac.BasicType.BOOLEAN),
                trac.FieldSchema("boolean_field", fieldType=trac.BasicType.BOOLEAN),
                trac.FieldSchema("boolean_field", fieldType=trac.BasicType.BOOLEAN)
            ]))

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)

        self.assertRaises(_ex.ERuntimeValidation, lambda: storage.create_table("test_create_table_bad_schema", bad_schema))

    def test_create_table_already_exists(self):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)

        storage.create_table("test_create_table_already_exists", self.sample_schema())

        self.assertRaises(_ex.ERuntimeValidation, lambda: storage.create_table("test_create_table_already_exists", self.sample_schema()))

    def _do_normalization(self, original_data, schema):

        converter = _data.DataConverter.for_framework(trac.PANDAS)
        arrow_schema = _data.DataMapping.trac_to_arrow_schema(schema)

        return converter.from_internal(converter.to_internal(original_data, arrow_schema), arrow_schema)

    def _do_round_trip(self, table_name, schema, original_data):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)
        storage.create_table(table_name, schema)

        converter = _data.DataConverter.for_framework(trac.PANDAS)
        arrow_schema = _data.DataMapping.trac_to_arrow_schema(schema)
        original_data = converter.from_internal(converter.to_internal(original_data, arrow_schema), arrow_schema)

        self.storage.write_table(table_name, original_data)
        return self.storage.read_table(table_name, schema)

    def test_round_trip_basic(self):

        original_data = pd.DataFrame.from_dict(self.sample_data())
        original_data = self._do_normalization(original_data, self.sample_schema())
        rt_data = self._do_round_trip("test_round_trip_basic", self.sample_schema(), original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_round_trip_nulls(self):

        sample_data = self.sample_data()
        for column_name, values in sample_data.items():
            values[1] = None
        original_data = pd.DataFrame.from_dict(sample_data)
        original_data["integer_field"] = original_data["integer_field"].astype(pd.Int64Dtype())
        original_data = self._do_normalization(original_data, self.sample_schema())

        rt_data = self._do_round_trip("test_round_trip_nulls", self.sample_schema(), original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_round_trip_no_data(self):

        original_data = pd.DataFrame.from_dict(self.sample_data())
        original_data = original_data[original_data["integer_field"] == -11111111]  # select no rows
        original_data = self._do_normalization(original_data, self.sample_schema())

        rt_data = self._do_round_trip("test_round_trip_no_data", self.sample_schema(), original_data)

        pd_test.assert_frame_equal(original_data, rt_data)
        self.assertEqual(0, len(original_data))
        self.assertEqual(0, len(rt_data))

    def test_edge_cases_integer(self):

        schema = self.one_field_schema(trac.INTEGER)

        original_data = pd.DataFrame.from_dict({"integer_field": [
            0,
            sys.maxsize,
            -sys.maxsize - 1
        ]})

        original_data = self._do_normalization(original_data, schema)
        rt_data = self._do_round_trip("test_edge_cases_integer", schema, original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_edge_cases_float(self):

        # It may be helpful to check for / prohibit inf and -inf in some places, e.g. model outputs
        # But still the storage layer should handle these values correctly if they are present

        schema = self.one_field_schema(trac.FLOAT)

        original_data = pd.DataFrame.from_dict({"float_field": [
            0.0,
            sys.float_info.min,
            sys.float_info.max,
            sys.float_info.epsilon,
            -sys.float_info.epsilon,
            math.inf,
            -math.inf
        ]})

        original_data = self._do_normalization(original_data, schema)
        rt_data = self._do_round_trip("test_edge_cases_float", schema, original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_edge_cases_float_nan(self):

        # For NaN, a special test that checks math.isnan on the round-trip result
        # Because math.nan != math.nan
        # Also, make sure to keep the distinction between NaN and None

        schema = self.one_field_schema(trac.FLOAT)

        original_data = pd.DataFrame.from_dict({"float_field": [math.nan]})
        original_data = self._do_normalization(original_data, schema)

        rt_data = self._do_round_trip("test_edge_cases_float_nan", schema, original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_edge_cases_decimal(self):

        # TRAC basic decimal has precision 38, scale 12
        # Should allow for 26 places before the decimal place and 12 after

        schema = self.one_field_schema(trac.DECIMAL)

        original_data = pd.DataFrame.from_dict({"decimal_field": [
            decimal.Decimal(0.0),
            decimal.Decimal(1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(1.0) / decimal.Decimal(1.0).shift(12),
            decimal.Decimal(-1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(-1.0) / decimal.Decimal(1.0).shift(12)
        ]})

        original_data = self._do_normalization(original_data, schema)
        rt_data = self._do_round_trip("test_edge_cases_decimal", schema, original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_edge_cases_string(self):

        schema = self.one_field_schema(trac.STRING)

        original_data = pd.DataFrame.from_dict({"string_field": [  # noqa
            "", " ", "  ", "\t", "\r\n", "  \r\n   ",
            "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@",
            "Olá Mundo", "你好，世界", "Привет, мир", "नमस्ते दुनिया",
            "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |"
        ]})

        original_data = self._do_normalization(original_data, schema)
        rt_data = self._do_round_trip("test_edge_cases_string", schema, original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_edge_cases_date(self):

        schema = self.one_field_schema(trac.DATE)

        original_data = pd.DataFrame.from_dict({"date_field": [  # noqa
            dt.date(1970, 1, 1),
            dt.date(2000, 1, 1),
            dt.date(2038, 1, 20),
            dt.date.max,
            dt.date.min
        ]})

        original_data = self._do_normalization(original_data, schema)
        rt_data = self._do_round_trip("test_edge_cases_date", schema, original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    @unittest.skip("Doesn't work on SQLite")
    def test_edge_cases_datetime(self):

        schema = self.one_field_schema(trac.DATETIME)

        original_data = pd.DataFrame.from_dict({"datetime_field": [  # noqa
            dt.datetime(1970, 1, 1, 0, 0, 0),
            dt.datetime(2000, 1, 1, 0, 0, 0),
            dt.datetime(2038, 1, 19, 3, 14, 8),
            # Fractional seconds before and after the epoch
            # Test fractions for both positive and negative encoded values
            dt.datetime(1972, 1, 1, 0, 0, 0, 500000),
            dt.datetime(1968, 1, 1, 23, 59, 59, 500000),
            dt.datetime.max - dt.timedelta(seconds = 1),
            dt.datetime.min + dt.timedelta(seconds = 1),
        ]})

        original_data = self._do_normalization(original_data, schema)
        rt_data = self._do_round_trip("test_edge_cases_datetime", schema, original_data)

        pd_test.assert_frame_equal(original_data, rt_data)

    def test_read_missing_table(self):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)

        self.assertRaises(_ex.ERuntimeValidation, lambda: storage.read_table("test_read_missing_table", self.sample_schema()))

    def test_read_wrong_schema(self):

        self.skipTest("Not needed?")

    def test_read_bad_data(self):

        self.skipTest("Not needed?")

    def test_write_missing_table(self):

        original_data = pd.DataFrame.from_dict(self.sample_data())

        self.assertRaises(
            _ex.ERuntimeValidation, lambda:
            self.storage.write_table("test_write_missing_table", original_data))

    def test_write_wrong_schema(self):

        original_data = pd.DataFrame.from_dict(self.sample_data())

        missing_field_schema = self.sample_schema()
        missing_field_schema.table.fields = missing_field_schema.table.fields[:-1]

        self.storage.create_table("test_write_wrong_schema", missing_field_schema)

        self.assertRaises(
            _ex.ERuntimeValidation, lambda:
            self.storage.write_table("test_write_wrong_schema", original_data))

        wrong_type_schema = self.sample_schema()
        for field in wrong_type_schema.table.fields:
            field.fieldType = trac.INTEGER

        self.storage.create_table("test_write_wrong_schema_2", wrong_type_schema)

        self.assertRaises(
            _ex.ERuntimeValidation, lambda:
            self.storage.write_table("test_write_wrong_schema_2", original_data))

    def test_write_append_ok(self):

        storage = self.ctx.get_data_storage(self.storage_key, self.framework)
        storage.create_table("test_round_trip_basic", self.sample_schema())

        original_data = pd.DataFrame.from_dict(self.sample_data())
        conv = _data.DataConverter.for_framework(trac.PANDAS)
        schema = _data.DataMapping.trac_to_arrow_schema(conv.infer_schema(original_data))
        original_data = conv.from_internal(conv.to_internal(original_data, schema), schema)

        storage.write_table("test_round_trip_basic", original_data)
        storage.write_table("test_round_trip_basic", original_data)
        rt_data = storage.read_table("test_round_trip_basic", self.sample_schema())

        original_data_x2 = pd.concat([original_data, original_data])
        original_data_x2 = conv.from_internal(conv.to_internal(original_data_x2, schema), schema)

        pd_test.assert_frame_equal(original_data_x2, rt_data)

    def test_write_bad_data(self):

        self.skipTest("Not needed?")

    def test_write_append_key_conflict(self):

        self.fail()

    def test_native_query_ok(self):

        self.fail()

    def test_native_query_missing_table(self):

        self.fail()

    def test_native_query_bad_query(self):

        self.fail()

    def test_native_query_bad_parameters(self):

        self.fail()

    def test_native_query_no_data(self):

        self.fail()
