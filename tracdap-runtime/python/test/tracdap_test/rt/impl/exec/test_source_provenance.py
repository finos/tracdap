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

import dataclasses
import datetime as dt
import unittest
import unittest.mock as mock

import tracdap.rt.api.experimental as trac
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.metadata as _meta

import tracdap.rt._impl.core.data as _data  # noqa
import tracdap.rt._impl.core.type_system as _types  # noqa
import tracdap.rt._impl.exec.context as _ctx  # noqa
import tracdap.rt._impl.exec.functions as _func  # noqa

import tracdap_test.resources.test_models as test_models


_test_import_def = _meta.ModelDefinition(

    language="python",
    repository="trac_integrated",
    entryPoint=f"{test_models.TestImportModel.__module__}.{test_models.TestImportModel.__name__}",

    parameters=test_models.TestImportModel().define_parameters(),
    inputs=test_models.TestImportModel().define_inputs(),
    outputs=test_models.TestImportModel().define_outputs())


def _attrs_by_name(view: _data.DataView):
    return {update.attrName: update for update in view.attrs}


class SourceProvenanceContextTest(unittest.TestCase):

    """Test TracDataContextImpl.set_attribute() / set_source_metadata(), which record
    provenance facts as TagUpdates on the dataset's DataView."""

    FILE_STORAGE_KEY = "test_file_storage"
    SQL_STORAGE_KEY = "test_sql_storage"

    def setUp(self):

        self.local_ctx = {}
        self.dynamic_outputs = []

        self.storage_map = {
            self.FILE_STORAGE_KEY: mock.Mock(spec=trac.TracFileStorage),
            self.SQL_STORAGE_KEY: mock.Mock(spec=trac.TracDataStorage)}

        self.ctx = _ctx.TracDataContextImpl(
            _test_import_def, test_models.TestImportModel,
            self.local_ctx, self.dynamic_outputs, self.storage_map)

        self.ctx.add_data_import("my_dataset")

    def test_set_attribute_ok(self):

        self.ctx.set_attribute("my_dataset", "business_date", dt.date(2024, 1, 1))

        attrs = _attrs_by_name(self.local_ctx["my_dataset"])
        self.assertIn("business_date", attrs)

        update = attrs["business_date"]
        self.assertEqual(_meta.TagOperation.CREATE_OR_REPLACE_ATTR, update.operation)
        self.assertEqual(dt.date(2024, 1, 1), _types.MetadataCodec.decode_value(update.value))

    def test_set_attribute_accumulates(self):

        self.ctx.set_attribute("my_dataset", "attr_one", "value_one")
        self.ctx.set_attribute("my_dataset", "attr_two", "value_two")

        attrs = _attrs_by_name(self.local_ctx["my_dataset"])
        self.assertEqual({"attr_one", "attr_two"}, set(attrs.keys()))

    def test_set_attribute_dataset_not_available(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.set_attribute("unknown_dataset", "business_date", dt.date(2024, 1, 1)))

    def test_set_attribute_reserved_name(self):

        for reserved_name in ["trac_model_type", "_hidden_attr"]:

            self.assertRaises(
                _ex.ERuntimeValidation,
                lambda name=reserved_name: self.ctx.set_attribute("my_dataset", name, "some_value"))

            attrs = _attrs_by_name(self.local_ctx["my_dataset"])
            self.assertEqual({}, attrs)

    def test_set_source_metadata_file_stat_ok(self):

        file_stat = trac.FileStat(
            file_name="data.csv", file_type=trac.FileType.FILE,
            storage_path="/imports/data.csv", size=1234,
            mtime=dt.datetime(2024, 1, 1, 12, 0, 0))

        self.ctx.set_source_metadata("my_dataset", self.FILE_STORAGE_KEY, file_stat)

        attrs = _attrs_by_name(self.local_ctx["my_dataset"])
        self.assertEqual(
            {"trac_import_location_key", "original_file_name", "original_file_size", "original_file_modified_date"},
            set(attrs.keys()))

        self.assertEqual(self.FILE_STORAGE_KEY, _types.MetadataCodec.decode_value(attrs["trac_import_location_key"].value))
        self.assertEqual("data.csv", _types.MetadataCodec.decode_value(attrs["original_file_name"].value))
        self.assertEqual(1234, _types.MetadataCodec.decode_value(attrs["original_file_size"].value))
        self.assertEqual(dt.datetime(2024, 1, 1, 12, 0, 0), _types.MetadataCodec.decode_value(attrs["original_file_modified_date"].value))

    def test_set_source_metadata_file_stat_no_mtime(self):

        file_stat = trac.FileStat(
            file_name="data.csv", file_type=trac.FileType.FILE,
            storage_path="/imports/data.csv", size=1234, mtime=None)

        self.ctx.set_source_metadata("my_dataset", self.FILE_STORAGE_KEY, file_stat)

        attrs = _attrs_by_name(self.local_ctx["my_dataset"])
        self.assertEqual(
            {"trac_import_location_key", "original_file_name", "original_file_size"},
            set(attrs.keys()))

    def test_set_source_metadata_string_source_not_supported(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.set_source_metadata("my_dataset", self.SQL_STORAGE_KEY, "some_table"))

        # Should raise cleanly with no partial attrs written, rather than silently no-oping
        attrs = _attrs_by_name(self.local_ctx["my_dataset"])
        self.assertEqual({}, attrs)

    def test_set_source_metadata_wrong_type_for_file_storage(self):

        self.assertRaises(
            _ex.ERuntimeValidation,
            lambda: self.ctx.set_source_metadata("my_dataset", self.FILE_STORAGE_KEY, "not_a_file_stat"))


class SourceProvenanceDataPlumbingTest(unittest.TestCase):

    """Test that attrs set on a DataView survive the transformations models actually apply
    (set_schema / put_table), and that DataSpecFunc-style rebuilds don't drop them."""

    def _tag_update(self, name: str, value):
        return _meta.TagUpdate(_meta.TagOperation.CREATE_OR_REPLACE_ATTR, name, _types.MetadataCodec.encode_value(value))

    def test_data_view_with_attrs_accumulates(self):

        view = _data.DataView.create_empty()
        view = view.with_attrs([self._tag_update("a", "1")])
        view = view.with_attrs([self._tag_update("b", "2")])

        self.assertEqual(["a", "b"], [u.attrName for u in view.attrs])

    def test_data_item_with_attrs(self):

        item = _data.DataItem.create_empty()
        item = item.with_attrs([self._tag_update("a", "1")])

        self.assertEqual(["a"], [u.attrName for u in item.attrs])

    def test_add_item_to_view_preserves_attrs(self):

        schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE, _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[_meta.FieldSchema("field_1", fieldType=_meta.BasicType.STRING)]))

        view = _data.DataView.for_trac_schema(schema)
        view = view.with_attrs([self._tag_update("trac_import_location_key", "test_storage")])

        item = _data.DataItem.for_table(None, view.arrow_schema, schema)
        updated_view = _data.DataMapping.add_item_to_view(view, _data.DataPartKey.for_root(), item)

        self.assertEqual(["trac_import_location_key"], [u.attrName for u in updated_view.attrs])


class SourceProvenanceJobResultTest(unittest.TestCase):

    """Test that JobResultFunc._process_data_spec forwards DataSpec.attrs into job_result.attrs,
    mirroring the existing GraphOutput.attrs forwarding path."""

    @staticmethod
    def _data_spec(object_id: str, storage_id: str, attrs):

        primary_id = _meta.TagHeader(objectType=_meta.ObjectType.DATA, objectId=object_id, objectVersion=1)
        storage_tag_id = _meta.TagHeader(objectType=_meta.ObjectType.STORAGE, objectId=storage_id, objectVersion=1)

        spec = _data.DataSpec.create_data_spec(
            "data_item_key", _meta.DataDefinition(), _meta.StorageDefinition())
        spec = spec.with_ids(primary_id, storage_tag_id)

        return dataclasses.replace(spec, attrs=attrs)

    def test_process_data_spec_forwards_attrs(self):

        attrs = [_meta.TagUpdate(
            _meta.TagOperation.CREATE_OR_REPLACE_ATTR,
            "trac_import_location_key", _types.MetadataCodec.encode_value("test_storage"))]

        data_spec = self._data_spec("data-1", "storage-1", attrs)
        job_result = _cfg.JobResult()

        _func.JobResultFunc._process_data_spec("my_output", data_spec, job_result)

        output_key = "DATA-data-1-v1"
        self.assertIn(output_key, job_result.attrs)
        self.assertEqual(attrs, job_result.attrs[output_key].attrs)

    def test_process_data_spec_multiple_outputs(self):

        attrs_1 = [_meta.TagUpdate(_meta.TagOperation.CREATE_OR_REPLACE_ATTR, "attr_1", _types.MetadataCodec.encode_value("v1"))]
        attrs_2 = [_meta.TagUpdate(_meta.TagOperation.CREATE_OR_REPLACE_ATTR, "attr_2", _types.MetadataCodec.encode_value("v2"))]

        data_spec_1 = self._data_spec("data-1", "storage-1", attrs_1)
        data_spec_2 = self._data_spec("data-2", "storage-2", attrs_2)

        job_result = _cfg.JobResult()

        _func.JobResultFunc._process_data_spec("output_one", data_spec_1, job_result)
        _func.JobResultFunc._process_data_spec("output_two", data_spec_2, job_result)

        self.assertEqual(attrs_1, job_result.attrs["DATA-data-1-v1"].attrs)
        self.assertEqual(attrs_2, job_result.attrs["DATA-data-2-v1"].attrs)

    def test_process_data_spec_no_attrs_not_forwarded(self):

        data_spec = self._data_spec("data-1", "storage-1", [])
        job_result = _cfg.JobResult()

        _func.JobResultFunc._process_data_spec("my_output", data_spec, job_result)

        self.assertNotIn("DATA-data-1-v1", job_result.attrs)


if __name__ == "__main__":
    unittest.main()
