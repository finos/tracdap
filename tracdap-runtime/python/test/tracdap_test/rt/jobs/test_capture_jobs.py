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

import dataclasses as dc
import datetime as dt
import decimal
import hashlib
import pathlib
import re
import tempfile
import typing as tp
import unittest
import unittest.mock as mock

import pyarrow as pa
import pyarrow.feather as pa_ft
import pyarrow.parquet as pa_pq
import yaml

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.launch as launch
import tracdap.rt.metadata as meta
import tracdap.rt._impl.runtime as runtime  # noqa
import tracdap.rt._impl.core.capture as capture  # noqa
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.core.data as data  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.plugins as plugins  # noqa
import tracdap.rt._impl.core.storage as storage  # noqa
import tracdap.rt._impl.core.type_system as types  # noqa
import tracdap.rt._impl.core.util as util  # noqa
import tracdap.rt._impl.exec.graph as graph  # noqa
import tracdap.rt._impl.exec.graph_builder as graph_builder  # noqa


plugins.PluginManagerImpl.register_core_plugins()


def _schema(*fields: tp.Tuple[str, meta.BasicType]) -> meta.SchemaDefinition:

    return meta.SchemaDefinition(
        schemaType=meta.SchemaType.TABLE,
        partType=meta.PartType.PART_ROOT,
        table=meta.TableSchema([
            meta.FieldSchema(name, index, field_type)
            for index, (name, field_type) in enumerate(fields)]))


def _declared(path: str, schema: meta.SchemaDefinition, storage_key: str = "staging") -> meta.CaptureSource:

    return meta.CaptureSource(
        location=meta.ExternalLocation(storage_key, path),
        schemaSource=meta.CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED,
        schema=schema)


def _file_schema(path: str, storage_key: str = "staging") -> meta.CaptureSource:

    return meta.CaptureSource(
        location=meta.ExternalLocation(storage_key, path),
        schemaSource=meta.CaptureSchemaSource.CAPTURE_SCHEMA_FILE)


def _attrs(tag_updates: tp.List[meta.TagUpdate]) -> tp.Dict[str, tp.Any]:

    return {attr.attrName: types.MetadataCodec.decode_value(attr.value) for attr in tag_updates}


_DECODER_LOG = "tracdap.rt._impl.core.capture.CaptureDecoder"

_LOANS_SCHEMA = _schema(
    ("id", meta.BasicType.INTEGER),
    ("amount", meta.BasicType.DECIMAL),
    ("region", meta.BasicType.STRING))


class CaptureTestBase(unittest.TestCase):

    tmp_dir: pathlib.Path
    staging: pathlib.Path

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    def setUp(self) -> None:

        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = pathlib.Path(self._tmp.name)

        for storage_dir in ["data", "staging", "scratch"]:
            self.tmp_dir.joinpath(storage_dir).mkdir()

        self.staging = self.tmp_dir.joinpath("staging")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _sys_config(self, staging_properties: tp.Optional[dict] = None, capture_size_mb: tp.Optional[int] = None):

        sys_config = cfg.RuntimeConfig()
        sys_config.properties["storage.default.location"] = "test_data"
        sys_config.properties["storage.default.format"] = "CSV"

        if capture_size_mb is not None:
            sys_config.properties[cfg_p.ConfigKeys.RUNTIME_LIMIT_CAPTURE_SIZE] = str(capture_size_mb)

        sys_config.resources["test_data"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.INTERNAL_STORAGE, protocol="LOCAL",
            properties={"rootPath": str(self.tmp_dir.joinpath("data"))})

        sys_config.resources["staging"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.EXTERNAL_STORAGE, protocol="LOCAL",
            properties={"rootPath": str(self.staging), **(staging_properties or {})})

        return sys_config

    def _write_csv(self, path: str, content: str):
        file_path = self.staging.joinpath(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content.encode("utf-8"))

    def _write_parquet(self, path: str, table: pa.Table):
        pa_pq.write_table(table, self.staging.joinpath(path))

    def _write_arrow(self, path: str, table: pa.Table):
        pa_ft.write_feather(table, self.staging.joinpath(path), compression="uncompressed")


class CaptureDecodeTest(CaptureTestBase):

    """Capture and decode single files, using the capture components directly"""

    def _capture(
            self, capture_source: meta.CaptureSource, name: str = "loans",
            staging_properties: tp.Optional[dict] = None, size_limit: int = 1024 * 1024):

        storage_manager = storage.StorageManager(self._sys_config(staging_properties))
        file_id = util.new_object_id(meta.ObjectType.FILE)

        extension = capture.CaptureFormats.file_extension(capture_source.location.storagePath)
        capture_format = capture.CaptureFormats.for_extension(extension)

        file_item = capture.CaptureReader(storage_manager).read_file(name, capture_source.location, size_limit)

        data_item = capture.CaptureDecoder(storage_manager).decode_table(
            name, file_item, capture_source.location.storageKey, capture_format.format_code,
            capture_source.schemaSource, capture_source.schema, file_id)

        return file_item, data_item, file_id

    # Column matching with a declared schema

    def test_declared_case_insensitive_columns(self):

        self._write_csv("loans.csv", "ID,Amount,REGION\n1,10.5,north\n2,20,south\n")
        self._write_parquet("loans.parquet", pa.table({"ID": [1, 2], "Amount": [10.5, 20.0], "REGION": ["north", "south"]}))
        self._write_arrow("loans.arrow", pa.table({"ID": [1, 2], "Amount": [10.5, 20.0], "REGION": ["north", "south"]}))

        for path in ["loans.csv", "loans.parquet", "loans.arrow"]:
            with self.subTest(path=path):

                _, data_item, _ = self._capture(_declared(path, _LOANS_SCHEMA))

                self.assertEqual(["id", "amount", "region"], data_item.table.schema.names)
                self.assertEqual([1, 2], data_item.table.column("id").to_pylist())
                self.assertEqual(["north", "south"], data_item.table.column("region").to_pylist())

    def test_declared_missing_column(self):

        self._write_csv("loans.csv", "id,amount\n1,10.5\n")
        self._write_parquet("loans.parquet", pa.table({"id": [1], "amount": [10.5]}))
        self._write_arrow("loans.arrow", pa.table({"id": [1], "amount": [10.5]}))

        lenient = {"csv.lenient_csv_parser": "true", "csv.lenient_missing_columns": "true"}

        for path, props in [("loans.csv", None), ("loans.csv", lenient), ("loans.parquet", None), ("loans.arrow", None)]:
            with self.subTest(path=path, lenient=props is not None):
                with self.assertRaisesRegex(ex.EDataConformance, re.escape("Capture [loans] failed: Field [region]")):
                    self._capture(_declared(path, _LOANS_SCHEMA), staging_properties=props)

    def test_declared_ambiguous_columns(self):

        self._write_csv("loans.csv", "id,amount,region,Region\n1,10.5,north,south\n")

        with self.assertRaisesRegex(ex.EDataConformance, "matches more than one column"):
            self._capture(_declared("loans.csv", _LOANS_SCHEMA))

    def test_declared_csv_bom_and_quoted_headers(self):

        self._write_csv("loans.csv", '﻿"Id","Amount","Region"\n1,10.5,north\n')

        _, data_item, _ = self._capture(_declared("loans.csv", _LOANS_SCHEMA))

        self.assertEqual(["id", "amount", "region"], data_item.table.schema.names)
        self.assertEqual([1], data_item.table.column("id").to_pylist())

    def test_declared_column_left_out(self):

        self._write_csv("loans.csv", "id,amount,region,notes\n1,10.5,north,a note\n")

        with self.assertLogs(_DECODER_LOG, level="INFO") as logs:
            _, data_item, _ = self._capture(_declared("loans.csv", _LOANS_SCHEMA))

        self.assertEqual(["id", "amount", "region"], data_item.table.schema.names)
        self.assertTrue(any("left out columns" in line and "notes" in line for line in logs.output))

        attrs = _attrs(data_item.attrs)
        self.assertEqual(4, attrs["trac_import_source_columns"])
        self.assertEqual("DECLARED", attrs["trac_import_schema_source"])

    # Type mapping and conformance with file schemas

    def test_file_schema_standard_types(self):

        table = pa.table({
            "name": pa.array(["a", "b"], pa.large_string()),
            "count": pa.array([1, 2], pa.int32()),
            "category": pa.array(["x", "y"]).dictionary_encode()})

        self._write_parquet("sample.parquet", table)

        _, data_item, _ = self._capture(_file_schema("sample.parquet"))

        fields = {f.fieldName: f for f in data_item.trac_schema.table.fields}
        self.assertEqual(meta.BasicType.STRING, fields["name"].fieldType)
        self.assertEqual(meta.BasicType.INTEGER, fields["count"].fieldType)
        self.assertEqual(meta.BasicType.STRING, fields["category"].fieldType)
        self.assertTrue(fields["category"].categorical)

        self.assertEqual(pa.utf8(), data_item.table.schema.field("name").type)
        self.assertEqual(pa.int64(), data_item.table.schema.field("count").type)
        self.assertEqual("FILE", _attrs(data_item.attrs)["trac_import_schema_source"])

    def test_file_schema_null_column(self):

        self._write_parquet("sample.parquet", pa.table({"id": [1, 2], "empty": pa.nulls(2)}))

        _, data_item, _ = self._capture(_file_schema("sample.parquet"))

        fields = {f.fieldName: f for f in data_item.trac_schema.table.fields}
        self.assertEqual(meta.BasicType.STRING, fields["empty"].fieldType)
        self.assertEqual([None, None], data_item.table.column("empty").to_pylist())

    def test_zoned_timestamps(self):

        # 08:00 UTC is 09:00 in London in July
        instant = dt.datetime(2026, 7, 1, 8, 0, tzinfo=dt.timezone.utc)

        table = pa.table({
            "id": [1],
            "utc_time": pa.array([instant], pa.timestamp("us", tz="UTC")),
            "london_time": pa.array([instant], pa.timestamp("us", tz="Europe/London"))})

        self.assertEqual("09:00", table.column("london_time").to_pylist()[0].strftime("%H:%M"))

        self._write_parquet("times.parquet", table)

        declared = _schema(
            ("id", meta.BasicType.INTEGER),
            ("utc_time", meta.BasicType.DATETIME),
            ("london_time", meta.BasicType.DATETIME))

        for source in [_file_schema("times.parquet"), _declared("times.parquet", declared)]:
            with self.subTest(schema_source=source.schemaSource.name):

                with self.assertLogs(_DECODER_LOG, level="INFO") as logs:
                    _, data_item, _ = self._capture(source)

                for column in ["utc_time", "london_time"]:
                    self.assertIsNone(data_item.table.schema.field(column).type.tz)
                    self.assertEqual(dt.datetime(2026, 7, 1, 8, 0), data_item.table.column(column).to_pylist()[0])

                self.assertTrue(any("[london_time] to UTC from zone [Europe/London]" in line for line in logs.output))

    def test_file_schema_unsupported_type(self):

        self._write_parquet("sample.parquet", pa.table({"id": [1], "blob": pa.array([b"x"], pa.binary())}))

        with self.assertRaisesRegex(ex.EDataConformance, re.escape("Column [blob] has a type TRAC can't represent [binary]")):
            self._capture(_file_schema("sample.parquet"))

    # CSV date-times

    def test_csv_datetimes(self):

        schema = _schema(("id", meta.BasicType.INTEGER), ("when", meta.BasicType.DATETIME))

        cases = {
            "no_offset.csv": ("id,when\n1,2026-07-01T09:00:00\n", dt.datetime(2026, 7, 1, 9, 0)),
            "zulu.csv": ("id,when\n1,2026-07-01T08:00:00Z\n", dt.datetime(2026, 7, 1, 8, 0)),
            "offset.csv": ("id,when\n1,2026-07-01T09:00:00+01:00\n", dt.datetime(2026, 7, 1, 8, 0))}

        for path, (content, expected) in cases.items():
            with self.subTest(path=path):

                self._write_csv(path, content)
                _, data_item, _ = self._capture(_declared(path, schema))

                self.assertIsNone(data_item.table.schema.field("when").type.tz)
                self.assertEqual(expected, data_item.table.column("when").to_pylist()[0])

    def test_csv_datetimes_mixed(self):

        schema = _schema(("id", meta.BasicType.INTEGER), ("when", meta.BasicType.DATETIME))
        self._write_csv("mixed.csv", "id,when\n1,2026-07-01T09:00:00\n2,2026-07-01T09:00:00Z\n")

        with self.assertRaisesRegex(ex.EDataConformance, re.escape("Column [when] mixes date-times")):
            self._capture(_declared("mixed.csv", schema))

    # Capture and decode

    def test_header_only_csv(self):

        self._write_csv("loans.csv", "id,amount,region\n")

        _, data_item, _ = self._capture(_declared("loans.csv", _LOANS_SCHEMA))

        self.assertEqual(0, data_item.table.num_rows)
        self.assertEqual(["id", "amount", "region"], data_item.table.schema.names)

    def test_empty_parquet(self):

        self._write_parquet("loans.parquet", pa.table({"id": pa.array([], pa.int64())}))

        _, data_item, _ = self._capture(_file_schema("loans.parquet"))

        self.assertEqual(0, data_item.table.num_rows)

    def test_missing_file(self):

        with self.assertRaisesRegex(ex.EStorageRequest, re.escape("Capture [loans] failed: File not found [missing.csv]")):
            self._capture(_declared("missing.csv", _LOANS_SCHEMA))

    def test_directory_path(self):

        self.staging.joinpath("folder.csv").mkdir()

        with self.assertRaisesRegex(ex.EStorageRequest, "is not a file"):
            self._capture(_declared("folder.csv", _LOANS_SCHEMA))

    def test_garbled_file(self):

        self.staging.joinpath("loans.parquet").write_bytes(b"not a parquet file")

        with self.assertRaisesRegex(ex.EDataCorruption, re.escape("Capture [loans] failed")):
            self._capture(_file_schema("loans.parquet"))

    def test_size_limit(self):

        self._write_csv("loans.csv", "id,amount,region\n1,10.5,north\n")

        with self.assertRaisesRegex(ex.EStorageRequest, re.escape("exceeds the capture size limit [10] bytes")):
            self._capture(_declared("loans.csv", _LOANS_SCHEMA), size_limit=10)

    def test_upper_case_extension(self):

        self._write_csv("LOANS.CSV", "id,amount,region\n1,10.5,north\n")

        self.assertEqual("CSV", capture.CaptureFormats.for_extension("CSV").format_code)

        file_item, data_item, _ = self._capture(_declared("LOANS.CSV", _LOANS_SCHEMA))

        self.assertEqual([1], data_item.table.column("id").to_pylist())

    def test_unsupported_extensions(self):

        self.assertIsNone(capture.CaptureFormats.file_extension("loans"))
        self.assertIsNone(capture.CaptureFormats.for_extension("txt"))
        self.assertIsNone(capture.CaptureFormats.for_extension("json"))

    def test_provenance_attrs(self):

        content = "id,amount,region\n1,10.5,north\n"
        self._write_csv("2026-09/loans.csv", content)

        file_item, data_item, file_id = self._capture(_declared("2026-09/loans.csv", _LOANS_SCHEMA))

        expected = {
            "trac_import_location_key": "staging",
            "trac_import_location_path": "2026-09/loans.csv",
            "trac_import_file_name": "loans.csv",
            "trac_import_file_size": len(content),
            "trac_import_content_hash": hashlib.sha256(content.encode()).hexdigest()}

        file_attrs = _attrs(file_item.attrs)
        data_attrs = _attrs(data_item.attrs)

        for attrs in [file_attrs, data_attrs]:
            for attr_name, value in expected.items():
                self.assertEqual(value, attrs[attr_name], attr_name)
            self.assertIsInstance(attrs["trac_import_file_modified"], dt.datetime)

        self.assertEqual(file_id.objectId, data_attrs["trac_capture_file"])
        self.assertEqual("DECLARED", data_attrs["trac_import_schema_source"])
        self.assertEqual(3, data_attrs["trac_import_source_columns"])

        self.assertNotIn("trac_capture_file", file_attrs)
        self.assertNotIn("trac_import_schema_source", file_attrs)

        self.assertEqual(content.encode(), file_item.content)

    def test_provenance_no_modified_time(self):

        self._write_csv("loans.csv", "id,amount,region\n1,10.5,north\n")

        original_stat = storage.CommonFileStorage.stat

        def stat_no_mtime(file_storage, storage_path):
            return dc.replace(original_stat(file_storage, storage_path), mtime=None)

        with mock.patch.object(storage.CommonFileStorage, "stat", stat_no_mtime):
            file_item, data_item, _ = self._capture(_declared("loans.csv", _LOANS_SCHEMA))

        self.assertNotIn("trac_import_file_modified", _attrs(file_item.attrs))
        self.assertNotIn("trac_import_file_modified", _attrs(data_item.attrs))


class CaptureGraphTest(unittest.TestCase):

    """Build capture jobs directly, as the platform supplies them to the runtime"""

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    @staticmethod
    def _build(captures: tp.Dict[str, meta.CaptureSource], model: meta.TagSelector = None, objects: dict = None, mapping: dict = None):

        preallocated = [
            dc.replace(util.new_object_id(object_type), objectVersion=0)
            for object_type in [meta.ObjectType.DATA, meta.ObjectType.FILE, meta.ObjectType.STORAGE, meta.ObjectType.STORAGE]
            for _ in captures]

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_DATA,
            importData=meta.ImportDataJob(model=model or meta.TagSelector(), captures=captures))

        job_config = cfg.JobConfig(
            jobId=util.new_object_id(meta.ObjectType.JOB), job=job_def,
            objects=objects or {}, objectMapping=mapping or {},
            preallocatedIds=preallocated)

        sys_config = cfg.RuntimeConfig(properties={
            cfg_p.ConfigKeys.STORAGE_DEFAULT_LOCATION: "test_storage",
            cfg_p.ConfigKeys.STORAGE_DEFAULT_LAYOUT: meta.StorageLayout.OBJECT_ID_LAYOUT.name})

        builder = graph_builder.GraphBuilder(sys_config, job_config)

        return builder.build_job(job_def), builder

    @staticmethod
    def _nodes(job_graph, node_class) -> tp.Dict[str, graph.Node]:
        return {node_id.name: node for node_id, node in job_graph.nodes.items() if isinstance(node, node_class)}

    def test_one_capture(self):

        job_graph, builder = self._build({"loans": _declared("2026-09/loans_2026-09-30.csv", _LOANS_SCHEMA)})

        self.assertEqual({"loans_file:CAPTURE"}, set(self._nodes(job_graph, graph.CaptureFileNode)))
        self.assertEqual({"loans:DECODE"}, set(self._nodes(job_graph, graph.DecodeTableNode)))

        saves = self._nodes(job_graph, graph.SaveDataNode)
        self.assertEqual({"loans:SAVE", "loans_file:SAVE"}, set(saves))

        file_spec = saves["loans_file:SAVE"].spec
        self.assertEqual("loans_2026-09-30.csv", file_spec.definition.name)
        self.assertEqual("csv", file_spec.definition.extension)
        self.assertEqual("text/csv", file_spec.definition.mimeType)

        storage_copy = file_spec.storage.dataItems[file_spec.data_item].incarnations[0].copies[0]
        self.assertTrue(storage_copy.storagePath.endswith("/loans_file.csv.csv"))

        decode_node = self._nodes(job_graph, graph.DecodeTableNode)["loans:DECODE"]
        self.assertEqual(file_spec.primary_id, decode_node.file_id)
        self.assertEqual(1, file_spec.primary_id.objectVersion)

        data_spec = saves["loans:SAVE"].spec
        self.assertEqual(_LOANS_SCHEMA, data_spec.definition.schema)

        self.assertTrue(all(len(ids) == 0 for ids in builder.unallocated_ids().values()))

    def test_several_captures(self):

        job_graph, builder = self._build({
            "loans": _declared("loans.csv", _LOANS_SCHEMA),
            "rates": _file_schema("rates.parquet"),
            "fx": _file_schema("FX.ARROW")})

        saves = self._nodes(job_graph, graph.SaveDataNode)
        self.assertEqual({"loans:SAVE", "loans_file:SAVE", "rates:SAVE", "rates_file:SAVE", "fx:SAVE", "fx_file:SAVE"}, set(saves))

        self.assertIsNotNone(saves["rates:SAVE"].spec_id)
        self.assertEqual("FX.ARROW", saves["fx_file:SAVE"].spec.definition.name)
        self.assertEqual("ARROW", saves["fx_file:SAVE"].spec.definition.extension)
        self.assertEqual("application/vnd.apache.arrow.file", saves["fx_file:SAVE"].spec.definition.mimeType)

        self.assertTrue(all(len(ids) == 0 for ids in builder.unallocated_ids().values()))

    def test_schema_id_pinned(self):

        schema_id = util.new_object_id(meta.ObjectType.SCHEMA)
        latest_selector = util.selector_for_latest(schema_id)

        source = _declared("loans.csv", None)
        source = dc.replace(source, schema=None, schemaId=latest_selector)

        job_graph, _ = self._build(
            {"loans": source},
            objects={util.object_key(schema_id): meta.ObjectDefinition(meta.ObjectType.SCHEMA, schema=_LOANS_SCHEMA)},
            mapping={util.object_key(latest_selector): schema_id})

        data_spec = self._nodes(job_graph, graph.SaveDataNode)["loans:SAVE"].spec

        self.assertIsNone(data_spec.definition.schema)
        self.assertEqual(schema_id.objectId, data_spec.definition.schemaId.objectId)
        self.assertEqual(schema_id.objectVersion, data_spec.definition.schemaId.objectVersion)
        self.assertEqual(_LOANS_SCHEMA, data_spec.schema)

        decode_node = self._nodes(job_graph, graph.DecodeTableNode)["loans:DECODE"]
        self.assertEqual(_LOANS_SCHEMA, decode_node.schema)

    def test_rejected(self):

        model_selector = util.selector_for(util.new_object_id(meta.ObjectType.MODEL))
        declared_no_schema = _declared("loans.csv", None)
        file_with_schema = dc.replace(_file_schema("loans.parquet"), schema=_LOANS_SCHEMA)
        no_schema_source = dc.replace(_declared("loans.csv", _LOANS_SCHEMA), schemaSource=meta.CaptureSchemaSource.CAPTURE_SCHEMA_SOURCE_NOT_SET)

        cases = [
            ("model with captures", {"loans": _declared("loans.csv", _LOANS_SCHEMA)}, model_selector, "cannot use a model with captures"),
            ("declared with no schema", {"loans": declared_no_schema}, None, "declared schema source but no schema"),
            ("file schema with a schema", {"loans": file_with_schema}, None, "can't also declare one"),
            ("file schema on CSV", {"loans": _file_schema("loans.csv")}, None, "needs a declared schema"),
            ("no schema source", {"loans": no_schema_source}, None, "requires a schema source"),
            ("no extension", {"loans": _declared("loans", _LOANS_SCHEMA)}, None, "unsupported file type"),
            ("unsupported extension", {"loans": _declared("loans.txt", _LOANS_SCHEMA)}, None, "unsupported file type"),
            ("no location", {"loans": meta.CaptureSource(schemaSource=meta.CaptureSchemaSource.CAPTURE_SCHEMA_FILE)}, None, "requires a storage location"),
            ("output clash", {"a": _declared("a.csv", _LOANS_SCHEMA), "a_file": _declared("b.csv", _LOANS_SCHEMA)}, None, "clashes with the file output")]

        for case_name, captures, model, message in cases:
            with self.subTest(case=case_name):
                with self.assertRaisesRegex(ex.EJobValidation, re.escape(message)):
                    self._build(captures, model=model)


class CaptureJobTest(CaptureTestBase):

    """Run capture jobs through the runtime, as on the platform"""

    def _run_job(
            self, captures: tp.Dict[str, meta.CaptureSource],
            objects: dict = None, mapping: dict = None,
            capture_size_mb: tp.Optional[int] = None) -> cfg.JobResult:

        job_id = util.new_object_id(meta.ObjectType.JOB)
        job_def = meta.JobDefinition(jobType=meta.JobType.IMPORT_DATA, importData=meta.ImportDataJob(captures=captures))
        job_config = cfg.JobConfig(job_id, job_def, objects=objects or {}, objectMapping=mapping or {})

        trac_runtime = runtime.TracRuntime(
            self._sys_config(capture_size_mb=capture_size_mb),
            scratch_dir=self.tmp_dir.joinpath("scratch"))

        trac_runtime.pre_start()

        with trac_runtime as rt:
            rt.submit_job(job_config)
            return rt.wait_for_job(job_id)

    @staticmethod
    def _output(result: cfg.JobResult, output_name: str) -> tp.Tuple[meta.ObjectDefinition, tp.Dict[str, tp.Any], str]:
        output_key = util.object_key(result.result.outputs[output_name])
        output_attrs = result.attrs[output_key].attrs if output_key in result.attrs else []
        return result.objects[output_key], _attrs(output_attrs), output_key

    @staticmethod
    def _storage(result: cfg.JobResult, storage_selector: meta.TagSelector) -> meta.StorageDefinition:
        storage_id = next(oid for oid in result.objectIds if oid.objectId == storage_selector.objectId)
        return result.objects[util.object_key(storage_id)].storage

    def _read_back(self, data_def: meta.DataDefinition, storage_def: meta.StorageDefinition, schema: meta.SchemaDefinition):
        data_item = next(iter(storage_def.dataItems.keys()))
        storage_copy = storage_def.dataItems[data_item].incarnations[0].copies[0]
        storage_manager = storage.StorageManager(self._sys_config())
        arrow_schema = data.DataMapping.trac_to_arrow_schema(schema)
        return storage_manager.get_data_storage("test_data").read_table(
            storage_copy.storagePath, storage_copy.storageFormat, arrow_schema)

    def test_capture_job(self):

        csv_content = "id,amount,region\n1,10.5,north\n2,20,south\n"
        self._write_csv("loans.csv", csv_content)

        self._write_parquet("rates.parquet", pa.table({
            "ccy": pa.array(["EUR", "GBP"], pa.large_string()),
            "rate": pa.array([1.17, 1.34], pa.float32())}))

        schema_id = util.new_object_id(meta.ObjectType.SCHEMA)
        latest_selector = util.selector_for_latest(schema_id)

        captures = {
            "loans": dc.replace(_declared("loans.csv", None), schemaId=latest_selector),
            "rates": _file_schema("rates.parquet")}

        result = self._run_job(
            captures,
            objects={util.object_key(schema_id): meta.ObjectDefinition(meta.ObjectType.SCHEMA, schema=_LOANS_SCHEMA)},
            mapping={util.object_key(latest_selector): schema_id})

        self.assertEqual({"loans", "loans_file", "rates", "rates_file"}, set(result.result.outputs.keys()))
        self.assertEqual(8, len(result.objectIds))

        loans_obj, loans_attrs, _ = self._output(result, "loans")
        loans_file_obj, loans_file_attrs, _ = self._output(result, "loans_file")

        self.assertEqual(schema_id.objectId, loans_obj.data.schemaId.objectId)
        self.assertIsNone(loans_obj.data.schema)
        self.assertEqual(2, loans_obj.data.rowCount)

        self.assertEqual("loans.csv", loans_file_obj.file.name)
        self.assertEqual(len(csv_content), loans_file_obj.file.size)
        self.assertEqual(result.result.outputs["loans_file"].objectId, loans_attrs["trac_capture_file"])
        self.assertEqual(loans_attrs["trac_import_content_hash"], loans_file_attrs["trac_import_content_hash"])

        loans_storage = self._storage(result, loans_obj.data.storageId)
        loans_table = self._read_back(loans_obj.data, loans_storage, _LOANS_SCHEMA)
        self.assertEqual([decimal.Decimal("10.5"), decimal.Decimal("20")], loans_table.column("amount").to_pylist())

        rates_obj, rates_attrs, _ = self._output(result, "rates")
        rates_fields = {f.fieldName: f.fieldType for f in rates_obj.data.schema.table.fields}
        self.assertEqual({"ccy": meta.BasicType.STRING, "rate": meta.BasicType.FLOAT}, rates_fields)

        rates_storage = self._storage(result, rates_obj.data.storageId)
        rates_table = self._read_back(rates_obj.data, rates_storage, rates_obj.data.schema)
        self.assertEqual(["EUR", "GBP"], rates_table.column("ccy").to_pylist())

        loans_file_storage = self._storage(result, loans_file_obj.file.storageId)
        storage_item = loans_file_storage.dataItems[loans_file_obj.file.dataItem]
        stored_path = self.tmp_dir.joinpath("data", storage_item.incarnations[0].copies[0].storagePath)
        self.assertEqual(csv_content.encode(), stored_path.read_bytes())
        self.assertTrue(stored_path.name.startswith("loans_file"))

    def test_capture_job_errors(self):

        self._write_csv("loans.csv", "id,amount\n1,10.5\n")

        captures = {
            "missing": _declared("missing.csv", _LOANS_SCHEMA),
            "bad_columns": _declared("loans.csv", _LOANS_SCHEMA)}

        with self.assertRaises(ex.ETrac) as error_context:
            self._run_job(captures)

        error_text = str(error_context.exception)
        self.assertIn("Capture [missing] failed", error_text)
        self.assertIn("Capture [bad_columns] failed", error_text)

    def test_capture_size_limit_property(self):

        self.staging.joinpath("big.csv").write_bytes(b"id\n" + b"1\n" * (1024 * 1024))

        with self.assertRaisesRegex(ex.EStorageRequest, re.escape(cfg_p.ConfigKeys.RUNTIME_LIMIT_CAPTURE_SIZE)):
            self._run_job({"big": _declared("big.csv", _schema(("id", meta.BasicType.INTEGER)))}, capture_size_mb=1)


class CaptureDevModeTest(CaptureTestBase):

    def _write_config(self, job_config: dict) -> tp.Tuple[pathlib.Path, pathlib.Path]:

        sys_config = {
            "properties": {
                "storage.default.location": "test_data",
                "storage.default.format": "CSV"},
            "resources": {
                "test_data": {"resourceType": "INTERNAL_STORAGE", "protocol": "LOCAL", "properties": {"rootPath": "data"}},
                "staging": {"resourceType": "EXTERNAL_STORAGE", "protocol": "LOCAL", "properties": {"rootPath": "staging"}}}}

        sys_config_path = self.tmp_dir.joinpath("sys_config.yaml")
        job_config_path = self.tmp_dir.joinpath("job_config.yaml")

        sys_config_path.write_text(yaml.safe_dump(sys_config))
        job_config_path.write_text(yaml.safe_dump(job_config))

        return job_config_path, sys_config_path

    def test_schema_file_and_reruns(self):

        self._write_csv("loans.csv", "ID,Amount,Region\n1,10.5,north\n")

        self.tmp_dir.joinpath("schemas").mkdir()
        self.tmp_dir.joinpath("schemas/loans.csv").write_text(
            "field_name,field_type,label\nid,INTEGER,ID\namount,DECIMAL,Amount\nregion,STRING,Region\n")

        job_config = {"job": {"importData": {
            "captures": {"loans": {
                "location": {"storageKey": "staging", "storagePath": "loans.csv"},
                "schemaSource": "CAPTURE_SCHEMA_DECLARED",
                "schema": "schemas/loans.csv"}},
            "outputs": {
                "loans": "outputs/loans.csv",
                "loans_file": "outputs/loans_file.csv"}}}}

        job_config_path, sys_config_path = self._write_config(job_config)

        launch.launch_job(job_config_path, sys_config_path, dev_mode=True)
        launch.launch_job(job_config_path, sys_config_path, dev_mode=True)

        outputs_dir = self.tmp_dir.joinpath("data/outputs")
        self.assertEqual(
            ["loans-2.csv", "loans.csv", "loans_file-2.csv", "loans_file.csv"],
            sorted(p.name for p in outputs_dir.iterdir()))

        self.assertEqual(
            "\"id\",\"amount\",\"region\"",
            outputs_dir.joinpath("loans.csv").read_text().splitlines()[0])

        self.assertEqual(
            outputs_dir.joinpath("loans_file.csv").read_bytes(),
            self.staging.joinpath("loans.csv").read_bytes())

    def test_missing_output(self):

        self._write_csv("loans.csv", "id,amount,region\n1,10.5,north\n")

        job_config = {"job": {"importData": {
            "captures": {"loans": {
                "location": {"storageKey": "staging", "storagePath": "loans.csv"},
                "schemaSource": "CAPTURE_SCHEMA_FILE"}},
            "outputs": {"loans": "outputs/loans.csv"}}}}

        job_config_path, sys_config_path = self._write_config(job_config)

        with self.assertRaisesRegex(ex.EJobValidation, re.escape("Missing required output [loans_file]")):
            launch.launch_job(job_config_path, sys_config_path, dev_mode=True)
