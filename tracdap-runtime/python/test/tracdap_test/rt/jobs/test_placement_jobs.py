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

import decimal
import hashlib
import pathlib
import re
import tempfile
import typing as tp
import unittest

import pyarrow as pa
import pyarrow.feather as pa_ft
import pyarrow.parquet as pa_pq
import yaml

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.launch as launch
import tracdap.rt.metadata as meta
import tracdap.rt._impl.runtime as runtime  # noqa
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.plugins as plugins  # noqa
import tracdap.rt._impl.core.type_system as types  # noqa
import tracdap.rt._impl.core.util as util  # noqa
import tracdap.rt._impl.exec.graph as graph  # noqa
import tracdap.rt._impl.exec.graph_builder as graph_builder  # noqa


plugins.PluginManagerImpl.register_core_plugins()


_LOANS_SCHEMA = meta.SchemaDefinition(
    schemaType=meta.SchemaType.TABLE,
    partType=meta.PartType.PART_ROOT,
    table=meta.TableSchema([
        meta.FieldSchema("id", 0, meta.BasicType.INTEGER),
        meta.FieldSchema("amount", 1, meta.BasicType.DECIMAL),
        meta.FieldSchema("region", 2, meta.BasicType.STRING)]))

_LOANS_CSV = "id,amount,region\n1,10.5,north\n2,20,south\n"


def _placement(data_selector: meta.TagSelector, path: str, storage_key: str = "outbound") -> meta.PlacementTarget:

    return meta.PlacementTarget(
        dataId=data_selector,
        location=meta.ExternalLocation(storage_key, path))


def _sha256(path: pathlib.Path) -> str:

    return hashlib.sha256(path.read_bytes()).hexdigest()


class PlacementTestBase(unittest.TestCase):

    tmp_dir: pathlib.Path
    staging: pathlib.Path
    outbound: pathlib.Path

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    def setUp(self) -> None:

        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = pathlib.Path(self._tmp.name)

        for storage_dir in ["data", "staging", "outbound"]:
            self.tmp_dir.joinpath(storage_dir).mkdir()

        self.staging = self.tmp_dir.joinpath("staging")
        self.outbound = self.tmp_dir.joinpath("outbound")
        self._runs = 0

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _sys_config(self):

        sys_config = cfg.RuntimeConfig()
        sys_config.properties["storage.default.location"] = "test_data"
        sys_config.properties["storage.default.format"] = "CSV"

        sys_config.resources["test_data"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.INTERNAL_STORAGE, protocol="LOCAL",
            properties={"rootPath": str(self.tmp_dir.joinpath("data"))})

        for storage_key, storage_dir in [("staging", self.staging), ("outbound", self.outbound)]:
            sys_config.resources[storage_key] = meta.ResourceDefinition(
                resourceType=meta.ResourceType.EXTERNAL_STORAGE, protocol="LOCAL",
                properties={"rootPath": str(storage_dir)})

        return sys_config


class PlacementGraphTest(unittest.TestCase):

    """Build placement jobs directly, as the platform supplies them to the runtime"""

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    @staticmethod
    def _dataset() -> tp.Tuple[meta.TagSelector, tp.Dict[str, meta.ObjectDefinition], tp.Dict[str, meta.TagHeader]]:

        data_id = util.new_object_id(meta.ObjectType.DATA)
        storage_id = util.new_object_id(meta.ObjectType.STORAGE)
        data_selector = util.selector_for_latest(data_id)

        part_key = meta.PartKey(opaqueKey="part-root", partType=meta.PartType.PART_ROOT)
        data_item = f"data/table/{data_id.objectId}/part-root/snap-0/delta-0"

        data_def = meta.DataDefinition(
            schema=_LOANS_SCHEMA,
            parts={"part-root": meta.DataPartition(
                part_key, snap=meta.DataSnapshot(0, deltas=[meta.DataDelta(0, dataItem=data_item)]))},
            storageId=util.selector_for(storage_id))

        storage_def = meta.StorageDefinition(dataItems={data_item: meta.StorageItem(incarnations=[
            meta.StorageIncarnation(
                copies=[meta.StorageCopy("test_data", f"{data_item}.csv", "CSV", meta.CopyStatus.COPY_AVAILABLE)],
                incarnationIndex=0, incarnationStatus=meta.IncarnationStatus.INCARNATION_AVAILABLE)])})

        objects = {
            util.object_key(data_id): meta.ObjectDefinition(meta.ObjectType.DATA, data=data_def),
            util.object_key(storage_id): meta.ObjectDefinition(meta.ObjectType.STORAGE, storage=storage_def)}

        mapping = {
            util.object_key(data_selector): data_id,
            util.object_key(util.selector_for(storage_id)): storage_id}

        return data_selector, objects, mapping

    @staticmethod
    def _build(export_data: meta.ExportDataJob, objects: dict = None, mapping: dict = None):

        job_def = meta.JobDefinition(jobType=meta.JobType.EXPORT_DATA, exportData=export_data)

        job_config = cfg.JobConfig(
            jobId=util.new_object_id(meta.ObjectType.JOB), job=job_def,
            objects=objects or {}, objectMapping=mapping or {})

        sys_config = cfg.RuntimeConfig(properties={
            cfg_p.ConfigKeys.STORAGE_DEFAULT_LOCATION: "test_data",
            cfg_p.ConfigKeys.STORAGE_DEFAULT_LAYOUT: meta.StorageLayout.OBJECT_ID_LAYOUT.name})

        return graph_builder.GraphBuilder(sys_config, job_config).build_job(job_def)

    @staticmethod
    def _nodes(job_graph, node_class) -> tp.Dict[str, graph.Node]:
        return {node_id.name: node for node_id, node in job_graph.nodes.items() if isinstance(node, node_class)}

    def test_placements_and_barrier(self):

        data_selector, objects, mapping = self._dataset()

        export_data = meta.ExportDataJob(placements={
            "loans_csv": _placement(data_selector, "out/loans.csv"),
            "loans_parquet": _placement(data_selector, "out/loans.parquet")})

        job_graph = self._build(export_data, objects, mapping)

        prepare_nodes = self._nodes(job_graph, graph.PreparePlacementNode)
        place_nodes = self._nodes(job_graph, graph.PlaceFileNode)

        self.assertEqual({"loans_csv:PREPARE", "loans_parquet:PREPARE"}, set(prepare_nodes))
        self.assertEqual({"loans_csv:PLACE", "loans_parquet:PLACE"}, set(place_nodes))
        self.assertEqual("PARQUET", prepare_nodes["loans_parquet:PREPARE"].file_format.format_code)
        self.assertEqual(meta.PlacementConflict.PLACEMENT_SUFFIX, prepare_nodes["loans_csv:PREPARE"].conflict)

        # The pinned DATA version is recorded, not the latest-version selector
        self.assertEqual(mapping[util.object_key(data_selector)], prepare_nodes["loans_csv:PREPARE"].data_id)

        prepare_ids = {node_id for node_id, node in job_graph.nodes.items() if isinstance(node, graph.PreparePlacementNode)}

        for place_node in place_nodes.values():
            self.assertTrue(prepare_ids.issubset(place_node.dependencies.keys()))

        result_node = next(n for n in job_graph.nodes.values() if isinstance(n, graph.JobResultNode))
        self.assertEqual({"loans_csv", "loans_parquet"}, set(result_node.placements))
        self.assertEqual({}, result_node.named_outputs)

    def test_rejected(self):

        data_selector, objects, mapping = self._dataset()
        model_selector = util.selector_for(util.new_object_id(meta.ObjectType.MODEL))

        cases = [
            ("model", meta.ExportDataJob(model=model_selector, placements={"a": _placement(data_selector, "a.csv")}), "takes no model"),
            ("storage access", meta.ExportDataJob(storageAccess=["outbound"], placements={"a": _placement(data_selector, "a.csv")}), "does not use [storageAccess]"),
            ("output attrs", meta.ExportDataJob(outputAttrs=[meta.TagUpdate(attrName="x")], placements={"a": _placement(data_selector, "a.csv")}), "does not use [outputAttrs]"),
            ("no placements", meta.ExportDataJob(), "requires at least one placement"),
            ("no dataset", meta.ExportDataJob(placements={"a": meta.PlacementTarget(location=meta.ExternalLocation("outbound", "a.csv"))}), "requires a dataset"),
            ("no location", meta.ExportDataJob(placements={"a": meta.PlacementTarget(dataId=data_selector)}), "requires a storage location"),
            ("no extension", meta.ExportDataJob(placements={"a": _placement(data_selector, "a")}), "unsupported file type"),
            ("unsupported extension", meta.ExportDataJob(placements={"a": _placement(data_selector, "a.xlsx")}), "unsupported file type"),
            ("same path", meta.ExportDataJob(placements={"a": _placement(data_selector, "out/A.csv"), "b": _placement(data_selector, "out/a.csv")}), "write to the same path")]

        for case_name, export_data, message in cases:
            with self.subTest(case=case_name):
                with self.assertRaisesRegex(ex.EJobValidation, re.escape(message)):
                    self._build(export_data, objects, mapping)


class PlacementJobTest(PlacementTestBase):

    """Run placement jobs through the runtime, as on the platform"""

    def _run_job(self, job_def: meta.JobDefinition, objects: dict = None, mapping: dict = None) -> cfg.JobResult:

        self._runs += 1
        scratch_dir = self.tmp_dir.joinpath(f"scratch-{self._runs}")
        scratch_dir.mkdir()

        job_id = util.new_object_id(meta.ObjectType.JOB)
        job_config = cfg.JobConfig(job_id, job_def, objects=objects or {}, objectMapping=mapping or {})

        trac_runtime = runtime.TracRuntime(self._sys_config(), scratch_dir=scratch_dir, scratch_dir_persist=True)
        trac_runtime.pre_start()

        with trac_runtime as rt:
            rt.submit_job(job_config)
            return rt.wait_for_job(job_id)

    def _capture(self, path: str, schema: meta.SchemaDefinition = _LOANS_SCHEMA, storage_key: str = "staging"):

        capture = meta.CaptureSource(
            location=meta.ExternalLocation(storage_key, path),
            schemaSource=meta.CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED,
            schema=schema)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_DATA,
            importData=meta.ImportDataJob(captures={"loans": capture}))

        return self._run_job(job_def)

    def _captured_dataset(self, content: str = _LOANS_CSV):

        self.staging.joinpath("loans.csv").write_text(content)
        result = self._capture("loans.csv")

        data_id = next(oid for oid in result.objectIds if oid.objectId == result.result.outputs["loans"].objectId)
        objects = dict(result.objects)
        mapping = {
            util.object_key(selector(oid)): oid
            for oid in result.objectIds
            for selector in [util.selector_for, util.selector_for_latest]}

        return util.selector_for(data_id), data_id, objects, mapping

    def _place(self, placements: tp.Dict[str, meta.PlacementTarget], objects, mapping, conflict=None) -> cfg.JobResult:

        export_data = meta.ExportDataJob(placements=placements)

        if conflict is not None:
            export_data.placementConflict = conflict

        return self._run_job(meta.JobDefinition(jobType=meta.JobType.EXPORT_DATA, exportData=export_data), objects, mapping)

    def test_formats_and_records(self):

        data_selector, data_id, objects, mapping = self._captured_dataset()

        result = self._place({
            "loans_csv": _placement(data_selector, "2026-09/loans.csv"),
            "loans_parquet": _placement(data_selector, "2026-09/loans.parquet"),
            "loans_arrow": _placement(data_selector, "2026-09/LOANS.ARROW")}, objects, mapping)

        self.assertEqual({}, result.result.outputs)
        self.assertEqual(0, len(result.objectIds))

        records = result.result.placements
        self.assertEqual({"loans_csv", "loans_parquet", "loans_arrow"}, set(records))

        expected = {
            "loans_csv": ("2026-09/loans.csv", "CSV", "text/csv"),
            "loans_parquet": ("2026-09/loans.parquet", "PARQUET", "application/vnd.apache.parquet"),
            "loans_arrow": ("2026-09/LOANS.ARROW", "ARROW_FILE", "application/vnd.apache.arrow.file")}

        for name, (path, format_code, mime_type) in expected.items():

            record = records[name]
            placed_file = self.outbound.joinpath(path)

            self.assertTrue(placed_file.exists(), name)
            self.assertEqual(path, record.location.storagePath)
            self.assertEqual("outbound", record.location.storageKey)
            self.assertEqual(format_code, record.format)
            self.assertEqual(mime_type, record.mimeType)
            self.assertEqual(placed_file.stat().st_size, record.size)
            self.assertEqual(_sha256(placed_file), record.contentHash)
            self.assertEqual(data_id.objectId, record.dataId.objectId)
            self.assertEqual(data_id.objectVersion, record.dataId.objectVersion)

        parquet_table = pa_pq.read_table(self.outbound.joinpath("2026-09/loans.parquet"))
        self.assertEqual(["north", "south"], parquet_table.column("region").to_pylist())

        arrow_table = pa_ft.read_table(self.outbound.joinpath("2026-09/LOANS.ARROW"))
        self.assertEqual([1, 2], arrow_table.column("id").to_pylist())

    def test_round_trip(self):

        data_selector, _, objects, mapping = self._captured_dataset()

        result = self._place({
            "csv": _placement(data_selector, "round_trip/loans.csv"),
            "parquet": _placement(data_selector, "round_trip/loans.parquet"),
            "arrow": _placement(data_selector, "round_trip/loans.arrow")}, objects, mapping)

        for name, record in result.result.placements.items():
            with self.subTest(format=name):

                captured = self._capture(record.location.storagePath, storage_key="outbound")

                file_key = util.object_key(captured.result.outputs["loans_file"])
                file_attrs = {a.attrName: types.MetadataCodec.decode_value(a.value) for a in captured.attrs[file_key].attrs}
                self.assertEqual(record.contentHash, file_attrs["trac_import_content_hash"])

                data_key = util.object_key(captured.result.outputs["loans"])
                self.assertEqual(2, captured.objects[data_key].data.rowCount)

    def test_suffix_policy(self):

        data_selector, _, objects, mapping = self._captured_dataset()
        placements = {"loans": _placement(data_selector, "out/loans.csv")}

        self.outbound.joinpath("out").mkdir()
        self.outbound.joinpath("out/loans.csv").write_text("existing")
        self.outbound.joinpath("out/loans-2.csv").write_text("existing")

        result = self._place(placements, objects, mapping)

        self.assertEqual("out/loans-3.csv", result.result.placements["loans"].location.storagePath)
        self.assertEqual("existing", self.outbound.joinpath("out/loans.csv").read_text())
        self.assertTrue(self.outbound.joinpath("out/loans-3.csv").read_text().startswith("\"id\""))

    def test_explicit_suffix_policy(self):

        data_selector, _, objects, mapping = self._captured_dataset()

        self.outbound.joinpath("loans.csv").write_text("existing")

        result = self._place(
            {"loans": _placement(data_selector, "loans.csv")}, objects, mapping,
            conflict=meta.PlacementConflict.PLACEMENT_SUFFIX)

        self.assertEqual("loans-2.csv", result.result.placements["loans"].location.storagePath)
        self.assertEqual("existing", self.outbound.joinpath("loans.csv").read_text())

    def test_overwrite_policy(self):

        data_selector, _, objects, mapping = self._captured_dataset()

        self.outbound.joinpath("loans.csv").write_text("existing")

        result = self._place(
            {"loans": _placement(data_selector, "loans.csv")}, objects, mapping,
            conflict=meta.PlacementConflict.PLACEMENT_OVERWRITE)

        self.assertEqual("loans.csv", result.result.placements["loans"].location.storagePath)
        self.assertEqual(result.result.placements["loans"].contentHash, _sha256(self.outbound.joinpath("loans.csv")))

    def test_fail_policy_writes_nothing(self):

        data_selector, _, objects, mapping = self._captured_dataset()

        self.outbound.joinpath("exists.csv").write_text("existing")

        with self.assertRaises(ex.ETrac) as error_context:
            self._place({
                "clash": _placement(data_selector, "exists.csv"),
                "new": _placement(data_selector, "new.csv")}, objects, mapping,
                conflict=meta.PlacementConflict.PLACEMENT_FAIL)

        self.assertIn("Placement [clash] failed: File already exists", str(error_context.exception))
        self.assertFalse(self.outbound.joinpath("new.csv").exists())
        self.assertEqual("existing", self.outbound.joinpath("exists.csv").read_text())

    def test_fail_policy_no_existing_files(self):

        data_selector, _, objects, mapping = self._captured_dataset()

        result = self._place({
            "loans_csv": _placement(data_selector, "fresh/loans.csv"),
            "loans_parquet": _placement(data_selector, "fresh/loans.parquet")}, objects, mapping,
            conflict=meta.PlacementConflict.PLACEMENT_FAIL)

        for name, path in [("loans_csv", "fresh/loans.csv"), ("loans_parquet", "fresh/loans.parquet")]:
            record = result.result.placements[name]
            self.assertEqual(path, record.location.storagePath)
            self.assertEqual(_sha256(self.outbound.joinpath(path)), record.contentHash)

    def test_write_failure(self):

        data_selector, _, objects, mapping = self._captured_dataset()

        self.outbound.joinpath("taken.csv").mkdir()

        with self.assertRaises(ex.ETrac) as error_context:
            self._place(
                {"loans": _placement(data_selector, "taken.csv")}, objects, mapping,
                conflict=meta.PlacementConflict.PLACEMENT_OVERWRITE)

        self.assertIn("taken.csv", str(error_context.exception))

    def test_empty_dataset(self):

        data_selector, _, objects, mapping = self._captured_dataset("id,amount,region\n")

        result = self._place({"loans": _placement(data_selector, "empty.csv")}, objects, mapping)

        self.assertEqual("\"id\",\"amount\",\"region\"", self.outbound.joinpath("empty.csv").read_text().strip())
        self.assertEqual(self.outbound.joinpath("empty.csv").stat().st_size, result.result.placements["loans"].size)

    def test_scratch_files_removed(self):

        data_selector, _, objects, mapping = self._captured_dataset()

        self._place({"loans": _placement(data_selector, "loans.csv")}, objects, mapping)

        self.outbound.joinpath("taken.csv").mkdir()

        with self.assertRaises(ex.ETrac):
            self._place(
                {"loans": _placement(data_selector, "taken.csv")}, objects, mapping,
                conflict=meta.PlacementConflict.PLACEMENT_OVERWRITE)

        for scratch_dir in self.tmp_dir.glob("scratch-*"):
            placement_dir = scratch_dir.joinpath("placements")
            leftover = list(placement_dir.iterdir()) if placement_dir.exists() else []
            self.assertEqual([], leftover, str(scratch_dir))


class PlacementDevModeTest(PlacementTestBase):

    def _write_config(self, job_config: dict) -> tp.Tuple[pathlib.Path, pathlib.Path]:

        sys_config = {
            "properties": {
                "storage.default.location": "test_data",
                "storage.default.format": "CSV"},
            "resources": {
                "test_data": {"resourceType": "INTERNAL_STORAGE", "protocol": "LOCAL", "properties": {"rootPath": "data"}},
                "outbound": {"resourceType": "EXTERNAL_STORAGE", "protocol": "LOCAL", "properties": {"rootPath": "outbound"}}}}

        sys_config_path = self.tmp_dir.joinpath("sys_config.yaml")
        job_config_path = self.tmp_dir.joinpath("job_config.yaml")

        sys_config_path.write_text(yaml.safe_dump(sys_config))
        job_config_path.write_text(yaml.safe_dump(job_config))

        return job_config_path, sys_config_path

    def test_csv_source_with_schema_file_and_reruns(self):

        self.tmp_dir.joinpath("data/inputs").mkdir()
        self.tmp_dir.joinpath("data/inputs/loans.csv").write_text(_LOANS_CSV)

        self.tmp_dir.joinpath("schemas").mkdir()
        self.tmp_dir.joinpath("schemas/loans.csv").write_text(
            "field_name,field_type,label\nid,INTEGER,ID\namount,DECIMAL,Amount\nregion,STRING,Region\n")

        job_config = {"job": {"exportData": {"placements": {
            "loans": {
                "dataId": {"path": "inputs/loans.csv", "schema": "schemas/loans.csv"},
                "location": {"storageKey": "outbound", "storagePath": "dev/loans.parquet"}}}}}}

        job_config_path, sys_config_path = self._write_config(job_config)

        launch.launch_job(job_config_path, sys_config_path, dev_mode=True)
        launch.launch_job(job_config_path, sys_config_path, dev_mode=True)

        placed = sorted(p.name for p in self.outbound.joinpath("dev").iterdir())
        self.assertEqual(["loans-2.parquet", "loans.parquet"], placed)

        table = pa_pq.read_table(self.outbound.joinpath("dev/loans.parquet"))
        self.assertEqual([decimal.Decimal("10.5"), decimal.Decimal("20")], table.column("amount").to_pylist())

    def test_parquet_source_without_schema(self):

        self.tmp_dir.joinpath("data/inputs").mkdir()
        pa_pq.write_table(
            pa.table({"ccy": ["EUR", "GBP"], "rate": [1.17, 1.34]}),
            self.tmp_dir.joinpath("data/inputs/rates.parquet"))

        job_config = {"job": {"exportData": {"placements": {
            "rates": {
                "dataId": "inputs/rates.parquet",
                "location": {"storageKey": "outbound", "storagePath": "rates.csv"}}}}}}

        job_config_path, sys_config_path = self._write_config(job_config)

        launch.launch_job(job_config_path, sys_config_path, dev_mode=True)

        lines = self.outbound.joinpath("rates.csv").read_text().splitlines()
        self.assertEqual("\"ccy\",\"rate\"", lines[0])
        self.assertEqual(3, len(lines))

    def test_placement_in_job_group(self):

        self.tmp_dir.joinpath("data/inputs").mkdir()
        pa_pq.write_table(
            pa.table({"ccy": ["EUR", "GBP"], "rate": [1.17, 1.34]}),
            self.tmp_dir.joinpath("data/inputs/rates.parquet"))

        job_config = {"job": {"jobGroup": {"sequential": {"jobs": [
            {"exportData": {"placements": {"rates": {
                "dataId": "inputs/rates.parquet",
                "location": {"storageKey": "outbound", "storagePath": "group/rates.arrow"}}}}}]}}}}

        job_config_path, sys_config_path = self._write_config(job_config)

        launch.launch_job(job_config_path, sys_config_path, dev_mode=True)

        table = pa_ft.read_table(self.outbound.joinpath("group/rates.arrow"))
        self.assertEqual(["EUR", "GBP"], table.column("ccy").to_pylist())
