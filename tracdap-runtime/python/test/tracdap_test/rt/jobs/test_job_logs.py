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
import json
import pathlib
import tempfile
import typing as tp
import unittest
import unittest.mock as mock

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as meta
import tracdap.rt._impl.runtime as runtime  # noqa
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.plugins as plugins  # noqa
import tracdap.rt._impl.core.storage as storage  # noqa
import tracdap.rt._impl.core.util as util  # noqa


plugins.PluginManagerImpl.register_core_plugins()


_LOANS_SCHEMA = meta.SchemaDefinition(
    schemaType=meta.SchemaType.TABLE,
    partType=meta.PartType.PART_ROOT,
    table=meta.TableSchema([
        meta.FieldSchema("id", 0, meta.BasicType.INTEGER),
        meta.FieldSchema("amount", 1, meta.BasicType.DECIMAL)]))


class JobLogTestBase(unittest.TestCase):

    """
    Run jobs through the runtime with job results and job logs enabled, as on the platform

    The job result is read back from result storage, for both succeeded and failed jobs.
    Saving the job log can be made to fail with fail_log_save().
    """

    RESULT_STORAGE_KEY = "test_data"
    RESULT_STORAGE_PATH = "results/trac_job_result.json"

    tmp_dir: pathlib.Path

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    def setUp(self) -> None:

        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = pathlib.Path(self._tmp.name)

        for storage_dir in ["data", "staging", "outbound", "scratch"]:
            self.tmp_dir.joinpath(storage_dir).mkdir()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _sys_config(self) -> cfg.RuntimeConfig:

        sys_config = cfg.RuntimeConfig()
        sys_config.properties["storage.default.location"] = "test_data"
        sys_config.properties["storage.default.format"] = "CSV"

        sys_config.resources["test_data"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.INTERNAL_STORAGE, protocol="LOCAL",
            properties={"rootPath": str(self.tmp_dir.joinpath("data"))})

        for storage_key in ["staging", "outbound"]:
            sys_config.resources[storage_key] = meta.ResourceDefinition(
                resourceType=meta.ResourceType.EXTERNAL_STORAGE, protocol="LOCAL",
                properties={"rootPath": str(self.tmp_dir.joinpath(storage_key))})

        return sys_config

    def _run_job(
            self, job_def: meta.JobDefinition,
            objects: dict = None, mapping: dict = None,
            preallocated_ids: tp.List[meta.TagHeader] = None) \
            -> tp.Tuple[cfg.JobResult, tp.Optional[Exception]]:

        job_id = util.new_object_id(meta.ObjectType.JOB)
        result_id = util.new_object_id(meta.ObjectType.RESULT)

        job_config = cfg.JobConfig(
            job_id, job_def, resultId=result_id,
            objects=objects or {}, objectMapping=mapping or {},
            preallocatedIds=preallocated_ids or [])

        job_config.properties[cfg_p.ConfigKeys.RESULT_ENABLED] = "true"
        job_config.properties[cfg_p.ConfigKeys.RESULT_LOGS_ENABLED] = "true"
        job_config.properties[cfg_p.ConfigKeys.RESULT_STORAGE_LOCATION] = self.RESULT_STORAGE_KEY
        job_config.properties[cfg_p.ConfigKeys.RESULT_STORAGE_PATH] = self.RESULT_STORAGE_PATH
        job_config.properties[cfg_p.ConfigKeys.RESULT_FORMAT] = "JSON"

        trac_runtime = runtime.TracRuntime(self._sys_config(), scratch_dir=self.tmp_dir.joinpath("scratch"))
        trac_runtime.pre_start()

        job_error = None

        with trac_runtime as rt:
            rt.submit_job(job_config)
            try:
                rt.wait_for_job(job_id)
            except ex.ETrac as e:
                job_error = e

        return self._read_job_result(), job_error

    def _read_job_result(self) -> cfg.JobResult:

        result_path = self.tmp_dir.joinpath("data", self.RESULT_STORAGE_PATH)
        result_dict = json.loads(result_path.read_text())

        return cfg_p.ConfigParser(cfg.JobResult).parse(result_dict)

    def _read_job_log(self, job_result: cfg.JobResult) -> str:

        log_file_id = next(oid for oid in job_result.objectIds if oid.objectId == job_result.result.logFileId.objectId)
        log_file = job_result.objects[util.object_key(log_file_id)].file

        storage_id = next(oid for oid in job_result.objectIds if oid.objectId == log_file.storageId.objectId)
        storage_def = job_result.objects[util.object_key(storage_id)].storage

        storage_copy = storage_def.dataItems[log_file.dataItem].incarnations[0].copies[0]

        return self.tmp_dir.joinpath("data", storage_copy.storagePath).read_text()

    @staticmethod
    def fail_log_save() -> tp.ContextManager:

        write_byte_stream = storage.CommonFileStorage.write_byte_stream

        def failing_write(file_storage, storage_path: str):
            if "trac_job_log_file" in storage_path:
                raise ex.EStorageCommunication(f"Simulated storage failure for [{storage_path}]")
            return write_byte_stream(file_storage, storage_path)

        return mock.patch.object(storage.CommonFileStorage, "write_byte_stream", failing_write)

    def _capture_job(self, *captures: tp.Tuple[str, str]) -> meta.JobDefinition:

        sources = {
            name: meta.CaptureSource(
                location=meta.ExternalLocation("staging", path),
                schemaSource=meta.CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED,
                schema=_LOANS_SCHEMA)
            for name, path in captures}

        return meta.JobDefinition(jobType=meta.JobType.IMPORT_DATA, importData=meta.ImportDataJob(captures=sources))


class JobLogTest(JobLogTestBase):

    def test_capture_lines_in_job_log(self):

        self.tmp_dir.joinpath("staging", "loans.csv").write_text("id,amount,region\n1,10.5,north\n")

        job_result, job_error = self._run_job(self._capture_job(("loans", "loans.csv")))

        self.assertIsNone(job_error)
        self.assertEqual(meta.JobStatusCode.SUCCEEDED, job_result.result.statusCode)

        job_log = self._read_job_log(job_result)

        self.assertIn("Captured [loans] from [staging] at [loans.csv]", job_log)
        self.assertIn("Capture [loans] left out columns not in the declared schema", job_log)

    def test_capture_lines_in_failed_job_log(self):

        self.tmp_dir.joinpath("staging", "loans.csv").write_text("id,amount\n1,10.5\n")

        job_def = self._capture_job(("loans", "loans.csv"), ("missing", "missing.csv"))
        job_result, job_error = self._run_job(job_def)

        self.assertIsNotNone(job_error)
        self.assertEqual(meta.JobStatusCode.FAILED, job_result.result.statusCode)
        self.assertIn("Capture [missing] failed", job_result.result.statusMessage)

        job_log = self._read_job_log(job_result)

        self.assertIn("Captured [loans] from [staging] at [loans.csv]", job_log)
        self.assertIn("Job failed", job_log)

    def test_placement_lines_in_job_log(self):

        self.tmp_dir.joinpath("staging", "loans.csv").write_text("id,amount\n1,10.5\n")

        capture_result, _ = self._run_job(self._capture_job(("loans", "loans.csv")))

        data_id = next(oid for oid in capture_result.objectIds if oid.objectId == capture_result.result.outputs["loans"].objectId)
        objects = dict(capture_result.objects)
        mapping = {
            util.object_key(selector(oid)): oid
            for oid in capture_result.objectIds
            for selector in [util.selector_for, util.selector_for_latest]}

        placement = meta.PlacementTarget(
            dataId=util.selector_for(data_id),
            location=meta.ExternalLocation("outbound", "loans.csv"))

        job_def = meta.JobDefinition(
            jobType=meta.JobType.EXPORT_DATA,
            exportData=meta.ExportDataJob(placements={"loans": placement}))

        job_result, job_error = self._run_job(job_def, objects, mapping)

        self.assertIsNone(job_error)
        self.assertIn("Placed [loans]", self._read_job_log(job_result))

    def test_log_save_failure_keeps_success(self):

        self.tmp_dir.joinpath("staging", "loans.csv").write_text("id,amount\n1,10.5\n")

        with self.fail_log_save():
            job_result, job_error = self._run_job(self._capture_job(("loans", "loans.csv")))

        self.assertIsNone(job_error)
        self.assertEqual(meta.JobStatusCode.SUCCEEDED, job_result.result.statusCode)
        self.assertIsNone(job_result.result.logFileId)
        self.assertIn("loans", job_result.result.outputs)

    def test_log_save_failure_keeps_job_error(self):

        with self.fail_log_save():
            job_result, job_error = self._run_job(self._capture_job(("missing", "missing.csv")))

        self.assertIsNotNone(job_error)
        self.assertEqual(meta.JobStatusCode.FAILED, job_result.result.statusCode)
        self.assertIn("Capture [missing] failed", job_result.result.statusMessage)
        self.assertNotIn("error in the TRAC execution engine", job_result.result.statusMessage)
        self.assertIsNone(job_result.result.logFileId)

    def test_graph_build_failure_uses_preallocated_ids(self):

        file_id = util.new_object_id(meta.ObjectType.FILE)
        storage_id = util.new_object_id(meta.ObjectType.STORAGE)
        preallocated_ids = [dc.replace(file_id, objectVersion=0), dc.replace(storage_id, objectVersion=0)]

        job_def = meta.JobDefinition(
            jobType=meta.JobType.IMPORT_DATA,
            importData=meta.ImportDataJob(captures={"no_location": meta.CaptureSource(
                schemaSource=meta.CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED,
                schema=_LOANS_SCHEMA)}))

        job_result, job_error = self._run_job(job_def, preallocated_ids=preallocated_ids)

        self.assertIsNotNone(job_error)
        self.assertEqual(meta.JobStatusCode.FAILED, job_result.result.statusCode)
        self.assertIn("requires a storage location and path", job_result.result.statusMessage)

        self.assertEqual(file_id.objectId, job_result.result.logFileId.objectId)
        self.assertEqual(
            {(file_id.objectId, 1), (storage_id.objectId, 1)},
            {(oid.objectId, oid.objectVersion) for oid in job_result.objectIds})

        self.assertIn("Job failed", self._read_job_log(job_result))


if __name__ == "__main__":
    unittest.main()
