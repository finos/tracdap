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
import pathlib
import re
import tempfile
import unittest

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as meta
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.storage as storage  # noqa
import tracdap.rt._impl.core.util as util  # noqa
import tracdap.rt._impl.exec.dev_mode as dev_mode  # noqa
import tracdap.rt._impl.exec.graph as graph  # noqa
import tracdap.rt._impl.exec.graph_builder as graph_builder  # noqa


_TXT_FILE = meta.FileType("txt", "text/plain")

_SCHEMA = meta.SchemaDefinition(
    schemaType=meta.SchemaType.TABLE_SCHEMA,
    partType=meta.PartType.PART_ROOT,
    table=meta.TableSchema([
        meta.FieldSchema("value", 0, meta.BasicType.INTEGER)]))


def _sys_config(layout: meta.StorageLayout) -> cfg.RuntimeConfig:

    return cfg.RuntimeConfig(properties={
        cfg_p.ConfigKeys.STORAGE_DEFAULT_LOCATION: "test_storage",
        cfg_p.ConfigKeys.STORAGE_DEFAULT_LAYOUT: layout.name})


def _model_def(model_type: meta.ModelType, outputs) -> meta.ModelDefinition:

    return meta.ModelDefinition(
        language="python", repository="trac_integrated",
        entryPoint="tracdap_test.unit_test_model.UnitTestModel",
        modelType=model_type, outputs=outputs)


class GraphBuilderOutputsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    @staticmethod
    def _build_run_model_job(
            sys_config: cfg.RuntimeConfig,
            outputs: dict, prior_outputs: dict = None,
            prior_objects: dict = None, prior_mapping: dict = None,
            preallocated: list = None):

        model_id = util.new_object_id(meta.ObjectType.MODEL)
        model_def = _model_def(meta.ModelType.STANDARD_MODEL, outputs)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.RUN_MODEL,
            runModel=meta.RunModelJob(
                model=util.selector_for(model_id),
                priorOutputs=prior_outputs or {}))

        objects = {util.object_key(model_id): meta.ObjectDefinition(meta.ObjectType.MODEL, model=model_def)}
        objects.update(prior_objects or {})

        job_config = cfg.JobConfig(
            jobId=util.new_object_id(meta.ObjectType.JOB), job=job_def,
            objects=objects, objectMapping=prior_mapping or {},
            preallocatedIds=preallocated or [])

        builder = graph_builder.GraphBuilder(sys_config, job_config)

        return builder.build_job(job_def)

    @staticmethod
    def _save_spec(job_graph: graph.Graph, output_name: str):

        save_nodes = [
            node for node_id, node in job_graph.nodes.items()
            if node_id.name == f"{output_name}:SAVE" and isinstance(node, graph.SaveDataNode)]

        return save_nodes[0].spec if len(save_nodes) == 1 else None

    @staticmethod
    def _prior_objects(spec):

        file_selector = util.selector_for(spec.primary_id)

        objects = {
            util.object_key(spec.primary_id): meta.ObjectDefinition(meta.ObjectType.FILE, file=spec.definition),
            util.object_key(spec.storage_id): meta.ObjectDefinition(meta.ObjectType.STORAGE, storage=spec.storage)}

        mapping = {
            util.object_key(file_selector): spec.primary_id,
            util.object_key(spec.definition.storageId): spec.storage_id}

        return file_selector, objects, mapping

    def _storage_path(self, spec):

        storage_copies = spec.storage.dataItems[spec.data_item].incarnations[0].copies
        self.assertEqual(1, len(storage_copies))

        return storage_copies[0].storagePath

    def test_file_output_new(self):

        outputs = {"report": meta.ModelOutputSchema(objectType=meta.ObjectType.FILE, fileType=_TXT_FILE)}

        preallocated_file = dc.replace(util.new_object_id(meta.ObjectType.FILE), objectVersion=0)
        preallocated_storage = dc.replace(util.new_object_id(meta.ObjectType.STORAGE), objectVersion=0)

        layouts = {
            meta.StorageLayout.OBJECT_ID_LAYOUT: rf"^file/{preallocated_file.objectId}/version-1-x[0-9a-f]{{6}}/report\.txt\.txt$",
            meta.StorageLayout.DATE_SNAP_LAYOUT: rf"^\d{{4}}/\d{{4}}-\d{{2}}-\d{{2}}/{preallocated_file.objectId}/version-1-x[0-9a-f]{{6}}/report\.txt$",
            meta.StorageLayout.DEVELOPER_LAYOUT: r"^Dev Outputs/report\.txt$"}

        for layout, path_pattern in layouts.items():
            with self.subTest(layout=layout.name):

                job_graph = self._build_run_model_job(
                    _sys_config(layout), outputs,
                    preallocated=[preallocated_file, preallocated_storage])

                spec = self._save_spec(job_graph, "report")

                self.assertEqual(preallocated_file.objectId, spec.primary_id.objectId)
                self.assertEqual(1, spec.primary_id.objectVersion)
                self.assertEqual(preallocated_storage.objectId, spec.storage_id.objectId)
                self.assertEqual(1, spec.storage_id.objectVersion)

                self.assertEqual("report.txt", spec.definition.name)
                self.assertEqual("txt", spec.definition.extension)
                self.assertEqual("text/plain", spec.definition.mimeType)

                self.assertRegex(self._storage_path(spec), path_pattern)

    def test_file_output_prior_versions(self):

        outputs = {"report": meta.ModelOutputSchema(objectType=meta.ObjectType.FILE, fileType=_TXT_FILE)}

        layouts = {
            meta.StorageLayout.OBJECT_ID_LAYOUT: lambda fid, v: rf"^file/{fid}/version-{v}-x[0-9a-f]{{6}}/report\.txt\.txt$",
            meta.StorageLayout.DEVELOPER_LAYOUT: lambda fid, v: rf"^outputs/report-{v}\.txt$"}

        for layout, path_pattern in layouts.items():
            with self.subTest(layout=layout.name):

                sys_config = _sys_config(layout)

                file_id = util.new_object_id(meta.ObjectType.FILE)
                storage_id = util.new_object_id(meta.ObjectType.STORAGE)
                prior_spec = storage.build_file_spec(file_id, storage_id, "report", _TXT_FILE, sys_config)

                if layout == meta.StorageLayout.DEVELOPER_LAYOUT:
                    prior_copy = prior_spec.storage.dataItems[prior_spec.data_item].incarnations[0].copies[0]
                    prior_copy.storagePath = "outputs/report.txt"

                for version in [2, 3]:

                    prior_selector, prior_objects, prior_mapping = self._prior_objects(prior_spec)

                    job_graph = self._build_run_model_job(
                        sys_config, outputs,
                        prior_outputs={"report": prior_selector},
                        prior_objects=prior_objects, prior_mapping=prior_mapping)

                    spec = self._save_spec(job_graph, "report")

                    self.assertEqual(file_id.objectId, spec.primary_id.objectId)
                    self.assertEqual(version, spec.primary_id.objectVersion)
                    self.assertEqual(storage_id.objectId, spec.storage_id.objectId)
                    self.assertEqual(version, spec.storage_id.objectVersion)

                    self.assertEqual("report.txt", spec.definition.name)
                    self.assertEqual("txt", spec.definition.extension)

                    self.assertRegex(self._storage_path(spec), path_pattern(file_id.objectId, version))

                    prior_spec = spec

    def test_job_log_file_spec(self):

        path_patterns = {
            meta.StorageLayout.OBJECT_ID_LAYOUT: r"/trac_job_log_file\.TXT\.txt$",
            meta.StorageLayout.DATE_SNAP_LAYOUT: r"/trac_job_log_file\.TXT$",
            meta.StorageLayout.DEVELOPER_LAYOUT: r"^Dev Outputs/trac_job_log_file\.txt$"}

        for layout in path_patterns:
            with self.subTest(layout=layout.name):

                file_id = util.new_object_id(meta.ObjectType.FILE)
                storage_id = util.new_object_id(meta.ObjectType.STORAGE)

                spec = storage.build_file_spec(
                    file_id, storage_id, "trac_job_log_file",
                    meta.FileType("TXT", "text/plain"), _sys_config(layout))

                self.assertEqual("trac_job_log_file.TXT", spec.definition.name)
                self.assertEqual("TXT", spec.definition.extension)
                self.assertRegex(self._storage_path(spec), path_patterns[layout])

    def test_data_spec_inline_schema(self):

        data_id = util.new_object_id(meta.ObjectType.DATA)
        storage_id = util.new_object_id(meta.ObjectType.STORAGE)

        spec = storage.build_data_spec(
            data_id, storage_id, "sample_data", _SCHEMA,
            _sys_config(meta.StorageLayout.OBJECT_ID_LAYOUT))

        self.assertEqual(_SCHEMA, spec.definition.schema)
        self.assertIsNone(spec.definition.schemaId)
        self.assertIsNone(spec.schema)
        self.assertEqual(meta.SchemaType.TABLE_SCHEMA, spec.schema_type)

    def test_data_spec_schema_id(self):

        data_id = util.new_object_id(meta.ObjectType.DATA)
        storage_id = util.new_object_id(meta.ObjectType.STORAGE)
        schema_selector = util.selector_for(util.new_object_id(meta.ObjectType.SCHEMA))

        spec = storage.build_data_spec(
            data_id, storage_id, "sample_data", _SCHEMA,
            _sys_config(meta.StorageLayout.OBJECT_ID_LAYOUT),
            schema_id=schema_selector)

        self.assertIsNone(spec.definition.schema)
        self.assertEqual(schema_selector, spec.definition.schemaId)
        self.assertEqual(_SCHEMA, spec.schema)
        self.assertEqual(meta.SchemaType.TABLE_SCHEMA, spec.schema_type)

    def test_data_output_static_schema(self):

        outputs = {"sample_data": meta.ModelOutputSchema(objectType=meta.ObjectType.DATA, schema=_SCHEMA)}

        job_graph = self._build_run_model_job(_sys_config(meta.StorageLayout.OBJECT_ID_LAYOUT), outputs)
        spec = self._save_spec(job_graph, "sample_data")

        self.assertEqual(_SCHEMA, spec.definition.schema)
        self.assertIsNone(spec.definition.schemaId)
        self.assertEqual(1, spec.primary_id.objectVersion)


class GraphBuilderImportExportTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    @staticmethod
    def _build_job(job_type: meta.JobType, model_type: meta.ModelType = None):

        outputs = {"sample_data": meta.ModelOutputSchema(objectType=meta.ObjectType.DATA, schema=_SCHEMA)}
        objects = {}

        if model_type is not None:
            model_id = util.new_object_id(meta.ObjectType.MODEL)
            model_selector = util.selector_for(model_id)
            model_def = _model_def(model_type, outputs if job_type == meta.JobType.IMPORT_DATA else {})
            objects[util.object_key(model_id)] = meta.ObjectDefinition(meta.ObjectType.MODEL, model=model_def)
        else:
            model_selector = meta.TagSelector()

        if job_type == meta.JobType.IMPORT_DATA:
            job_def = meta.JobDefinition(jobType=job_type, importData=meta.ImportDataJob(model=model_selector))
        else:
            job_def = meta.JobDefinition(jobType=job_type, exportData=meta.ExportDataJob(model=model_selector))

        job_config = cfg.JobConfig(
            jobId=util.new_object_id(meta.ObjectType.JOB), job=job_def, objects=objects)

        builder = graph_builder.GraphBuilder(_sys_config(meta.StorageLayout.OBJECT_ID_LAYOUT), job_config)

        return builder.build_job(job_def)

    @staticmethod
    def _node_names(job_graph: graph.Graph, node_class):
        return set(node_id.name for node_id, node in job_graph.nodes.items() if isinstance(node, node_class))

    def test_import_model_job(self):

        job_graph = self._build_job(meta.JobType.IMPORT_DATA, meta.ModelType.DATA_IMPORT_MODEL)

        self.assertEqual({"sample_data:SAVE"}, self._node_names(job_graph, graph.SaveDataNode))
        self.assertEqual(1, len(self._node_names(job_graph, graph.RunModelNode)))

    def test_import_job_standard_model(self):

        with self.assertRaisesRegex(ex.EJobValidation, re.escape("Job type [IMPORT_DATA] cannot use model type [STANDARD_MODEL]")):
            self._build_job(meta.JobType.IMPORT_DATA, meta.ModelType.STANDARD_MODEL)

    def test_export_job_model_rejected(self):

        for model_type in [meta.ModelType.DATA_EXPORT_MODEL, meta.ModelType.DATA_IMPORT_MODEL]:
            with self.subTest(model_type=model_type.name):
                with self.assertRaisesRegex(ex.EJobValidation, re.escape("Job type [EXPORT_DATA] takes no model")):
                    self._build_job(meta.JobType.EXPORT_DATA, model_type)

    def test_import_job_no_model(self):

        with self.assertRaisesRegex(ex.ETracInternal, re.escape("Unexpected error preparing the job execution graph")):
            self._build_job(meta.JobType.IMPORT_DATA)


class DevModeImportJobTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    def test_import_job_no_model(self):

        job_def = meta.JobDefinition(jobType=meta.JobType.IMPORT_DATA, importData=meta.ImportDataJob())
        job_config = cfg.JobConfig(job=job_def)

        with tempfile.TemporaryDirectory() as tmpdir:

            translator = dev_mode.DevModeTranslator(_sys_config(meta.StorageLayout.DEVELOPER_LAYOUT), None, pathlib.Path(tmpdir))

            with self.assertRaisesRegex(ex.EJobValidation, re.escape("Missing required property [model] for job type [IMPORT_DATA]")):
                translator.translate_job_config(job_config)
