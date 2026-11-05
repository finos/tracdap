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

import csv
import json
import math
import pathlib
import shutil
import tempfile
import typing as tp
import unittest

import pyarrow.parquet as pq
import yaml

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as meta
import tracdap.rt._impl.runtime as runtime  # noqa
import tracdap.rt._impl.static_api as api_hook  # noqa
import tracdap.rt._impl.core.config_parser as cfg_p  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.models as models  # noqa
import tracdap.rt._impl.core.plugins as plugins  # noqa
import tracdap.rt._impl.core.repos as repos  # noqa
import tracdap.rt._impl.core.type_system as types  # noqa
import tracdap.rt._impl.core.util as util  # noqa

import tracdap_test.flow_scenarios.models as scenario_models


CATALOGUE_DIR = pathlib.Path(scenario_models.__file__).parent
SCENARIOS_DIR = CATALOGUE_DIR.joinpath("scenarios")
DATA_DIR = CATALOGUE_DIR.joinpath("data")
TRAC_REPO_DIR = CATALOGUE_DIR.parents[4]
MODELS_PATH = CATALOGUE_DIR.parents[1].relative_to(TRAC_REPO_DIR)


def _read_yaml(path: pathlib.Path) -> dict:
    return yaml.safe_load(path.read_text()) or {}


def _model_interfaces() -> tp.Dict[str, meta.ModelDefinition]:
    parser = cfg_p.ConfigParser(meta.ModelDefinition)
    return {
        entry_point: parser.parse(interface)
        for entry_point, interface in _read_yaml(CATALOGUE_DIR.joinpath("models.yaml")).items()}


def _scenario_names() -> tp.List[str]:
    return sorted(path.name for path in SCENARIOS_DIR.iterdir() if path.joinpath("expected.yaml").exists())


_CSV_TYPES = {
    meta.BasicType.STRING: str,
    meta.BasicType.FLOAT: float,
    meta.BasicType.INTEGER: int}


def _read_table(path: pathlib.Path, field_types: tp.Dict[str, meta.BasicType]) -> dict:

    """A CSV table as its columns and its rows, typed by the schema and sorted, so row order is not compared"""

    with path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        columns = list(reader.fieldnames or [])
        rows = [
            tuple(_CSV_TYPES[field_types[col]](row[col]) if row[col] != "" else None for col in columns)
            for row in reader]

    return {"columns": columns, "rows": sorted(rows, key=repr)}


def _assert_values_match(test: unittest.TestCase, expected: tp.Any, actual: tp.Any, msg: str):

    if expected is None or actual is None:
        test.assertEqual(expected, actual, msg)
    elif isinstance(expected, float) or isinstance(actual, float):
        test.assertTrue(
            isinstance(actual, (int, float)) and math.isclose(expected, actual, rel_tol=1e-9, abs_tol=1e-9),
            f"{msg}: expected {expected}, got {actual}")
    elif isinstance(expected, dict) and isinstance(actual, dict):
        test.assertEqual(set(expected), set(actual), msg)
        for key in expected:
            _assert_values_match(test, expected[key], actual[key], f"{msg} [{key}]")
    elif isinstance(expected, (list, tuple)) and isinstance(actual, (list, tuple)):
        test.assertEqual(len(expected), len(actual), f"{msg}: expected {expected}, got {actual}")
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual)):
            _assert_values_match(test, expected_item, actual_item, f"{msg} [{index}]")
    else:
        test.assertEqual(expected, actual, msg)


class FlowScenarioDataTest(unittest.TestCase):

    """Each parquet file in the catalogue holds the same table as the CSV file and schema the platform runner uploads"""

    def test_parquet_matches_csv(self):

        for parquet_file in DATA_DIR.glob("*.parquet"):
            with self.subTest(parquet_file.name):

                schema_def = cfg_p.ConfigParser(meta.SchemaDefinition).parse(
                    _read_yaml(DATA_DIR.joinpath(f"{parquet_file.stem}.schema.yaml")))

                field_types = {f.fieldName: f.fieldType for f in schema_def.table.fields}
                csv_table = _read_table(DATA_DIR.joinpath(f"{parquet_file.stem}.csv"), field_types)

                parquet_table = pq.read_table(parquet_file)
                parquet_rows = sorted((tuple(row.values()) for row in parquet_table.to_pylist()), key=repr)

                self.assertEqual([f.fieldName for f in schema_def.table.fields], parquet_table.column_names)
                _assert_values_match(self, csv_table, {"columns": parquet_table.column_names, "rows": parquet_rows}, parquet_file.name)


class FlowScenarioModelsTest(unittest.TestCase):

    """The model interfaces in models.yaml match the scenario models, loaded as the platform loads them"""

    @classmethod
    def setUpClass(cls) -> None:
        plugins.PluginManagerImpl.register_core_plugins()
        api_hook.StaticApiImpl.register_impl()
        log.configure_logging()

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.scratch_dir = pathlib.Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_models_match_interfaces(self):

        sys_config = cfg.RuntimeConfig()
        sys_config.resources["scenario_repo"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.MODEL_REPOSITORY, protocol="local",
            properties={"repoUrl": str(TRAC_REPO_DIR)})

        loader = models.ModelLoader(repos.RepositoryManager(sys_config), self.scratch_dir)
        loader.create_scope("flow_scenarios")

        try:
            for entry_point, interface in _model_interfaces().items():
                with self.subTest(entry_point):

                    model_stub = meta.ModelDefinition(
                        language="python", repository="scenario_repo",
                        path=str(MODELS_PATH), entryPoint=entry_point)

                    model_class = loader.load_model_class("flow_scenarios", model_stub)
                    model_def = loader.scan_model(model_stub, model_class)

                    self.assertEqual(interface.modelType, model_def.modelType)
                    self.assertEqual(interface.parameters, model_def.parameters)
                    self.assertEqual(interface.inputs, model_def.inputs)
                    self.assertEqual(interface.outputs, model_def.outputs)
                    self.assertEqual(interface.resources, model_def.resources)
        finally:
            loader.destroy_scope("flow_scenarios")


class FlowScenarioDevModeTest(unittest.TestCase):

    """Run every scenario in the catalogue through dev mode and check it against expected.yaml"""

    RESULT_STORAGE_KEY = "scenario_data"
    RESULT_STORAGE_PATH = "results/trac_job_result.json"

    @classmethod
    def setUpClass(cls) -> None:
        plugins.PluginManagerImpl.register_core_plugins()
        log.configure_logging()
        cls.interfaces = _model_interfaces()

    def test_scenarios(self):

        for scenario_name in _scenario_names():
            with self.subTest(scenario_name):
                self._run_scenario(scenario_name)

    def _run_scenario(self, scenario_name: str):

        scenario_dir = SCENARIOS_DIR.joinpath(scenario_name)
        expected = _read_yaml(scenario_dir.joinpath("expected.yaml"))

        if "devMode" in expected:
            self.assertTrue(
                expected["devMode"].get("reason") or expected["devMode"].get("skip"),
                f"[{scenario_name}]: the devMode override needs a reason or a skip")

        saved_outputs = {*(expected.get("outputs") or {}), *(expected.get("devMode", {}).get("outputs") or {})}
        expected_files = {p.stem for p in scenario_dir.joinpath("expected").glob("*")}
        self.assertEqual(
            sorted(saved_outputs), sorted(expected_files),
            f"[{scenario_name}]: every output saved by either runner needs one file in expected/")

        expected = {**expected, **expected.get("devMode", {})}

        if expected["outcome"] != "SUCCEEDED":
            self.assertTrue(expected.get("error"), f"[{scenario_name}]: expects {expected['outcome']}, so it needs an error")

        if "skip" in expected:
            self.skipTest(f"[{scenario_name}] does not run in dev mode: {expected['skip']}")

        with tempfile.TemporaryDirectory() as tmp:

            tmp_dir = pathlib.Path(tmp)
            job_spec = _read_yaml(scenario_dir.joinpath("job.yaml"))
            runs = 2 if job_spec.get("rerun") else 1

            for run in range(1, runs + 1):

                job_config, job_result, job_error = self._run_job(tmp_dir, scenario_dir, job_spec)

                if expected["outcome"] == "REJECTED":
                    self.assertIsNone(job_result, "Expected the job to be rejected")
                    self.assertIsInstance(job_error, ex.ETrac)
                    self.assertTrue(
                        str(job_error).startswith(expected["error"]),
                        f"Expected an error starting [{expected['error']}], got [{job_error}]")
                    return

                self._check_strict_flow(job_config, expected)
                self._check_result(job_result, job_error, expected, expected_version=run)
                self._check_output_values(tmp_dir, scenario_dir, job_spec, job_result, expected)
                self._check_model_log_lines(tmp_dir, job_spec, job_result, expected)

    def _run_job(self, tmp_dir: pathlib.Path, scenario_dir: pathlib.Path, job_spec: dict) \
            -> tp.Tuple[tp.Optional[cfg.JobConfig], tp.Optional[cfg.JobResult], tp.Optional[Exception]]:

        storage_dir = tmp_dir.joinpath("data")
        inputs_dir = storage_dir.joinpath("inputs")
        inputs_dir.mkdir(parents=True, exist_ok=True)

        for data_file in job_spec.get("inputs", {}).values():
            shutil.copy(DATA_DIR.joinpath(data_file), inputs_dir.joinpath(data_file))

        flow_def = _read_yaml(scenario_dir.joinpath("flow.yaml"))

        sys_config = {
            "properties": {
                "storage.default.location": self.RESULT_STORAGE_KEY,
                "storage.default.format": "CSV"},
            "resources": {
                self.RESULT_STORAGE_KEY: {
                    "resourceType": "INTERNAL_STORAGE", "protocol": "LOCAL",
                    "properties": {"rootPath": str(storage_dir)}}}}

        job_config = {"job": {"runFlow": {
            "flow": str(scenario_dir.joinpath("flow.yaml")),
            "parameters": job_spec.get("parameters", {}),
            "inputs": {name: f"inputs/{data_file}" for name, data_file in job_spec.get("inputs", {}).items()},
            "outputs": {name: f"outputs/{name}.{self._output_extension(name, flow_def, job_spec)}"
                        for name, node in flow_def["nodes"].items() if node["nodeType"] == "OUTPUT_NODE"},
            "models": job_spec["models"]}}}

        sys_config_path = tmp_dir.joinpath("sys_config.yaml")
        job_config_path = tmp_dir.joinpath("job_config.yaml")
        sys_config_path.write_text(yaml.safe_dump(sys_config))
        job_config_path.write_text(yaml.safe_dump(job_config))

        trac_runtime = runtime.TracRuntime(sys_config_path, scratch_dir=tmp_dir.joinpath("scratch"), dev_mode=True)
        tmp_dir.joinpath("scratch").mkdir(exist_ok=True)
        trac_runtime.pre_start()

        with trac_runtime as rt:

            try:
                translated_config = rt.load_job_config(job_config_path)
            except ex.ETrac as e:
                return None, None, e

            translated_config.properties[cfg_p.ConfigKeys.RESULT_ENABLED] = "true"
            translated_config.properties[cfg_p.ConfigKeys.RESULT_LOGS_ENABLED] = "true"
            translated_config.properties[cfg_p.ConfigKeys.RESULT_STORAGE_LOCATION] = self.RESULT_STORAGE_KEY
            translated_config.properties[cfg_p.ConfigKeys.RESULT_STORAGE_PATH] = self.RESULT_STORAGE_PATH
            translated_config.properties[cfg_p.ConfigKeys.RESULT_FORMAT] = "JSON"

            job_error = None
            rt.submit_job(translated_config)

            try:
                rt.wait_for_job(translated_config.jobId)
            except ex.ETrac as e:
                job_error = e

        result_path = storage_dir.joinpath(self.RESULT_STORAGE_PATH)
        job_result = cfg_p.ConfigParser(cfg.JobResult).parse(json.loads(result_path.read_text()))

        return translated_config, job_result, job_error

    def _output_socket(self, output_name: str, flow_def: dict, job_spec: dict) -> meta.ModelOutputSchema:

        edge = next(edge for edge in flow_def["edges"] if edge["target"]["node"] == output_name)
        source = edge["source"]

        return self.interfaces[job_spec["models"][source["node"]]].outputs[source["socket"]]

    def _output_extension(self, output_name: str, flow_def: dict, job_spec: dict) -> str:

        output_schema = self._output_socket(output_name, flow_def, job_spec)

        if output_schema.objectType == meta.ObjectType.FILE:
            return output_schema.fileType.extension
        if output_schema.schema is not None and output_schema.schema.schemaType == meta.SchemaType.STRUCT_SCHEMA:
            return "yaml"

        return "csv"

    def _check_strict_flow(self, job_config: cfg.JobConfig, expected: dict):

        flow_def = job_config.objects[util.object_key(job_config.job.runFlow.flow)].flow

        expected_params = expected["flow"].get("parameters", {})
        self.assertEqual(set(expected_params.keys()), set(flow_def.parameters.keys()), "Flow parameters")

        for name, expected_param in expected_params.items():
            param = flow_def.parameters[name]
            self.assertEqual(
                meta.BasicType[expected_param["paramType"]], param.paramType.basicType,
                f"Type of flow parameter [{name}]")
            self.assertEqual(
                expected_param.get("default"),
                types.MetadataCodec.decode_value(param.defaultValue) if param.defaultValue is not None else None,
                f"Default of flow parameter [{name}]")
            if "label" in expected_param:
                self.assertEqual(expected_param["label"], param.label, f"Label of flow parameter [{name}]")

        for section, declared in [("inputs", flow_def.inputs), ("outputs", flow_def.outputs)]:

            expected_section = expected["flow"].get(section, {})
            self.assertEqual(set(expected_section.keys()), set(declared.keys()), f"Flow {section}")

            for name, expected_entry in expected_section.items():
                self.assertEqual(
                    meta.ObjectType[expected_entry["objectType"]], declared[name].objectType,
                    f"Object type of flow {section} [{name}]")
                self.assertEqual(
                    expected_entry.get("optional", False), declared[name].optional,
                    f"Optional flag of flow {section} [{name}]")
                if "label" in expected_entry:
                    self.assertEqual(expected_entry["label"], declared[name].label, f"Label of flow {section} [{name}]")
                if "fields" in expected_entry:
                    self.assertEqual(expected_entry["fields"], self._field_names(declared[name].schema), f"Fields of flow {section} [{name}]")
                if "fileType" in expected_entry:
                    self.assertEqual(expected_entry["fileType"], declared[name].fileType.extension, f"File type of flow {section} [{name}]")

    @staticmethod
    def _field_names(schema: meta.SchemaDefinition) -> tp.List[str]:
        fields = schema.struct.fields if schema.schemaType == meta.SchemaType.STRUCT_SCHEMA else schema.table.fields
        return [field.fieldName for field in fields]

    def _check_result(
            self, job_result: cfg.JobResult, job_error: tp.Optional[Exception],
            expected: dict, expected_version: int):

        result = job_result.result

        if expected["outcome"] == "SUCCEEDED":
            self.assertIsNone(job_error, f"Job failed: {job_error}")
            self.assertEqual(meta.JobStatusCode.SUCCEEDED, result.statusCode)
        else:
            self.assertIsNotNone(job_error, "Expected the job to fail")
            self.assertEqual(meta.JobStatusCode.FAILED, result.statusCode)
            self.assertTrue(
                result.statusMessage.startswith(expected["error"]),
                f"Expected an error starting [{expected['error']}], got [{result.statusMessage}]")

        expected_outputs = expected.get("outputs") or {}
        self.assertEqual(set(expected_outputs.keys()), set(result.outputs.keys()), "Saved outputs")

        for output_name, object_type in expected_outputs.items():
            output_selector = result.outputs[output_name]
            self.assertEqual(meta.ObjectType[object_type], output_selector.objectType, f"Output [{output_name}]")
            output_id = next(oid for oid in job_result.objectIds if oid.objectId == output_selector.objectId)
            self.assertEqual(expected_version, output_id.objectVersion, f"Version of output [{output_name}]")

    def _check_output_values(
            self, tmp_dir: pathlib.Path, scenario_dir: pathlib.Path,
            job_spec: dict, job_result: cfg.JobResult, expected: dict):

        flow_def = _read_yaml(scenario_dir.joinpath("flow.yaml"))

        for output_name in expected.get("outputs") or {}:

            socket = self._output_socket(output_name, flow_def, job_spec)
            expected_file = next(scenario_dir.joinpath("expected").glob(f"{output_name}.*"))
            saved_file = self._saved_output_path(tmp_dir, job_result, output_name)

            if socket.objectType == meta.ObjectType.FILE:
                self.assertEqual(expected_file.read_bytes(), saved_file.read_bytes(), f"Content of output [{output_name}]")
            elif socket.schema.schemaType == meta.SchemaType.STRUCT_SCHEMA:
                _assert_values_match(self, _read_yaml(expected_file), _read_yaml(saved_file), f"Content of output [{output_name}]")
            else:
                field_types = {f.fieldName: f.fieldType for f in socket.schema.table.fields}
                _assert_values_match(
                    self, _read_table(expected_file, field_types), _read_table(saved_file, field_types),
                    f"Content of output [{output_name}]")

    @staticmethod
    def _saved_output_path(tmp_dir: pathlib.Path, job_result: cfg.JobResult, output_name: str) -> pathlib.Path:

        def job_object(selector):
            object_id = next(oid for oid in job_result.objectIds if oid.objectId == selector.objectId)
            return job_result.objects[util.object_key(object_id)]

        output_obj = job_object(job_result.result.outputs[output_name])

        if output_obj.file is not None:
            storage_id, data_item = output_obj.file.storageId, output_obj.file.dataItem
        else:
            part = next(iter(output_obj.data.parts.values()))
            storage_id, data_item = output_obj.data.storageId, part.snap.deltas[0].dataItem

        storage_copy = job_object(storage_id).storage.dataItems[data_item].incarnations[0].copies[0]

        return tmp_dir.joinpath("data", storage_copy.storagePath)

    def _check_model_log_lines(self, tmp_dir: pathlib.Path, job_spec: dict, job_result: cfg.JobResult, expected: dict):

        log_file_id = next(oid for oid in job_result.objectIds if oid.objectId == job_result.result.logFileId.objectId)
        log_file = job_result.objects[util.object_key(log_file_id)].file
        storage_id = next(oid for oid in job_result.objectIds if oid.objectId == log_file.storageId.objectId)
        storage_def = job_result.objects[util.object_key(storage_id)].storage
        storage_copy = storage_def.dataItems[log_file.dataItem].incarnations[0].copies[0]

        job_log = tmp_dir.joinpath("data", storage_copy.storagePath).read_text()

        ran = expected.get("ran", list(job_spec["models"].keys()))
        classes_run = {self._model_class(ep) for node, ep in job_spec["models"].items() if node in ran}
        classes_not_run = {self._model_class(ep) for ep in job_spec["models"].values()} - classes_run

        for model_class_name in classes_run:
            self.assertIn(scenario_models.MODEL_LOG_LINE.format(model_class_name), job_log)

        for model_class_name in classes_not_run:
            self.assertNotIn(scenario_models.MODEL_LOG_LINE.format(model_class_name), job_log)

    @staticmethod
    def _model_class(entry_point: str) -> str:
        return entry_point.rsplit(".", 1)[-1]
