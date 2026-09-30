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

import pathlib
import tempfile
import typing as tp
import unittest
import unittest.mock as mock

import pandas as pd
import yaml

import tracdap.rt.api.experimental as trac
import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.launch as launch
import tracdap.rt.metadata as meta
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.util as util  # noqa
import tracdap.rt._impl.exec.graph as graph  # noqa
import tracdap.rt._impl.exec.graph_builder as graph_builder  # noqa


class SampleDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("row_count", trac.INTEGER, "Number of rows to generate"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"sample_data": trac.define_output_table(trac.F("value", trac.INTEGER, "Value"))}

    def run_model(self, ctx: trac.TracContext):
        row_count = ctx.get_parameter("row_count")
        ctx.put_pandas_table("sample_data", pd.DataFrame({"value": range(row_count)}))


class OptionalInputModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return {}

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {
            "sample_data": trac.define_input_table(trac.F("value", trac.INTEGER, "Value")),
            "extra_data": trac.define_input_table(trac.F("value", trac.INTEGER, "Value"), optional=True)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {"combined_data": trac.define_output_table(trac.F("value", trac.INTEGER, "Value"))}

    def run_model(self, ctx: trac.TracContext):
        ctx.put_pandas_table("combined_data", ctx.get_pandas_table("sample_data"))


class SlotExportModel(trac.TracDataExport):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("storage_key", trac.STRING, "External storage key"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {
            f"slot_{i}": trac.define_input_table(optional=True, dynamic=True)
            for i in range(1, 4)}

    def run_model(self, ctx: trac.TracDataContext):

        storage = ctx.get_file_storage(ctx.get_parameter("storage_key"))
        slots = [slot for slot in sorted(self.define_inputs().keys()) if ctx.has_dataset(slot)]

        with storage.write_byte_stream("slots.txt") as stream:
            stream.write(",".join(slots).encode("utf-8"))


class RequiredSlotExportModel(trac.TracDataExport):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(trac.P("storage_key", trac.STRING, "External storage key"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {
            "slot_1": trac.define_input_table(dynamic=True),
            "slot_2": trac.define_input_table(optional=True, dynamic=True)}

    def run_model(self, ctx: trac.TracDataContext):
        pass


def _entry_point(model_class: type) -> str:
    return f"{model_class.__module__}.{model_class.__name__}"


class FlowExportJobsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    def setUp(self) -> None:

        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = pathlib.Path(self._tmp.name)

        for storage_dir in ["data", "export_a", "export_b"]:
            self.tmp_dir.joinpath(storage_dir).mkdir()

        sys_config = {
            "properties": {
                "storage.default.location": "test_data",
                "storage.default.format": "CSV"},
            "resources": {
                "test_data": self._storage_resource("INTERNAL_STORAGE", "data"),
                "export_a": self._storage_resource("EXTERNAL_STORAGE", "export_a"),
                "export_b": self._storage_resource("EXTERNAL_STORAGE", "export_b")}}

        self.sys_config_path = self._write_yaml("sys_config.yaml", sys_config)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _storage_resource(self, resource_type: str, storage_dir: str):
        return {
            "resourceType": resource_type,
            "protocol": "LOCAL",
            "properties": {"rootPath": str(self.tmp_dir.joinpath(storage_dir))}}

    def _write_yaml(self, file_name: str, content: dict) -> pathlib.Path:
        file_path = self.tmp_dir.joinpath(file_name)
        file_path.write_text(yaml.safe_dump(content))
        return file_path

    def _run_flow(self, nodes: dict, edges: list, models: dict, parameters: dict, export_storage: tp.List[str]):

        flow_path = self._write_yaml("flow.yaml", {"nodes": nodes, "edges": edges})

        job_config = {"job": {"runFlow": {
            "flow": str(flow_path),
            "parameters": parameters,
            "models": {name: _entry_point(model) for name, model in models.items()},
            "exportStorageAccess": export_storage}}}

        job_config_path = self._write_yaml("job_config.yaml", job_config)

        launch.launch_job(job_config_path, self.sys_config_path, dev_mode=True)

    @staticmethod
    def _edge(source_node, source_socket, target_node, target_socket):
        return {
            "source": {"node": source_node, "socket": source_socket},
            "target": {"node": target_node, "socket": target_socket}}

    @staticmethod
    def _sample_node():
        return {"nodeType": "MODEL_NODE", "outputs": ["sample_data"]}

    @staticmethod
    def _export_node(inputs: tp.List[str], model_type: tp.Optional[str] = "DATA_EXPORT_MODEL"):
        node = {"nodeType": "MODEL_NODE", "inputs": inputs}
        if model_type is not None:
            node["modelType"] = model_type
        return node

    def _run_export_flow(
            self, export_model=SlotExportModel, export_model_type="DATA_EXPORT_MODEL",
            storage_key="export_a", export_storage=None, connected_slots=("slot_2",)):

        nodes = {
            "sample": self._sample_node(),
            "export": self._export_node(["slot_1", "slot_2", "slot_3"], export_model_type)}

        edges = [self._edge("sample", "sample_data", "export", slot) for slot in connected_slots]
        models = {"sample": SampleDataModel, "export": export_model}
        parameters = {"row_count": 5, "storage_key": storage_key}

        self._run_flow(nodes, edges, models, parameters, export_storage or ["export_a"])

    def test_export_node_terminal_with_unconnected_optional_inputs(self):

        self._run_export_flow()

        exported_slots = self.tmp_dir.joinpath("export_a/slots.txt").read_text()
        self.assertEqual("slot_2", exported_slots)

    def test_export_node_unconnected_required_input_dev_mode(self):

        nodes = {
            "sample": self._sample_node(),
            "export": self._export_node(["slot_1", "slot_2"])}

        edges = [self._edge("sample", "sample_data", "export", "slot_2")]
        models = {"sample": SampleDataModel, "export": RequiredSlotExportModel}
        parameters = {"row_count": 5, "storage_key": "export_a"}

        with self.assertRaisesRegex(ex.EConfigParse, "slot_1 is not provided by any node"):
            self._run_flow(nodes, edges, models, parameters, ["export_a"])

    def test_export_model_on_standard_node(self):

        with self.assertRaisesRegex(ex.EJobValidation, "requires model type \\[STANDARD_MODEL\\]"):
            self._run_export_flow(export_model_type=None, connected_slots=("slot_1", "slot_2", "slot_3"))

    def test_standard_model_on_export_node(self):

        nodes = {
            "sample": self._sample_node(),
            "consumer": {"nodeType": "MODEL_NODE", "modelType": "DATA_EXPORT_MODEL",
                         "inputs": ["sample_data", "extra_data"], "outputs": ["combined_data"]}}

        models = {"sample": SampleDataModel, "consumer": OptionalInputModel}

        with self.assertRaisesRegex(ex.EJobValidation, "requires model type \\[DATA_EXPORT_MODEL\\]"):
            self._run_flow(nodes, [], models, {"row_count": 5}, ["export_a"])

    def test_import_model_type_on_flow_node(self):

        with self.assertRaisesRegex(ex.EJobValidation, "unsupported model type \\[DATA_IMPORT_MODEL\\]"):
            self._run_export_flow(export_model_type="DATA_IMPORT_MODEL", connected_slots=("slot_1", "slot_2", "slot_3"))

    def test_export_model_in_run_model_job(self):

        job_config = {"job": {"runModel": {"parameters": {"storage_key": "export_a"}}}}
        job_config_path = self._write_yaml("job_config.yaml", job_config)

        with self.assertRaisesRegex(ex.EJobValidation, "cannot use model type \\[DATA_EXPORT_MODEL\\]"):
            launch.launch_model(SlotExportModel, job_config_path, self.sys_config_path, dev_mode=True)

    def test_export_node_storage_access(self):

        original_build_model = graph_builder.GraphBuilder.build_model
        storage_access = dict()

        def capture_storage_access(builder, *args, **kwargs):
            section = original_build_model(builder, *args, **kwargs)
            for node in section.nodes.values():
                if isinstance(node, graph.RunModelNode):
                    storage_access[node.model_def.entryPoint] = node.storage_access
            return section

        with mock.patch.object(graph_builder.GraphBuilder, "build_model", capture_storage_access):
            self._run_export_flow()

        self.assertEqual(["export_a"], storage_access[_entry_point(SlotExportModel)])
        self.assertIsNone(storage_access[_entry_point(SampleDataModel)])

    def test_export_node_storage_not_listed(self):

        with self.assertRaisesRegex(ex.ERuntimeValidation, "export_b"):
            self._run_export_flow(storage_key="export_b")

        self.assertFalse(self.tmp_dir.joinpath("export_b/slots.txt").exists())

    def test_standard_node_unconnected_optional_input(self):

        nodes = {
            "sample": self._sample_node(),
            "consumer": {"nodeType": "MODEL_NODE", "inputs": ["sample_data", "extra_data"], "outputs": ["combined_data"]},
            "export": self._export_node(["slot_1", "slot_2", "slot_3"])}

        edges = [self._edge("consumer", "combined_data", "export", "slot_1")]
        models = {"sample": SampleDataModel, "consumer": OptionalInputModel, "export": SlotExportModel}
        parameters = {"row_count": 5, "storage_key": "export_a"}

        with self.assertRaisesRegex(ex.EConfigParse, "extra_data is not provided by any node"):
            self._run_flow(nodes, edges, models, parameters, ["export_a"])

    def test_standard_node_terminal(self):

        nodes = {
            "sample": self._sample_node(),
            "consumer": {"nodeType": "MODEL_NODE", "inputs": ["sample_data", "extra_data"], "outputs": ["combined_data"]},
            "extra_data": {"nodeType": "INPUT_NODE"}}

        models = {"sample": SampleDataModel, "consumer": OptionalInputModel}

        with self.assertRaises(ex.ETrac):
            self._run_flow(nodes, [], models, {"row_count": 5}, [])


class FlowExportGraphTest(unittest.TestCase):

    """Build flows directly, as the platform supplies them to the runtime (no dev-mode auto-wiring)"""

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()

    @staticmethod
    def _build_graph(model_type: meta.ModelType, slot_1_optional: bool):

        input_schema = meta.ModelInputSchema(objectType=meta.ObjectType.DATA, optional=True, dynamic=True)

        model_def = meta.ModelDefinition(
            language="python", repository="trac_integrated",
            entryPoint=_entry_point(RequiredSlotExportModel), modelType=model_type,
            inputs={
                "slot_1": meta.ModelInputSchema(objectType=meta.ObjectType.DATA, optional=slot_1_optional, dynamic=True),
                "slot_2": input_schema})

        flow_def = meta.FlowDefinition(
            nodes={
                "source": meta.FlowNode(meta.FlowNodeType.INPUT_NODE),
                "export": meta.FlowNode(
                    meta.FlowNodeType.MODEL_NODE, inputs=["slot_1", "slot_2"],
                    modelType=meta.ModelType.DATA_EXPORT_MODEL)},
            edges=[meta.FlowEdge(meta.FlowSocket("source"), meta.FlowSocket("export", "slot_2"))],
            inputs={"source": input_schema})

        flow_id = util.new_object_id(meta.ObjectType.FLOW)
        model_id = util.new_object_id(meta.ObjectType.MODEL)

        job_def = meta.JobDefinition(
            jobType=meta.JobType.RUN_FLOW,
            runFlow=meta.RunFlowJob(
                flow=util.selector_for(flow_id),
                models={"export": util.selector_for(model_id)},
                exportStorageAccess=["export_a"]))

        job_config = cfg.JobConfig(
            jobId=util.new_object_id(meta.ObjectType.JOB), job=job_def,
            objects={
                util.object_key(flow_id): meta.ObjectDefinition(objectType=meta.ObjectType.FLOW, flow=flow_def),
                util.object_key(model_id): meta.ObjectDefinition(objectType=meta.ObjectType.MODEL, model=model_def)})

        builder = graph_builder.GraphBuilder(cfg.RuntimeConfig(), job_config)

        return builder.build_job(job_def)

    def test_unconnected_optional_input_is_empty(self):

        job_graph = self._build_graph(meta.ModelType.DATA_EXPORT_MODEL, slot_1_optional=True)

        empty_inputs = [
            node for node_id, node in job_graph.nodes.items()
            if node_id.name == "export.slot_1:EMPTY" and isinstance(node, graph.StaticValueNode)]

        self.assertEqual(1, len(empty_inputs))
        self.assertTrue(empty_inputs[0].value.is_empty())

    def test_unconnected_required_input(self):

        with self.assertRaisesRegex(ex.EJobValidation, "Socket \\[export.slot_1\\] is not connected"):
            self._build_graph(meta.ModelType.DATA_EXPORT_MODEL, slot_1_optional=False)

    def test_model_type_mismatch(self):

        with self.assertRaisesRegex(ex.EJobValidation, "requires model type \\[DATA_EXPORT_MODEL\\]"):
            self._build_graph(meta.ModelType.STANDARD_MODEL, slot_1_optional=True)
