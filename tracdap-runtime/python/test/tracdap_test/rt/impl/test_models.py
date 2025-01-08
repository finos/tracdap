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

import tracdap.rt.api as api
import tracdap.rt.metadata as meta
import tracdap.rt.config as config
import tracdap.rt.ext.plugins as plugins
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.models as models  # noqa
import tracdap.rt._impl.static_api as api_hook  # noqa
import tracdap.rt._impl.core.util as util  # noqa


class SampleModel(api.TracModel):

    def define_parameters(self) -> tp.Dict[str, meta.ModelParameter]:

        return api.define_parameters(
            api.P("param1", api.INTEGER, label="A first parameter"),
            api.P("param2", api.STRING, label="A second parameter"),
            api.P("param3", api.FLOAT, label="Param 3, test default type coercion", default_value=1))

    def define_inputs(self) -> tp.Dict[str, api.ModelInputSchema]:

        input_table_1 = api.define_input_table(
            api.F("input_field_1", api.STRING, label="Input field 1", business_key=True),
            api.F("input_field_2", api.INTEGER, label="Input field 2"),
            api.F("input_field_3", api.STRING, label="Input field 3", categorical=True),
            api.F("input_field_4", api.DECIMAL, label="Input field 4"))

        return {"input_table_1": input_table_1}

    def define_outputs(self) -> tp.Dict[str, api.ModelOutputSchema]:

        output_table_1 = api.define_output_table(
            api.F("output_field_1", api.STRING, label="Output field 1", business_key=True),
            api.F("output_field_2", api.DATE, label="Output field 2"),
            api.F("output_field_3", api.FLOAT, label="Output field 3"))

        return {"output_table_1": output_table_1}

    def run_model(self, ctx: api.TracContext):
        pass


class ImportModelTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:

        plugins.PluginManager.register_core_plugins()
        api_hook.StaticApiImpl.register_impl()
        log.configure_logging()

    def setUp(self) -> None:

        self.test_scope = f"{self.__class__.__name__}.{self._testMethodName}"
        self.scratch_dir = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self) -> None:

        util.try_clean_dir(self.scratch_dir, remove=True)

    def test_load_model(self, override_scope = None):

        if override_scope:
            self.test_scope = override_scope

        example_repo_url = pathlib.Path(__file__) \
            .parent \
            .joinpath("../../../../../..") \
            .resolve()

        example_repo_config = config.PluginConfig(
            protocol="local",
            properties={"repoUrl": str(example_repo_url)})

        sys_config = config.RuntimeConfig()
        sys_config.repositories["example_repo"] = example_repo_config

        stub_model_def = meta.ModelDefinition(
            language="python",
            repository="example_repo",
            path="examples/models/python/src",
            entryPoint="tutorial.hello_world.HelloWorldModel"
        )

        loader = models.ModelLoader(sys_config, self.scratch_dir)
        loader.create_scope(self.test_scope)

        model_class = loader.load_model_class(self.test_scope, stub_model_def)
        model = model_class()

        self.assertIsInstance(model_class, api.TracModel.__class__)
        self.assertIsInstance(model, api.TracModel)

        loader.destroy_scope(self.test_scope)

    def test_load_model_long_path(self):

        long_path_scope = "long_" + "A" * 250
        self.test_load_model(long_path_scope)

    def test_load_model_integrated(self):

        # Integrated repo uses a different loader mechanism so include a test here
        # All other repo types copy into the loader scope, so loader behavior is the same as local
        # Also, tests for remote repo types are integration tests

        sys_config = config.RuntimeConfig()
        sys_config.repositories["trac_integrated"] = config.PluginConfig(protocol="integrated")

        stub_model_def = meta.ModelDefinition(
            language="python",
            repository="trac_integrated",
            entryPoint="tracdap_test.rt.impl.test_models.SampleModel"
        )

        loader = models.ModelLoader(sys_config, self.scratch_dir)
        loader.create_scope(self.test_scope)

        model_class = loader.load_model_class(self.test_scope, stub_model_def)
        model = model_class()

        self.assertIsInstance(model_class, api.TracModel.__class__)
        self.assertIsInstance(model, api.TracModel)

        loader.destroy_scope(self.test_scope)

    def test_scan_model(self):

        def _td(basic_type: meta.BasicType) -> meta.TypeDescriptor:
            return meta.TypeDescriptor(basic_type)

        sys_config = config.RuntimeConfig()
        loader = models.ModelLoader(sys_config, self.scratch_dir)

        model_class = SampleModel
        model_def = loader.scan_model(meta.ModelDefinition(), model_class)

        self.assertIsInstance(model_def, meta.ModelDefinition)

        self.assertIsInstance(model_def.parameters, dict)
        self.assertEqual({"param1", "param2", "param3"}, set(model_def.parameters.keys()))
        self.assertEqual(_td(meta.BasicType.INTEGER), model_def.parameters["param1"].paramType, )
        self.assertEqual(_td(meta.BasicType.STRING), model_def.parameters["param2"].paramType)
        self.assertEqual(_td(meta.BasicType.FLOAT), model_def.parameters["param3"].paramType)
        # Check type coercion is correctly applied to the default parameter
        self.assertEqual(_td(meta.BasicType.FLOAT), model_def.parameters["param3"].defaultValue.type)

        self.assertIsInstance(model_def.inputs, dict)
        self.assertEqual({"input_table_1"}, set(model_def.inputs.keys()))
        self.assertEqual(meta.SchemaType.TABLE, model_def.inputs["input_table_1"].schema.schemaType)

        input_table_schema = model_def.inputs["input_table_1"].schema.table
        self.assertIsInstance(input_table_schema, meta.TableSchema)
        self.assertEqual(4, len(input_table_schema.fields))
        self.assertEqual(0, input_table_schema.fields[0].fieldOrder)
        self.assertEqual("input_field_2", input_table_schema.fields[1].fieldName)
        self.assertEqual(meta.BasicType.STRING, input_table_schema.fields[2].fieldType)
        self.assertEqual("Input field 4", input_table_schema.fields[3].label)
        self.assertEqual(True, input_table_schema.fields[0].businessKey)
        self.assertEqual(True, input_table_schema.fields[2].categorical)

        self.assertIsInstance(model_def.outputs, dict)
        self.assertEqual({"output_table_1"}, set(model_def.outputs.keys()))
        self.assertEqual(meta.SchemaType.TABLE, model_def.outputs["output_table_1"].schema.schemaType)

        output_table_schema = model_def.outputs["output_table_1"].schema.table
        self.assertIsInstance(output_table_schema, meta.TableSchema)
        self.assertEqual(3, len(output_table_schema.fields),)
        self.assertEqual("output_field_1", output_table_schema.fields[0].fieldName)
        self.assertEqual(1, output_table_schema.fields[1].fieldOrder)
        self.assertEqual(meta.BasicType.FLOAT, output_table_schema.fields[2].fieldType)
        self.assertEqual("Output field 3", output_table_schema.fields[2].label)
        self.assertEqual(True, output_table_schema.fields[0].businessKey)
