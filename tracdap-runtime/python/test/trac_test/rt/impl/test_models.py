#  Copyright 2022 Accenture Global Solutions Limited
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

import typing as tp
import unittest
import pathlib

import trac.rt.api as api
import trac.rt.metadata as meta
import trac.rt.config as config
import trac.rt.impl.models as models
import trac.rt.impl.util as util


class SampleModel(api.TracModel):

    def define_parameters(self) -> tp.Dict[str, meta.ModelParameter]:

        return api.declare_parameters(
            api.P("param1", api.INTEGER, label="A first parameter"),
            api.P("param2", api.STRING, label="A second parameter"))

    def define_inputs(self) -> tp.Dict[str, api.ModelInputSchema]:

        input_table_1 = api.declare_input_table(
            api.F("input_field_1", api.STRING, label="Input field 1", business_key=True),
            api.F("input_field_2", api.INTEGER, label="Input field 2"),
            api.F("input_field_3", api.STRING, label="Input field 3", categorical=True),
            api.F("input_field_4", api.DECIMAL, label="Input field 4"))

        return {"input_table_1": input_table_1}

    def define_outputs(self) -> tp.Dict[str, api.ModelOutputSchema]:

        output_table_1 = api.declare_output_table(
            api.F("output_field_1", api.STRING, label="Output field 1", business_key=True),
            api.F("output_field_2", api.DATE, label="Output field 2"),
            api.F("output_field_3", api.FLOAT, label="Output field 3"))

        return {"output_table_1": output_table_1}

    def run_model(self, ctx: api.TracContext):
        pass


class ImportModelTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def setUp(self) -> None:
        self.test_scope = f"{self.__class__.__name__}.{self._testMethodName}"

    def test_load_integrated_ok(self):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["trac_integrated"] = config.RepositoryConfig(repoType="integrated")

        stub_model_def = meta.ModelDefinition(
            language="python",
            repository="trac_integrated",
            entryPoint="trac_test.rt.impl.test_models.SampleModel"
        )

        loader = models.ModelLoader(sys_config)
        loader.create_scope(self.test_scope)

        model_class = loader.load_model_class(self.test_scope, stub_model_def)
        model = model_class()

        self.assertIsInstance(model_class, api.TracModel.__class__)
        self.assertIsInstance(model, model_class)
        self.assertIsInstance(model, api.TracModel)

        loader.destroy_scope(self.test_scope)

    def test_load_local_ok(self):

        example_repo_url = pathlib.Path(__file__) \
            .joinpath("../../../../../../..") \
            .joinpath("examples/models/python") \
            .resolve()

        example_repo_config = config.RepositoryConfig(repoType="local", repoUrl=str(example_repo_url))

        sys_config = config.RuntimeConfig()
        sys_config.repositories["example_repo"] = example_repo_config

        stub_model_def = meta.ModelDefinition(
            language="python",
            repository="example_repo",
            path="hello_world",
            entryPoint="hello_world.HelloWorldModel"
        )

        loader = models.ModelLoader(sys_config)
        loader.create_scope(self.test_scope)

        model_class = loader.load_model_class(self.test_scope, stub_model_def)
        model = model_class()

        self.assertIsInstance(model_class, api.TracModel.__class__)
        self.assertIsInstance(model, model_class)
        self.assertIsInstance(model, api.TracModel)

        loader.destroy_scope(self.test_scope)

    def test_load_git_ok(self):

        trac_repo_url = "https://github.com/accenture/trac"
        trac_repo_version = "main"  # TODO: Loading models from a branch should be prohibited! Or converted to a hash

        example_repo_config = config.RepositoryConfig(
            repoType="git",
            repoUrl=trac_repo_url)

        sys_config = config.RuntimeConfig()
        sys_config.repositories["example_repo"] = example_repo_config

        stub_model_def = meta.ModelDefinition(
            language="python",
            repository="example_repo",
            path="examples/models/python/hello_world",
            entryPoint="hello_world.HelloWorldModel",
            version=trac_repo_version
        )

        loader = models.ModelLoader(sys_config)
        loader.create_scope(self.test_scope)

        model_class = loader.load_model_class(self.test_scope, stub_model_def)
        model = model_class()

        self.assertIsInstance(model_class, api.TracModel.__class__)
        self.assertIsInstance(model, model_class)
        self.assertIsInstance(model, api.TracModel)

        loader.destroy_scope(self.test_scope)

    def test_scan_model_ok(self):

        def _td(basic_type: meta.BasicType) -> meta.TypeDescriptor:
            return meta.TypeDescriptor(basic_type)

        sys_config = config.RuntimeConfig()
        loader = models.ModelLoader(sys_config)

        model_class = SampleModel
        model_def = loader.scan_model(model_class)

        self.assertIsInstance(model_def, meta.ModelDefinition)

        self.assertIsInstance(model_def.parameters, dict)
        self.assertEqual({"param1", "param2"}, set(model_def.parameters.keys()))
        self.assertEqual(_td(meta.BasicType.INTEGER), model_def.parameters["param1"].paramType, )
        self.assertEqual(_td(meta.BasicType.STRING), model_def.parameters["param2"].paramType)

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
