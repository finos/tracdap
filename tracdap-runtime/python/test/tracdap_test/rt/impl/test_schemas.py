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

import pathlib
import typing as tp
import unittest

import tracdap.rt.api as trac
import tracdap.rt.launch as launch
import tracdap.rt.exceptions as ex
import tracdap.rt.impl.api_hook as api_hook
import tracdap.rt.impl.util as util

import tracdap_test.resources as test_resources


_run_model_guard_flag = []


class RunModelGuard(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("meaning_of_life", trac.FLOAT, "Sample parameter from hello world model"))

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        return {}

    def run_model(self, ctx: trac.TracContext):

        if _run_model_guard_flag:
            trac.load_schema(test_resources, "schema_sample.csv")


class SchemaResourcesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        api_hook.RuntimeHookImpl.register_impl()
        util.configure_logging()

    def test_load_from_package(self):

        schema = trac.load_schema(test_resources, "schema_sample.csv")

        self.assertIsInstance(schema, trac.SchemaDefinition)
        self.assertEqual(schema.schemaType, trac.SchemaType.TABLE)
        self.assertEqual(len(schema.table.fields), 2)

    def test_load_from_package_name(self):

        schema = trac.load_schema("tracdap_test.resources", "schema_sample.csv")

        self.assertIsInstance(schema, trac.SchemaDefinition)
        self.assertEqual(schema.schemaType, trac.SchemaType.TABLE)
        self.assertEqual(len(schema.table.fields), 2)

    def test_case_sensitivity(self):

        schema = trac.load_schema(test_resources, "schema_sample_2.csv")

        self.assertIsInstance(schema, trac.SchemaDefinition)
        self.assertEqual(schema.schemaType, trac.SchemaType.TABLE)
        self.assertEqual(len(schema.table.fields), 2)

    def test_invalid_arguments(self):

        # Passing in nulls or wrong types to the runtime API is a runtime validation error

        self.assertRaises(
            ex.ERuntimeValidation, lambda:
            trac.load_schema(None, "schema_sample.csv"))  # noqa

        self.assertRaises(
            ex.ERuntimeValidation, lambda:
            trac.load_schema(test_resources, None))  # noqa

        self.assertRaises(
            ex.ERuntimeValidation, lambda:
            trac.load_schema(object(), "schema_sample.csv"))  # noqa

        self.assertRaises(
            ex.ERuntimeValidation, lambda:
            trac.load_schema(test_resources, object()))  # noqa

    def test_package_not_found(self):

        # Missing package is a model repo resource not found error

        self.assertRaises(
            ex.EModelRepoResource, lambda:
            trac.load_schema("tracdap_test.nonexistent", "schema_sample.csv"))

    def test_schema_not_found(self):

        # Missing schema file is a model repo resource not found error

        self.assertRaises(
            ex.EModelRepoResource, lambda:
            trac.load_schema(test_resources, "not_found.csv"))

    def test_schema_empty(self):

        self.assertRaises(
            ex.EValidation, lambda:
            trac.load_schema(test_resources, "schema_empty.csv"))

    def test_schema_garbled(self):

        self.assertRaises(
            ex.EValidation, lambda:
            trac.load_schema(test_resources, "schema_garbled.dat"))

    def test_required_fields_missing(self):

        self.assertRaises(
            ex.EValidation, lambda:
            trac.load_schema(test_resources, "schema_invalid_1.csv"))

    def test_required_fields_null(self):

        self.assertRaises(
            ex.EValidation, lambda:
            trac.load_schema(test_resources, "schema_invalid_2.csv"))

    def test_invalid_field_type(self):
        
        self.assertRaises(
            ex.EValidation, lambda:
            trac.load_schema(test_resources, "schema_invalid_3.csv"))

    def test_run_model_guard(self):

        # Trying to use the load resource functions inside run_model is a runtime validation error
        # Because it is an illegal use of the runtime API

        repo_root = pathlib.Path(__file__) \
            .parent \
            .joinpath("../../../../../..") \
            .resolve()

        examples_root = repo_root.joinpath("examples/models/python")
        job_config = examples_root.joinpath("config/hello_world.yaml")
        sys_config = examples_root.joinpath("config/sys_config.yaml")

        # Make sure the model run completes when it is not trying to load resources

        _run_model_guard_flag.clear()

        launch.launch_model(RunModelGuard, job_config, sys_config)

        # Now turn on loading resources in run_model, and make sure there is an error

        _run_model_guard_flag.append(True)

        try:

            launch.launch_model(RunModelGuard, job_config, sys_config)
            self.fail("Expected exception was not raised")

        except Exception as e:

            # The engine wraps errors that occur during job execution
            # The cause of the engine failure error should be the original exception

            cause = e.__cause__
            self.assertIsInstance(cause, ex.ERuntimeValidation)
