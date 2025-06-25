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

import datetime as dt
import pathlib
import typing as _tp
import unittest

import tracdap.rt.api as api
import tracdap.rt.launch as launch
import tracdap.rt.exceptions as ex


class RuntimeMetadataTest(api.TracModel):

    TEST_CASE: unittest.TestCase

    def define_parameters(self) -> _tp.Dict[str, api.ModelParameter]:

        return api.define_parameters(
            api.P("param_1", api.INTEGER, label="Parameter 1", default_value=1)
        )

    def define_inputs(self) -> _tp.Dict[str, api.ModelInputSchema]:

        customer_loans = api.define_input_table(
            api.F("id", api.STRING, label="Customer account ID", business_key=True),
            api.F("loan_amount", api.DECIMAL, label="Principal loan amount"),
            api.F("total_pymnt", api.DECIMAL, label="Total amount repaid"),
            api.F("region", api.STRING, label="Customer home region", categorical=True),
            api.F("loan_condition_cat", api.INTEGER, label="Loan condition category"))

        return {"customer_loans": customer_loans}

    def define_outputs(self) -> _tp.Dict[str, api.ModelOutputSchema]:

        return {
            "data_report": api.define_output(api.define_file_type("md", "text/markdown"))
        }

    def run_model(self, ctx: api.TracContext):

        self.TEST_CASE.assertTrue(ctx.has_dataset("customer_loans"))

        metadata = ctx.get_metadata("customer_loans")

        self.TEST_CASE.assertEqual(api.ObjectType.DATA, metadata.objectId.objectType)
        self.TEST_CASE.assertTrue("trac_dev_mode" in metadata.attributes)
        self.TEST_CASE.assertTrue("trac_create_time" in metadata.attributes)
        self.TEST_CASE.assertTrue("trac_update_user_id" in metadata.attributes)
        self.TEST_CASE.assertEqual(True, metadata.attributes["trac_dev_mode"])
        self.TEST_CASE.assertEqual(dt.date.today(), metadata.attributes["trac_create_time"].date())
        self.TEST_CASE.assertEqual("local_user", metadata.attributes["trac_update_user_id"])

        # Trying to get metadata for an item that doesn't exist is an error
        self.TEST_CASE.assertRaises(ex.ERuntimeValidation, lambda: ctx.get_metadata("data_report"))

        # Trying to get metadata for model parameters is an error
        self.TEST_CASE.assertRaises(ex.ERuntimeValidation, lambda: ctx.get_metadata("param_1"))

        ctx.put_file("data_report", b"Test data report")

        output_metadata = ctx.get_metadata("data_report")

        # Metadata not available for outputs - it is only created after the job completes
        self.TEST_CASE.assertIsNone(output_metadata)


class AssertJobsTest(unittest.TestCase):

    examples_root: pathlib.Path

    @classmethod
    def setUpClass(cls) -> None:

        repo_root = pathlib.Path(__file__) \
            .parent \
            .joinpath("../../../../../..") \
            .resolve()

        cls.examples_root = repo_root.joinpath("examples/models/python")

    def test_runtime_metadata(self):

        RuntimeMetadataTest.TEST_CASE = self

        job_config = self.examples_root.joinpath("config/runtime_metadata.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(RuntimeMetadataTest, job_config, sys_config)
