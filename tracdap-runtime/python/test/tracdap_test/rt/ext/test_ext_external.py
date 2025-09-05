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

import typing as _tp
import pathlib
import tempfile as temp
import unittest
from http.client import HTTPConnection

import tracdap.rt.api as trac
import tracdap.rt.ext.plugins as plugins
import tracdap.rt.launch as launch
import tracdap.rt.metadata as meta
import tracdap.rt._impl.runtime as runtime  # noqa
import tracdap.rt._impl.core.config_parser as cfg  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.type_system as trac_types  # noqa
import tracdap.rt._impl.core.util as util  # noqa

ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()


class ExternalSystemModel(trac.TracModel):

    TEST_CASE: unittest.TestCase

    def define_parameters(self) -> _tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(
            trac.P("download_path", trac.STRING, "URL path of the file to download"),
            trac.P("first_line", trac.STRING, "Expected first line of content")
        )

    def define_inputs(self) -> _tp.Dict[str, trac.ModelInputSchema]:
        return {}

    def define_outputs(self) -> _tp.Dict[str, trac.ModelOutputSchema]:
        return {}

    def define_resources(self) -> _tp.Dict[str, trac.ModelResource]:

        return {
            "github_content": trac.define_external_system(
                protocol="test-ext-external",
                client_type=HTTPConnection)
        }

    def run_model(self, ctx: trac.TracContext):

        download_path = ctx.get_parameter("download_path")
        expected_first_line = ctx.get_parameter("first_line")

        with ctx.get_external_system("github_content", HTTPConnection) as github_content:

            github_content.connect()
            github_content.request("GET", download_path)

            response = github_content.getresponse()
            response_data = response.read()
            response_text = response_data.decode("utf-8")
            actual_first_line = response_text.splitlines()[0]

            self.TEST_CASE.assertEqual(expected_first_line, actual_first_line)


class ExtExternalSystemTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        log.configure_logging()
        plugins.PluginManager.register_core_plugins()

    def setUp(self) -> None:

        ExternalSystemModel.TEST_CASE = self

        sys_config = cfg.RuntimeConfig()
        sys_config.properties["storage.default.location"] = "storage_1"
        sys_config.properties["storage.default.format"] = "CSV"

        sys_config.resources["storage_1"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.INTERNAL_STORAGE,
            protocol="LOCAL",
            properties={"rootPath": str(ROOT_DIR.joinpath("examples/models/python/data"))})

        sys_config.resources["github_content"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.EXTERNAL_SYSTEM,
            protocol="test-ext-external",
            properties={
                "scheme": "https",
                "host": "raw.githubusercontent.com",
                "port": "443"
            })

        self.sys_config = sys_config
        self.temp_dir = temp.TemporaryDirectory()

        self._write_config(sys_config, "sys_config.json")

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_config(self, config_obj, config_file):
        with open(self._config_path(config_file), "wt") as stream:
            sys_config_json = cfg.ConfigQuoter.quote(config_obj, "json")
            stream.write(sys_config_json)

    def _config_path(self, config_file):
        return pathlib.Path(self.temp_dir.name).joinpath(config_file)

    def test_full_runtime_ok(self):

        plugin_package = "tracdap_test.rt.ext.plugins"
        download_path = "https://raw.githubusercontent.com/finos/tracdap/refs/heads/main/README.md"
        first_line = "# ![TRAC: The modern model platform](doc/_images/tracmmp_horizontal_400.png)"

        job_config = cfg.JobConfig(job=meta.JobDefinition(
            runModel=meta.RunModelJob(
                parameters={  # noqa
                    "download_path": download_path,
                    "first_line": first_line
                }
            )
        ))

        self._write_config(job_config, "job_config.json")

        launch.launch_model(
            ExternalSystemModel,
            self._config_path("job_config.json"),
            self._config_path("sys_config.json"),
            plugin_package=plugin_package)
