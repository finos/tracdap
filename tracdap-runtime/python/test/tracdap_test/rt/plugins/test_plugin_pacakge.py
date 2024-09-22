#  Copyright 2024 Accenture Global Solutions Limited
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
import unittest
import subprocess as sp

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt._exec.runtime as runtime
import tracdap.rt._impl.util as util


class ConfigParserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def setUp(self) -> None:

        commit_hash_proc = sp.run(["git", "rev-parse", "HEAD"], stdout=sp.PIPE)
        self.commit_hash = commit_hash_proc.stdout.decode('utf-8').strip()

        current_repo_url = pathlib.Path(__file__) \
            .joinpath("../../../../../../..") \
            .resolve()

        repos = {
            "unit_test_repo": cfg.PluginConfig(
                protocol="local",
                properties={"repoUrl": str(current_repo_url)})
        }

        storage = cfg.StorageConfig(
            buckets={
                "storage_1": cfg.PluginConfig(
                    protocol="LOCAL",
                    properties={"rootPath": str(current_repo_url.joinpath("examples/models/python/data"))}
                )
            },
            defaultBucket="storage_1",
            defaultFormat="CSV"
        )

        self.sys_config = cfg.RuntimeConfig(repositories=repos, storage=storage)

    def test_ext_config_loader_sys_ok(self):

        plugin_package = "tracdap_test.rt.plugins.test_ext"

        trac_runtime = runtime.TracRuntime("test-ext:sys_config_HuX-7", plugin_packages=[plugin_package], dev_mode=True)
        trac_runtime.pre_start()

        # Check that the sys config contains what came from the TEST EXT loader plugin
        self.assertTrue("TEST_EXT_REPO" in trac_runtime._sys_config.repositories)

    def test_ext_config_loader_sys_not_found(self):

        plugin_package = "tracdap_test.rt.plugins.test_ext"

        trac_runtime = runtime.TracRuntime("test-ext:sys_config_unknown", plugin_packages=[plugin_package], dev_mode=True)

        # Config error gets wrapped in startup error
        self.assertRaises(ex.EStartup, lambda: trac_runtime.pre_start())

    def test_ext_config_loader_job_ok(self):

        plugin_package = "tracdap_test.rt.plugins.test_ext"

        trac_runtime = runtime.TracRuntime(self.sys_config, plugin_packages=[plugin_package], dev_mode=True)
        trac_runtime.pre_start()

        # Load a config object that exists
        job_config = trac_runtime.load_job_config("test-ext:job_config_A1-6")
        self.assertIsInstance(job_config, cfg.JobConfig)

    def test_ext_config_loader_job_not_found(self):

        plugin_package = "tracdap_test.rt.plugins.test_ext"

        trac_runtime = runtime.TracRuntime(self.sys_config, plugin_packages=[plugin_package], dev_mode=True)
        trac_runtime.pre_start()

        # Load a config object that does not exist
        self.assertRaises(ex.EConfigLoad, lambda: trac_runtime.load_job_config("test-ext:job_config_B1-9"))

    def test_ext_config_loader_wrong_protocol(self):

        plugin_package = "tracdap_test.rt.plugins.test_ext"

        trac_runtime = runtime.TracRuntime(self.sys_config, plugin_packages=[plugin_package], dev_mode=True)
        trac_runtime.pre_start()

        # Load a config object with the wrong protocol
        self.assertRaises(ex.EConfigLoad, lambda: trac_runtime.load_job_config("test-ext_2:job_config_B1-9"))
