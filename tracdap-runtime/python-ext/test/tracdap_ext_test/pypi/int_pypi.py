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

import tempfile
import unittest
import pathlib

import tracdap.rt.metadata as meta
import tracdap.rt.config as config

import tracdap.rt._impl.core.plugins as plugins  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.static_api as api_hook  # noqa
import tracdap.rt._impl.core.repos as repos  # noqa
import tracdap.rt._impl.core.util as util  # noqa


class PyPIExtensionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        plugins.PluginManagerImpl.register_trac_extensions()
        api_hook.StaticApiImpl.register_impl()
        log.configure_logging()

    def setUp(self) -> None:

        self.scratch_dir = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self) -> None:

        util.try_clean_dir(self.scratch_dir, remove=True)

    def test_checkout_pypi(self):

        checkout_key = "pypi_short"
        self._test_checkout_pypi(checkout_key)

    def test_checkout_pypi_long_path(self):

        checkout_key = "pypi_long_" + "A" * 245  # key is 255 chars
        self._test_checkout_pypi(checkout_key)

    def _test_checkout_pypi(self, checkout_key):

        sys_config = config.RuntimeConfig()
        sys_config.resources["pypi_test"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.MODEL_REPOSITORY,
            protocol="pypi",
            properties={
                "pipIndex": "https://pypi.python.org/pypi",
                "username": "pypi_TEST_USER",
                "password": "pypi_TEST_PASSWORD"})

        # entryPoint does not exist in the package
        # But we only check out, we don't actually try to load the model

        model_def = meta.ModelDefinition(
            language="python",
            repository="pypi_test",
            package="tracdap-runtime",
            version="0.6.0",
            entryPoint="tutorial.hello_world.HelloWorldModel"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("pypi_test")

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_key)
        safe_checkout_dir = util.windows_unc_path(checkout_dir)
        safe_checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.checkout(model_def, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tracdap").exists())

    def test_checkout_pypi_simple_json(self):

        checkout_key = "pypi_simple_json_short"
        self._test_checkout_pypi_simple_json(checkout_key)

    def test_checkout_pypi_simple_json_long_path(self):

        checkout_key = "pypi_simple_json_long_" + "A" * 233  # key is 255 chars
        self._test_checkout_pypi_simple_json(checkout_key)

    def _test_checkout_pypi_simple_json(self, checkout_key):

        sys_config = config.RuntimeConfig()
        sys_config.resources["pypi_test"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.MODEL_REPOSITORY,
            protocol="pypi",
            properties={
                "pipIndexUrl": "https://pypi.python.org/simple",
                "pipSimpleFormat": "json",  # This is the default
                "username": "pypi_TEST_USER",
                "password": "pypi_TEST_PASSWORD"})

        # entryPoint does not exist in the package
        # But we only check out, we don't actually try to load the model

        model_def = meta.ModelDefinition(
            language="python",
            repository="pypi_test",
            package="tracdap-runtime",
            version="0.6.0",
            entryPoint="tutorial.hello_world.HelloWorldModel"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("pypi_test")

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_key)
        safe_checkout_dir = util.windows_unc_path(checkout_dir)
        safe_checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.checkout(model_def, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tracdap").exists())

    def test_checkout_pypi_simple_html(self):

        checkout_key = "pypi_simple_html_short"
        self._test_checkout_pypi_simple_html(checkout_key)

    def test_checkout_pypi_simple_html_long_path(self):

        checkout_key = "pypi_simple_html_long_" + "A" * 233  # key is 255 chars
        self._test_checkout_pypi_simple_html(checkout_key)

    def _test_checkout_pypi_simple_html(self, checkout_key):

        sys_config = config.RuntimeConfig()
        sys_config.resources["pypi_test"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.MODEL_REPOSITORY,
            protocol="pypi",
            properties={
                "pipIndexUrl": "https://pypi.python.org/simple",
                "pipSimpleFormat": "html",
                "username": "pypi_TEST_USER",
                "password": "pypi_TEST_PASSWORD"})

        # entryPoint does not exist in the package
        # But we only check out, we don't actually try to load the model

        model_def = meta.ModelDefinition(
            language="python",
            repository="pypi_test",
            package="tracdap-runtime",
            version="0.6.0",
            entryPoint="tutorial.hello_world.HelloWorldModel"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("pypi_test")

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_key)
        safe_checkout_dir = util.windows_unc_path(checkout_dir)
        safe_checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.checkout(model_def, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tracdap").exists())
