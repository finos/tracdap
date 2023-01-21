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

import tempfile
import unittest
import pathlib
import subprocess as sp

import tracdap.rt.metadata as meta
import tracdap.rt.config as config
import tracdap.rt.ext.plugins as plugins
import tracdap.rt._impl.static_api as api_hook  # noqa
import tracdap.rt._impl.models as models  # noqa
import tracdap.rt._impl.repos as repos  # noqa
import tracdap.rt._impl.util as util  # noqa


class ModelRepositoriesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        plugins.PluginManager.register_core_plugins()
        api_hook.StaticApiImpl.register_impl()
        util.configure_logging()

    def setUp(self) -> None:
        self.test_scope = f"{self.__class__.__name__}.{self._testMethodName}"

        repo_url_proc = sp.run(["git", "config", "--get", "remote.origin.url"], stdout=sp.PIPE)
        commit_hash_proc = sp.run(["git", "rev-parse", "HEAD"], stdout=sp.PIPE)

        if repo_url_proc.returncode != 0 or commit_hash_proc.returncode != 0:
            raise RuntimeError("Could not discover details of the current git repo")

        self.repo_url = repo_url_proc.stdout.decode('utf-8').strip()
        self.commit_hash = commit_hash_proc.stdout.decode('utf-8').strip()

        self.scratch_dir = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self) -> None:

        util.try_clean_dir(self.scratch_dir, remove=True)

    def test_checkout_git(self):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["git_test"] = config.PluginConfig(
            protocol="git",
            properties={
                "repoUrl": "https://github.com/finos/tracdap",
                "token": "ghe_UNUSED_TOKEN"})

        model_def = meta.ModelDefinition(
            language="python",
            repository="git_test",
            packageGroup="finos",
            package="tracdap",
            version="v0.5.0",
            entryPoint="tutorial.hello_world.HelloWorldModel",
            path="examples/models/python/src"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("git_test")

        checkout_key = "test_checkout_git"
        checkout_subdir = pathlib.Path(checkout_key)

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_subdir)
        checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.do_checkout(model_def, checkout_dir)

        self.assertTrue(package_dir.joinpath("tutorial/hello_world.py").exists())

    def test_checkout_pypi(self):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["pypi_test"] = config.PluginConfig(
            protocol="pypi",
            properties={
                "pipIndex": "https://pypi.python.org/pypi",
                "username": "pypi_TEST_USER",
                "password": "pypi_TEST_PASSWORD"})

        model_def = meta.ModelDefinition(
            language="python",
            repository="pypi_test",
            package="tracdap-runtime",
            version="0.5.0",
            entryPoint="tutorial.hello_world.HelloWorldModel"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("pypi_test")

        checkout_key = "test_checkout_pypi"
        checkout_subdir = pathlib.Path(checkout_key)

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_subdir)
        checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.do_checkout(model_def, checkout_dir)

        self.assertTrue(package_dir.joinpath("tracdap").exists())

    def test_checkout_pypi_simple_json(self):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["pypi_test"] = config.PluginConfig(
            protocol="pypi",
            properties={
                "pipIndexUrl": "https://pypi.python.org/simple",
                "pipSimpleFormat": "json",  # This is the default
                "username": "pypi_TEST_USER",
                "password": "pypi_TEST_PASSWORD"})

        model_def = meta.ModelDefinition(
            language="python",
            repository="pypi_test",
            package="tracdap-runtime",
            version="0.5.0",
            entryPoint="tutorial.hello_world.HelloWorldModel"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("pypi_test")

        checkout_key = "test_checkout_pypi_simple_json"
        checkout_subdir = pathlib.Path(checkout_key)

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_subdir)
        checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.do_checkout(model_def, checkout_dir)

        self.assertTrue(package_dir.joinpath("tracdap").exists())

    def test_checkout_pypi_simple_html(self):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["pypi_test"] = config.PluginConfig(
            protocol="pypi",
            properties={
                "pipIndexUrl": "https://pypi.python.org/simple",
                "pipSimpleFormat": "html",
                "username": "pypi_TEST_USER",
                "password": "pypi_TEST_PASSWORD"})

        model_def = meta.ModelDefinition(
            language="python",
            repository="pypi_test",
            package="tracdap-runtime",
            version="0.5.0",
            entryPoint="tutorial.hello_world.HelloWorldModel"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("pypi_test")

        checkout_key = "test_checkout_pypi_simple_html"
        checkout_subdir = pathlib.Path(checkout_key)

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_subdir)
        checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.do_checkout(model_def, checkout_dir)

        self.assertTrue(package_dir.joinpath("tracdap").exists())
