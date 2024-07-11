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

import platform
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

    def test_checkout_local(self):

        checkout_key = "local_short"
        self._test_checkout_local(checkout_key)

    def test_checkout_local_long_path(self):

        # Using Python Git on Windows, the checkout dir path is allowed to exceed Windows MAX_PATH
        # However no individual path segment can be longer than 255 chars

        checkout_key = "local_long_" + "A" * 244
        self._test_checkout_local(checkout_key)

    def _test_checkout_local(self, checkout_key):

        local_repo_url = pathlib.Path(__file__) \
            .parent \
            .joinpath("../../../../../..") \
            .resolve()

        sys_config = config.RuntimeConfig()
        sys_config.repositories["local_test"] = config.PluginConfig(
            protocol="local",
            properties={"repoUrl": str(local_repo_url)})

        model_def = meta.ModelDefinition(
            language="python",
            repository="local_test",
            entryPoint="tutorial.hello_world.HelloWorldModel",
            path="examples/models/python/src"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("local_test")

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_key)
        safe_checkout_dir = util.windows_unc_path(checkout_dir)
        safe_checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.do_checkout(model_def, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tutorial/hello_world.py").exists())

    def test_checkout_git_native(self):

        checkout_key = "git_native_short"
        self._test_checkout_git_native(checkout_key)

    def test_checkout_git_native_long_path(self):

        # Using native Git on Windows, the checkout dir cannot exceed the Windows max path length
        # Specifically, checkout_dir/.git/config must be reachable, to enable the long paths setting
        # However, paths for the items inside the repo can (and should) exceed the max path length

        prefix_length = len(str(self.scratch_dir.joinpath("git_test", "git_native_long_")))
        checkout_key = "git_native_long_" + "A" * (240 - prefix_length)

        self._test_checkout_git_native(checkout_key)

    def _test_checkout_git_native(self, checkout_key):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["git_test"] = config.PluginConfig(
            protocol="git",
            properties={
                "repoUrl": "https://github.com/finos/tracdap"})

        model_def = meta.ModelDefinition(
            language="python",
            repository="git_test",
            packageGroup="finos",
            package="tracdap",
            version="v0.6.0",
            entryPoint="tutorial.hello_world.HelloWorldModel",
            path="examples/models/python/src"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("git_test")

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_key)
        safe_checkout_dir = util.windows_unc_path(checkout_dir)
        safe_checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.do_checkout(model_def, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tutorial/hello_world.py").exists())

    def test_checkout_git_python(self):

        checkout_key = "git_python_short"
        self._test_checkout_git_python(checkout_key)

    def test_checkout_git_python_long_path(self):

        # Using Python Git on Windows, the checkout dir path is allowed to exceed Windows MAX_PATH
        # However no individual path segment can be longer than 255 chars

        checkout_key = "git_python_long_" + "A" * 230
        self._test_checkout_git_python(checkout_key)

    def _test_checkout_git_python(self, checkout_key):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["git_test"] = config.PluginConfig(
            protocol="git",
            properties={
                "repoUrl": "https://github.com/finos/tracdap",
                "nativeGit": "false"})

        # On macOS, SSL certificates are not set up correctly by default in urllib3
        # We can reconfigure them by passing Git config properties into the pure python Git client
        if platform.system() == "Darwin":
            sys_config.repositories["git_test"].properties["git.http.sslCaInfo"] = "/etc/ssl/cert.pem"

        model_def = meta.ModelDefinition(
            language="python",
            repository="git_test",
            packageGroup="finos",
            package="tracdap",
            version="v0.6.0",
            entryPoint="tutorial.hello_world.HelloWorldModel",
            path="examples/models/python/src"
        )

        repo_mgr = repos.RepositoryManager(sys_config)
        repo = repo_mgr.get_repository("git_test")

        checkout_dir = self.scratch_dir.joinpath(model_def.repository, checkout_key)
        safe_checkout_dir = util.windows_unc_path(checkout_dir)
        safe_checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)

        package_dir = repo.do_checkout(model_def, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tutorial/hello_world.py").exists())

    def test_checkout_pypi(self):

        checkout_key = "pypi_short"
        self._test_checkout_pypi(checkout_key)

    def test_checkout_pypi_long_path(self):

        checkout_key = "pypi_long_" + "A" * 245  # key is 255 chars
        self._test_checkout_pypi(checkout_key)

    def _test_checkout_pypi(self, checkout_key):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["pypi_test"] = config.PluginConfig(
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

        package_dir = repo.do_checkout(model_def, checkout_dir)
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
        sys_config.repositories["pypi_test"] = config.PluginConfig(
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

        package_dir = repo.do_checkout(model_def, checkout_dir)
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
        sys_config.repositories["pypi_test"] = config.PluginConfig(
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

        package_dir = repo.do_checkout(model_def, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tracdap").exists())
