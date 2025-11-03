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
import subprocess as sp

import tracdap.rt.metadata as meta
import tracdap.rt.config as config
import tracdap.rt._impl.core.plugins as plugins  # noqa
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.models as models  # noqa
import tracdap.rt._impl.static_api as api_hook  # noqa
import tracdap.rt._impl.core.repos as repos  # noqa
import tracdap.rt._impl.core.util as util  # noqa


class ModelRepositoriesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        plugins.PluginManagerImpl.register_core_plugins()
        api_hook.StaticApiImpl.register_impl()
        log.configure_logging()

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
            .joinpath("../../../../../../..") \
            .resolve()

        sys_config = config.RuntimeConfig()
        sys_config.resources["local_test"] = meta.ResourceDefinition(
            resourceType=meta.ResourceType.MODEL_REPOSITORY,
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

        package = self._package_for_model_def(model_def)
        package_dir = repo.checkout(package, checkout_dir)
        safe_package_dir = util.windows_unc_path(package_dir)

        self.assertTrue(safe_package_dir.joinpath("tutorial/hello_world.py").exists())

    @staticmethod
    def _package_for_model_def(model_def: meta.ModelDefinition) -> repos.ModelPackage:

        return repos.ModelPackage(
            language=model_def.language,
            repository=model_def.repository,
            packageGroup=model_def.packageGroup,
            package=model_def.package,
            version=model_def.version,
            path=model_def.path)
