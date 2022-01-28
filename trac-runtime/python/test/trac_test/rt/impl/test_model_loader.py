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
import trac.rt.impl.model_loader as models
import trac.rt.impl.util as util


class SampleModel(api.TracModel):

    def define_parameters(self) -> tp.Dict[str, meta.ModelParameter]:
        pass

    def define_inputs(self) -> tp.Dict[str, meta.ModelInputSchema]:
        pass

    def define_outputs(self) -> tp.Dict[str, meta.ModelOutputSchema]:
        pass

    def run_model(self, ctx: api.TracContext):
        pass


class ImportModelTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def setUp(self) -> None:
        self.test_scope = f"{self.__class__.__name__}.{self._testMethodName}"

    def test_integrated_ok(self):

        sys_config = config.RuntimeConfig()
        sys_config.repositories["trac_integrated"] = config.RepositoryConfig(repoType="integrated")

        stub_model_def = meta.ModelDefinition(
            language="python",
            repository="trac_integrated",
            entryPoint="trac_test.rt.impl.test_model_loader.SampleModel"
        )

        loader = models.ModelLoader(sys_config)
        loader.create_scope(self.test_scope)

        model_class = loader.load_model_class(self.test_scope, stub_model_def)
        model = model_class()

        self.assertIsInstance(model_class, api.TracModel.__class__)
        self.assertIsInstance(model, model_class)
        self.assertIsInstance(model, api.TracModel)

        loader.destroy_scope(self.test_scope)

    def test_local_ok(self):

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
