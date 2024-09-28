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

import unittest
import pathlib
import sys

import tracdap.rt.launch as launch


class TutorialModelsTest(unittest.TestCase):

    __original_python_path = sys.path

    examples_root: pathlib.Path

    @classmethod
    def setUpClass(cls) -> None:

        repo_root = pathlib.Path(__file__) \
            .parent \
            .joinpath("../../../..") \
            .resolve()

        cls.examples_root = repo_root.joinpath("examples/models/python")

        examples_src = str(cls.examples_root.joinpath("src"))

        sys.path.append(examples_src)

    @classmethod
    def tearDownClass(cls) -> None:

        sys.path = cls.__original_python_path

    def test_hello_world(self):

        from tutorial.hello_world import HelloWorldModel  # noqa

        job_config = self.examples_root.joinpath("config/hello_world.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(HelloWorldModel, job_config, sys_config)

    def test_using_data(self):

        from tutorial.using_data import UsingDataModel  # noqa

        job_config = self.examples_root.joinpath("config/using_data.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(UsingDataModel, job_config, sys_config)

    def test_schema_files(self):

        from tutorial.schema_files import SchemaFilesModel  # noqa

        job_config = self.examples_root.joinpath("config/using_data.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(SchemaFilesModel, job_config, sys_config)

    def test_optional_io(self):

        # First invocation does not supply the optional input

        from tutorial.optional_io import OptionalIOModel  # noqa

        job_config = self.examples_root.joinpath("config/optional_io.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(OptionalIOModel, job_config, sys_config)

    def test_optional_io_2(self):

        # Second invocation supplies the optional input

        from tutorial.optional_io import OptionalIOModel  # noqa

        job_config = self.examples_root.joinpath("config/optional_io_2.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(OptionalIOModel, job_config, sys_config)

    def test_dynamic_io(self):

        # First invocation does not supply the optional input

        from tutorial.dynamic_io import DynamicIOModel  # noqa

        job_config = self.examples_root.joinpath("config/dynamic_io.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(DynamicIOModel, job_config, sys_config)

    def test_chaining(self):

        job_config = self.examples_root.joinpath("config/chaining.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)

    def test_chaining_2(self):

        job_config = self.examples_root.joinpath("config/chaining_2.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)
