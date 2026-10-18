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

import unittest
import pathlib
import sys

import tracdap.rt.launch as launch

# Optional example dependencies are imported before any job runs, while guard rails are not yet active
try:
    import matplotlib  # noqa
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    import pptx  # noqa
    PPTX_AVAILABLE = True
except ImportError:
    PPTX_AVAILABLE = False


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

    def test_quick_start(self):

        from tutorial.quick_start import QuickStartModel  # noqa

        job_config = self.examples_root.joinpath("config/quick_start.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(QuickStartModel, job_config, sys_config)

    def test_using_data(self):

        from tutorial.using_data import PnlAggregation  # noqa

        job_config = self.examples_root.joinpath("config/using_data.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(PnlAggregation, job_config, sys_config)

    def test_schema_files(self):

        from tutorial.schema_files import PnlAggregationSchemas  # noqa

        job_config = self.examples_root.joinpath("config/using_data.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(PnlAggregationSchemas, job_config, sys_config)

    def test_chaining(self):

        job_config = self.examples_root.joinpath("config/chaining.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)

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

        from tutorial.dynamic_io import DynamicSchemaInspection  # noqa

        job_config = self.examples_root.joinpath("config/dynamic_io.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(DynamicSchemaInspection, job_config, sys_config)

    def test_dynamic_io_2(self):

        # First invocation does not supply the optional input

        from tutorial.dynamic_io import DynamicGenerator  # noqa

        job_config = self.examples_root.joinpath("config/dynamic_io_2.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(DynamicGenerator, job_config, sys_config)

    def test_dynamic_io_3(self):

        # First invocation does not supply the optional input

        from tutorial.dynamic_io import DynamicDataFilter  # noqa

        job_config = self.examples_root.joinpath("config/dynamic_io_3.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(DynamicDataFilter, job_config, sys_config)

    def test_dynamic_chaining(self):

        job_config = self.examples_root.joinpath("config/dynamic_chaining.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)

    def test_capture_job(self):

        job_config = self.examples_root.joinpath("config/capture_job.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)

    def test_data_export(self):

        job_config = self.examples_root.joinpath("config/data_export.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)

    def test_chaining_export(self):

        job_config = self.examples_root.joinpath("config/chaining_export.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)

    def test_using_polars(self):

        from tutorial.using_polars import PnlAggregationPolars  # noqa

        job_config = self.examples_root.joinpath("config/using_data.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(PnlAggregationPolars, job_config, sys_config, dev_mode=True)

    def test_structured_objects(self):

        from tutorial.structured_objects import StructModel  # noqa

        job_config = self.examples_root.joinpath("config/structured_objects.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(StructModel, job_config, sys_config, dev_mode=True)

    def test_chaining_struct(self):

        job_config = self.examples_root.joinpath("config/chaining_struct.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_job(job_config, sys_config, dev_mode=True)

    def test_runtime_metadata(self):

        from tutorial.runtime_metadata import RuntimeMetadataReport  # noqa

        job_config = self.examples_root.joinpath("config/runtime_metadata.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(RuntimeMetadataReport, job_config, sys_config, dev_mode=True)

    @unittest.skipIf(not MATPLOTLIB_AVAILABLE, "File IO example requires matplotlib")
    def test_file_io_matplotlib(self):

        from tutorial.file_io_matplotlib import MatplotlibReport  # noqa

        job_config = self.examples_root.joinpath("config/file_io_matplotlib.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(MatplotlibReport, job_config, sys_config, dev_mode=True)

    @unittest.skipIf(not PPTX_AVAILABLE, "File IO example requires python-pptx")
    def test_file_io_powerpoint(self):

        from tutorial.file_io_powerpoint import PowerpointReport  # noqa

        job_config = self.examples_root.joinpath("config/file_io_powerpoint.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(PowerpointReport, job_config, sys_config, dev_mode=True)

    @unittest.skipIf(not MATPLOTLIB_AVAILABLE, "Model resources example requires matplotlib")
    def test_load_resources(self):

        from tutorial.load_resources import ResourcesExample  # noqa

        job_config = self.examples_root.joinpath("config/load_resources.yaml")
        sys_config = self.examples_root.joinpath("config/sys_config.yaml")

        launch.launch_model(ResourcesExample, job_config, sys_config, dev_mode=True)
