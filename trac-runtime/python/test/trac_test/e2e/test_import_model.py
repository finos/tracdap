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
import typing as _tp
import unittest
import pathlib

import trac.rt.metadata as meta
import trac.rt.config as config
import trac.rt.launch as launch


import trac.rt.api as api
from trac.rt import metadata as _meta
from trac.rt.api import TracContext


class SampleModel(api.TracModel):

    def define_parameters(self) -> _tp.Dict[str, _meta.ModelParameter]:
        pass

    def define_inputs(self) -> _tp.Dict[str, _meta.ModelInputSchema]:
        pass

    def define_outputs(self) -> _tp.Dict[str, _meta.ModelOutputSchema]:
        pass

    def run_model(self, ctx: TracContext):
        pass


class ImportModelTest(unittest.TestCase):

    def test_model_import_ok(self):

        sys_config_path = pathlib.Path(__file__).parent.joinpath("sys_config.yaml")
        job_config_path = pathlib.Path(__file__).parent.joinpath("test_import_model.yaml")

        launch.launch_job(job_config_path, sys_config_path, dev_mode=True)
