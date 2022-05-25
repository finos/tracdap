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

_ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../..") \
    .resolve()

_EXAMPLES_DIR = _ROOT_DIR.joinpath("examples/models/python")


class ChainingExample(unittest.TestCase):

    def test_chaining(self):

        job_config = _EXAMPLES_DIR.joinpath("chaining/chaining.yaml")
        sys_config = _EXAMPLES_DIR.joinpath("sys_config.yaml")
        test_dir = str(_EXAMPLES_DIR.joinpath("chaining"))

        try:

            sys.path.append(test_dir)

            launch.launch_job(job_config, sys_config, dev_mode=True)

            self.assertTrue(True)

        finally:

            sys.path.remove(test_dir)
