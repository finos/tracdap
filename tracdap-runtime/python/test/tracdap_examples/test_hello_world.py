#  Copyright 2021 Accenture Global Solutions Limited
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
import importlib.util
import sys

import tracdap.rt.launch as launch


class HelloWorldExample(unittest.TestCase):

    def test_hello_world(self):

        job_config = 'hello_world.yaml'
        sys_config = '../sys_config.yaml'

        spec = importlib.util.spec_from_file_location("hello_world", "examples/models/python/hello_world/hello_world.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        model_class = module.__dict__["HelloWorldModel"]

        launch.launch_model(model_class, job_config, sys_config)
