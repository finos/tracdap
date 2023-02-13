#  Copyright 2023 Accenture Global Solutions Limited
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

import tracdap.rt.api as trac
import tracdap.rt.exceptions as ex
import tracdap.rt._impl.guard_rails as guard  # noqa

import typing as tp
import unittest

from tracdap.rt.api import TracContext
from tracdap.rt_gen.domain.tracdap.metadata import ModelOutputSchema, ModelInputSchema, ModelParameter


def create_model(logic_func):

    class TestModel(trac.TracModel):

        def define_parameters(self) -> tp.Dict[str, ModelParameter]:
            pass

        def define_inputs(self) -> tp.Dict[str, ModelInputSchema]:
            pass

        def define_outputs(self) -> tp.Dict[str, ModelOutputSchema]:
            pass

        def run_model(self, ctx: TracContext):
            logic_func()

    return TestModel


class PythonGuardRailsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        guard.PythonGuardRails.protect_dangerous_functions()

    def test_builtin_from_model(self):

        test_model_class = create_model(lambda: exec("print('Hello world')"))
        test_model = test_model_class()

        self.assertRaises(ex.EModelValidation, lambda: test_model.run_model(None))  # noqa

    def test_builtin_outside_model(self):

        exec("print('Hello world')")
        self.assertTrue(True)
