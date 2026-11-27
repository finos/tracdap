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

import tracdap.rt.api as trac
import tracdap.rt.exceptions as ex
import tracdap.rt._impl.core.guard_rails as guard  # noqa

import typing as tp
import unittest
import sys

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

    def test_sys_exit_inside_model(self):

        test_model_class = create_model(lambda: sys.exit(-1))
        test_model = test_model_class()

        self.assertRaises(ex.EModelValidation, lambda: test_model.run_model(None))  # noqa

    def test_memoryview_type_inside_model(self):

        results = []

        def use_memoryview_type():
            results.append(isinstance(memoryview(b"abc"), memoryview))
            results.append(isinstance(b"abc", bytes | bytearray | memoryview))

        test_model_class = create_model(use_memoryview_type)
        test_model = test_model_class()
        test_model.run_model(None)  # noqa

        self.assertEqual([True, True], results)

    def test_exec_outside_model_uses_caller_locals(self):

        class StorageError(Exception):
            pass

        def raise_storage_error():
            error = StorageError("not found")
            exec("raise error from None")

        self.assertRaises(StorageError, raise_storage_error)

    def test_eval_outside_model_uses_caller_locals(self):

        a, b = 2, 3

        self.assertEqual(5, eval("a + b"))

    def test_exec_outside_model_locals_are_a_snapshot(self):

        x = 1
        exec("x = 2")

        self.assertEqual(1, x)

    def test_exec_outside_model_module_level(self):

        module_globals = {"__name__": "guard_rails_test_module"}
        exec("exec('y = 1')\nz = y + 1", module_globals)

        self.assertEqual(1, module_globals["y"])
        self.assertEqual(2, module_globals["z"])

    def test_exec_outside_model_explicit_namespace(self):

        error = ValueError("caller")
        namespace = {"error": KeyError("explicit")}

        self.assertRaises(KeyError, lambda: exec("raise error", namespace))
        self.assertEqual(4, eval("n * 2", {}, {"n": 2}))
        self.assertIsInstance(error, ValueError)

    def test_exec_outside_model_globals_none(self):

        c = 7
        result = {}

        exec("result['c'] = c", None, {"result": result, "c": c})
        self.assertEqual(7, result["c"])
        self.assertEqual(7, eval("c", None))

    def test_eval_from_model(self):

        test_model_class = create_model(lambda: eval("1 + 1"))
        test_model = test_model_class()

        self.assertRaises(ex.EModelValidation, lambda: test_model.run_model(None))  # noqa

    def test_exec_with_namespace_from_model(self):

        test_model_class = create_model(lambda: exec("x = 1", {}, {}))
        test_model = test_model_class()

        self.assertRaises(ex.EModelValidation, lambda: test_model.run_model(None))  # noqa
