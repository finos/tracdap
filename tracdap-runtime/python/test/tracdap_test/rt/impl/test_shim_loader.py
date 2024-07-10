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

import pathlib
import shutil
import tempfile
import unittest

import tracdap.rt._impl.shim as shim
import tracdap.rt._impl.util as util
import tracdap.rt._impl.guard_rails as guard
import tracdap.rt.exceptions as _ex

_SHIM_TEST_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../..") \
    .joinpath("test_data/shim_test") \
    .resolve()


_SHIM_TEST_DIR_2 = pathlib.Path(__file__).parent \
    .joinpath("../../../..") \
    .joinpath("test_data/shim_test_2") \
    .resolve()


class TestShimLoader(unittest.TestCase):

    _shim = shim.ShimLoader.create_shim(_SHIM_TEST_DIR)

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging(enable_debug=True)
        guard.PythonGuardRails.protect_dangerous_functions()

    def test_absolute_import(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.abs1", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_resource_import(self):

        with shim.ShimLoader.use_shim(self._shim):

            class_ = shim.ShimLoader.load_class("acme.rockets.abs1", "ImportTest", object)

            module_ = class_.__module__
            package_ = module_[0: module_.rindex(".")]

            resource_ = shim.ShimLoader.load_resource(package_, "resource.txt")

        content = resource_.decode("utf-8")

        self.assertEqual(content, "Hello world!")

    def test_relative_import_1(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.rel1", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_relative_import_2(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.rel2", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_relative_import_3(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.rel3", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_relative_import_4(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.rel4", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_package_import_1(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.pkg1", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_package_import_2(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.pkg2", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_package_relative(self):

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.pkg_rel", "ImportTest", object)

        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_duplicate_import_1(self):

        # A module can exist as both a module and a package in the same source path
        # This is a very bad thing to do and should always be avoided
        # However, it is allowed when using the regular Python loader mechanisms
        # We want to replicate the same behaviour, to avoid unexpected breaks when loading to the platform
        # The Python behaviour is to give precedence to packages, so the shim loader should do the same

        with shim.ShimLoader.use_shim(self._shim):
            class_ = shim.ShimLoader.load_class("acme.rockets.dup1", "DupClass", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "DupClass")
        self.assertIsInstance(instance_, class_)
        self.assertEqual(instance_.dup_source, "package")

    def test_unknown_module(self):

        with shim.ShimLoader.use_shim(self._shim):

            self.assertRaises(
                _ex.EModelLoad, lambda:
                shim.ShimLoader.load_class("nonexistent.module", "ImportTest", object))

    def test_unknown_class(self):

        with shim.ShimLoader.use_shim(self._shim):

            self.assertRaises(
                _ex.EModelLoad, lambda:
                shim.ShimLoader.load_class("acme.rockets.abs1", "NonexistentClass", object))

    def test_load_wrong_type(self):

        with shim.ShimLoader.use_shim(self._shim):

            self.assertRaises(
                _ex.EModelLoad, lambda:
                shim.ShimLoader.load_class("acme.rockets.abs1", "ImportTest", dict))

    def test_multiple_scopes(self):

        scope_1 = shim.ShimLoader.create_shim(_SHIM_TEST_DIR_2.joinpath("scope_1"))
        scope_2 = shim.ShimLoader.create_shim(_SHIM_TEST_DIR_2.joinpath("scope_2"))

        with shim.ShimLoader.use_shim(scope_1):
            model_1_class = shim.ShimLoader.load_class("pkg_1.sub_1.model", "SampleModel", object)

        with shim.ShimLoader.use_shim(scope_2):
            model_2_class = shim.ShimLoader.load_class("pkg_1.sub_1.model", "SampleModel", object)

        model_1 = model_1_class()
        model_2 = model_2_class()

        result_1 = model_1.use_utils()
        result_2 = model_2.use_utils()

        self.assertEqual("scope_1", result_1)
        self.assertEqual("scope_2", result_2)

    def test_long_paths(self):

        with tempfile.TemporaryDirectory() as tmp:

            try:

                # Make scope path longer than Windows MAX_PATH length (260 chars, 259 without a nul terminator)
                # Still no individual path segment is allowed to be longer than 255 chars

                long_dir = "long_" + "A" * 250
                scope_path = pathlib.Path(tmp).joinpath(long_dir).resolve()

                if util.is_windows() and not str(scope_path).startswith("\\\\?\\"):
                    setup_path = pathlib.Path("\\\\?\\" + str(scope_path))
                else:
                    setup_path = scope_path

                shutil.copytree(_SHIM_TEST_DIR, setup_path)

                scope = shim.ShimLoader.create_shim(scope_path)

                with shim.ShimLoader.use_shim(scope):
                    class_ = shim.ShimLoader.load_class("acme.rockets.abs1", "ImportTest", object)

                instance_ = class_()
                self.assertEqual(class_.__name__, "ImportTest")
                self.assertIsInstance(instance_, class_)

                module_ = class_.__module__
                package_ = module_[0: module_.rindex(".")]

                with shim.ShimLoader.use_shim(scope):
                    resource_ = shim.ShimLoader.load_resource(package_, "resource.txt")

                content = resource_.decode("utf-8")

                self.assertEqual(content, "Hello world!")

            finally:
                util.try_clean_dir(pathlib.Path(tmp))
