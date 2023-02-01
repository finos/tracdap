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
import unittest

import tracdap.rt._impl.shim as shim
import tracdap.rt._impl.util as util
import tracdap.rt.exceptions as _ex

_SHIM_TEST_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../..") \
    .joinpath("test_data/shim_test") \
    .resolve()


class TestShimLoader(unittest.TestCase):

    _shim_loader = shim.ShimLoader()
    _shim = ""

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging(enable_debug=True)
        cls._shim = cls._shim_loader.create_shim(_SHIM_TEST_DIR)

    def setUp(self):
        self._shim_loader.activate_shim(self._shim)

    def tearDown(self):
        self._shim_loader.deactivate_shim()

    def test_absolute_import(self):

        class_ = self._shim_loader.load_class("acme.rockets.abs1", "ImportTest", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_relative_import_1(self):

        class_ = self._shim_loader.load_class("acme.rockets.rel1", "ImportTest", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_relative_import_2(self):

        class_ = self._shim_loader.load_class("acme.rockets.rel2", "ImportTest", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_relative_import_3(self):

        class_ = self._shim_loader.load_class("acme.rockets.rel3", "ImportTest", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_relative_import_4(self):

        class_ = self._shim_loader.load_class("acme.rockets.rel4", "ImportTest", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_package_import_1(self):

        class_ = self._shim_loader.load_class("acme.rockets.pkg1", "ImportTest", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_package_import_2(self):

        class_ = self._shim_loader.load_class("acme.rockets.pkg2", "ImportTest", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "ImportTest")
        self.assertIsInstance(instance_, class_)

    def test_duplicate_import_1(self):

        # A module can exist as both a module and a package in the same source path
        # This is a very bad thing to do and should always be avoided
        # However, it is allowed when using the regular Python loader mechanisms
        # We want to replicate the same behaviour, to avoid unexpected breaks when loading to the platform
        # The Python behaviour is to give precedence to packages, so the shim loader should do the same

        class_ = self._shim_loader.load_class("acme.rockets.dup1", "DupClass", object)
        instance_ = class_()
        self.assertEqual(class_.__name__, "DupClass")
        self.assertIsInstance(instance_, class_)
        self.assertEqual(instance_.dup_source, "package")

    def test_unknown_module(self):

        self.assertRaises(
            _ex.EModelLoad, lambda:
            self._shim_loader.load_class("nonexistent.module", "ImportTest", object))

    def test_unknown_class(self):

        self.assertRaises(
            _ex.EModelLoad, lambda:
            self._shim_loader.load_class("acme.rockets.abs1", "NonexistentClass", object))

    def test_load_wrong_type(self):

        self.assertRaises(
            _ex.EModelLoad, lambda:
            self._shim_loader.load_class("acme.rockets.abs1", "ImportTest", dict))
