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

import tracdap.rt.metadata as meta
import tracdap.rt._impl.core.file_formats as file_formats  # noqa
import tracdap.rt._impl.core.plugins as plugins  # noqa


plugins.PluginManagerImpl.register_core_plugins()


class ExternalFileFormatsTest(unittest.TestCase):

    def test_supported_extensions(self):

        self.assertEqual("CSV", file_formats.ExternalFileFormats.for_extension("csv").format_code)
        self.assertEqual("PARQUET", file_formats.ExternalFileFormats.for_extension("parquet").format_code)
        self.assertEqual("ARROW_FILE", file_formats.ExternalFileFormats.for_extension("arrow").format_code)

    def test_upper_case_extension(self):

        self.assertEqual("CSV", file_formats.ExternalFileFormats.for_extension("CSV").format_code)
        self.assertEqual("CSV", file_formats.ExternalFileFormats.file_extension("LOANS.CSV").upper())

    def test_unsupported_extensions(self):

        self.assertIsNone(file_formats.ExternalFileFormats.file_extension("loans"))
        self.assertIsNone(file_formats.ExternalFileFormats.for_extension("txt"))
        self.assertIsNone(file_formats.ExternalFileFormats.for_extension("json"))

    def test_file_type(self):

        csv_format = file_formats.ExternalFileFormats.for_extension("CSV")
        file_type = file_formats.ExternalFileFormats.file_type("CSV", csv_format)

        self.assertEqual(meta.FileType("csv", "text/csv"), file_type)
