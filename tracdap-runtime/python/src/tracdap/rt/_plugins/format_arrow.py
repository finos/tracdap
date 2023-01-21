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


import typing as tp

import pyarrow as pa
import pyarrow.feather as pa_ft

import tracdap.rt.ext.plugins as plugins
import tracdap.rt.exceptions as ex

# Import storage interfaces
from tracdap.rt.ext.storage import IDataFormat

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class ArrowFileFormat(IDataFormat):

    FORMAT_CODE = "ARROW_FILE"
    FILE_EXTENSION = "arrow"

    def __init__(self, format_options: tp.Dict[str, tp.Any] = None):
        self._format_options = format_options
        self._log = _helpers.logger_for_object(self)

    def format_code(self) -> str:
        return self.FORMAT_CODE

    def file_extension(self) -> str:
        return self.FILE_EXTENSION

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        try:
            columns = schema.names if schema else None
            return pa_ft.read_table(source, columns)

        except pa.ArrowInvalid as e:
            err = f"Arrow file decoding failed, content is garbled"
            self._log.exception(err)
            raise ex.EDataCorruption(err) from e

    def write_table(self, target: tp.BinaryIO, table: pa.Table):

        # Compression support in Java is limited
        # For now, let's get Arrow format working without compression or dictionaries

        pa_ft.write_feather(table, target, compression="uncompressed")  # noqa


plugins.PluginManager.register_plugin(
    IDataFormat, ArrowFileFormat,
    ["ARROW_FILE", ".arrow", "application/vnd.apache.arrow.file", "application/x-apache-arrow-file"])
