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
import pyarrow.parquet as pa_pq

import tracdap.rt.ext.plugins as plugins
import tracdap.rt.exceptions as ex

# Import storage interfaces
from tracdap.rt.ext.storage import IDataFormat

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class ParquetStorageFormat(IDataFormat):

    FORMAT_CODE = "PARQUET"
    FILE_EXTENSION = "parquet"

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
            return pa_pq.read_table(source, columns=columns)

        except pa.ArrowInvalid as e:
            err = f"Parquet file decoding failed, content is garbled"
            self._log.exception(err)
            raise ex.EDataCorruption(err) from e

    def write_table(self, target: tp.BinaryIO, table: pa.Table):

        pa_pq.write_table(table, target)


# Mime type for Parquet is not registered yet! But there is an issue open to register one:
# https://issues.apache.org/jira/browse/PARQUET-1889
plugins.PluginManager.register_plugin(
    IDataFormat, ParquetStorageFormat,
    ["PARQUET", ".parquet", "application/vnd.apache.parquet"])
