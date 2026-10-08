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

import dataclasses as dc
import pathlib
import typing as tp

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.storage as _storage


@dc.dataclass(frozen=True)
class ExternalFileFormat:

    format_code: str
    mime_type: str


class ExternalFileFormats:

    # Kept in step with EXTERNAL_FILE_FORMATS in the platform validation library
    __EXTERNAL_FILE_FORMATS = {
        "CSV": ExternalFileFormat("CSV", "text/csv"),
        "PARQUET": ExternalFileFormat("PARQUET", "application/vnd.apache.parquet"),
        "ARROW_FILE": ExternalFileFormat("ARROW_FILE", "application/vnd.apache.arrow.file")
    }

    @classmethod
    def file_extension(cls, storage_path: str) -> tp.Optional[str]:

        suffix = pathlib.PurePosixPath(storage_path).suffix
        return suffix[1:] if len(suffix) > 1 else None

    @classmethod
    def for_extension(cls, extension: str) -> tp.Optional[ExternalFileFormat]:

        try:
            codec = _storage.FormatManager.get_data_format(f".{extension.lower()}", format_options={})
            return cls.__EXTERNAL_FILE_FORMATS.get(codec.format_code())
        except _ex.EStorageConfig:
            return None

    @classmethod
    def file_type(cls, extension: str, file_format: ExternalFileFormat) -> _meta.FileType:
        return _meta.FileType(extension.lower(), file_format.mime_type)
