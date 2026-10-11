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
import hashlib
import itertools
import pathlib
import shutil
import typing as tp
import uuid

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.file_formats as _file_formats
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.util as _util


@dc.dataclass(frozen=True)
class PreparedPlacement:

    placement_name: str
    data_id: _meta.TagHeader
    location: _meta.ExternalLocation
    file_format: _file_formats.ExternalFileFormat
    encoded_file: pathlib.Path
    size: int
    content_hash: str


class PlacementWriter:

    """Place TRAC datasets in external storage as data files, recording exactly what was written"""

    __COPY_CHUNK_SIZE = 1024 * 1024

    def __init__(self, storage: _storage.StorageManager, scratch_dir: pathlib.Path):
        self.__storage = storage
        self.__scratch_dir = scratch_dir.joinpath("placements")
        self.__log = _logging.logger_for_object(self)

    def prepare(
            self, placement_name: str, data_view: _data.DataView, data_id: _meta.TagHeader,
            location: _meta.ExternalLocation, file_format: _file_formats.ExternalFileFormat,
            conflict: _meta.PlacementConflict) \
            -> PreparedPlacement:

        file_storage = self._file_storage(placement_name, location.storageKey)

        if conflict == _meta.PlacementConflict.PLACEMENT_FAIL and file_storage.exists(location.storagePath):
            raise _ex.EStorageRequest(
                f"Placement [{placement_name}] failed: File already exists" +
                f" [{location.storagePath}] in [{location.storageKey}]")

        self.__scratch_dir.mkdir(parents=True, exist_ok=True)
        encoded_file = self.__scratch_dir.joinpath(f"{placement_name}-{uuid.uuid4().hex}")

        try:

            table = _data.DataMapping.view_to_arrow(data_view, _data.DataPartKey.for_root())
            codec = _storage.FormatManager.get_data_format(file_format.format_code, format_options={})

            with open(encoded_file, "wb") as stream:
                codec.write_table(stream, table)

        except _ex.ETrac as e:
            encoded_file.unlink(missing_ok=True)
            raise type(e)(f"Placement [{placement_name}] failed: {e}") from e

        except Exception as e:
            encoded_file.unlink(missing_ok=True)
            raise _ex.ETracInternal(f"Placement [{placement_name}] failed: Unexpected error encoding the file") from e

        size, content_hash = self._size_and_hash(encoded_file)

        return PreparedPlacement(
            placement_name, data_id, location, file_format,
            encoded_file, size, content_hash)

    def place(self, prepared: PreparedPlacement, conflict: _meta.PlacementConflict) -> _meta.PlacementRecord:

        location = prepared.location
        file_storage = self._file_storage(prepared.placement_name, location.storageKey)

        try:

            target_path = self._target_path(prepared.placement_name, file_storage, location, conflict)

            with open(prepared.encoded_file, "rb") as source, file_storage.write_byte_stream(target_path) as target:
                shutil.copyfileobj(source, target, self.__COPY_CHUNK_SIZE)

        finally:
            prepared.encoded_file.unlink(missing_ok=True)

        self.__log.info(
            f"Placed [{prepared.placement_name}] in [{location.storageKey}] at [{target_path}]," +
            f" size = [{prepared.size}], sha256 = [{prepared.content_hash}]")

        return _meta.PlacementRecord(
            dataId=_util.selector_for(prepared.data_id),
            location=dc.replace(location, storagePath=target_path),
            format=prepared.file_format.format_code,
            mimeType=prepared.file_format.mime_type,
            size=prepared.size,
            contentHash=prepared.content_hash)

    def _file_storage(self, placement_name: str, storage_key: str) -> _storage.IFileStorage:

        if not self.__storage.has_file_storage(storage_key, external=True):
            raise _ex.EStorageConfig(
                f"Placement [{placement_name}] failed: [{storage_key}] is not a configured external storage location")

        return self.__storage.get_file_storage(storage_key, external=True)

    @staticmethod
    def _target_path(
            placement_name: str, file_storage: _storage.IFileStorage,
            location: _meta.ExternalLocation, conflict: _meta.PlacementConflict) \
            -> str:

        storage_path = location.storagePath

        if conflict == _meta.PlacementConflict.PLACEMENT_OVERWRITE or not file_storage.exists(storage_path):
            return storage_path

        if conflict == _meta.PlacementConflict.PLACEMENT_FAIL:
            raise _ex.EStorageRequest(
                f"Placement [{placement_name}] failed: File already exists" +
                f" [{storage_path}] in [{location.storageKey}]")

        path = pathlib.PurePosixPath(storage_path)

        for suffix in itertools.count(2):
            candidate = str(path.with_name(f"{path.stem}-{suffix}{path.suffix}"))
            if not file_storage.exists(candidate):
                return candidate

    @classmethod
    def _size_and_hash(cls, encoded_file: pathlib.Path) -> tp.Tuple[int, str]:

        digest = hashlib.sha256()
        size = 0

        with open(encoded_file, "rb") as stream:
            for chunk in iter(lambda: stream.read(cls.__COPY_CHUNK_SIZE), b""):
                digest.update(chunk)
                size += len(chunk)

        return size, digest.hexdigest()
