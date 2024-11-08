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

import abc as _abc
import datetime as _dt
import dataclasses as _dc
import enum as _enum
import typing as _tp

import pyarrow as _pa
import pyarrow.fs as _afs


class FileType(_enum.Enum):

    FILE = 1
    DIRECTORY = 2


@_dc.dataclass
class FileStat:

    """
    Dataclass to represent some basic  file stat info independent of the storage technology used
    I.e. do not depend on Python stat_result class that refers to locally-mounted filesystems
    Timestamps are held in UTC
    """

    file_name: str
    file_type: FileType
    storage_path: str
    size: int

    mtime: _tp.Optional[_dt.datetime] = None
    atime: _tp.Optional[_dt.datetime] = None


class IFileStorage:

    @_abc.abstractmethod
    def exists(self, storage_path: str) -> bool:
        """The exists method can be used for both files and directories"""
        pass

    @_abc.abstractmethod
    def size(self, storage_path: str) -> int:
        """The rm method only works on regular files, it cannot be used for directories"""
        pass

    @_abc.abstractmethod
    def stat(self, storage_path: str) -> FileStat:
        """The stat method can be used for both files and directories, so long as they exist"""
        pass

    @_abc.abstractmethod
    def ls(self, storage_path: str, recursive: bool = False) -> _tp.List[FileStat]:
        """The ls method only works on directories, it cannot be used for regular files"""
        pass

    @_abc.abstractmethod
    def mkdir(self, storage_path: str, recursive: bool = False):
        """The mkdir method will succeed silently if the directory already exists"""
        pass

    @_abc.abstractmethod
    def rm(self, storage_path: str):
        """The rm method only works on regular files, it cannot be used for directories and is not recursive"""
        pass

    @_abc.abstractmethod
    def rmdir(self, storage_path: str):
        """The rmdir method only works on directories and is always recursive"""
        pass

    @_abc.abstractmethod
    def read_byte_stream(self, storage_path: str) -> _tp.ContextManager[_tp.BinaryIO]:
        """The read_byte_stream method only works for existing files"""
        pass

    @_abc.abstractmethod
    def write_byte_stream(self, storage_path: str) -> _tp.ContextManager[_tp.BinaryIO]:
        """The write_byte_stream method will always overwrite an existing file if it exists"""
        pass

    def read_bytes(self, storage_path: str) -> bytes:
        """The read_bytes method only works for existing files"""
        with self.read_byte_stream(storage_path) as stream:
            return stream.read()

    def write_bytes(self, storage_path: str, data: bytes):
        """The write_bytes method will always overwrite an existing file if it exists"""
        with self.write_byte_stream(storage_path) as stream:
            stream.write(data)


class IDataStorage:

    @_abc.abstractmethod
    def read_table(
            self,
            storage_path: str, storage_format: str,
            schema: _tp.Optional[_pa.Schema],
            storage_options: _tp.Dict[str, _tp.Any] = None) \
            -> _pa.Table:
        pass

    @_abc.abstractmethod
    def write_table(
            self,
            storage_path: str, storage_format: str,
            table: _pa.Table,
            storage_options: _tp.Dict[str, _tp.Any] = None,
            overwrite: bool = False):
        pass


class IStorageProvider(_abc.ABC):

    def has_arrow_native(self) -> bool:
        pass

    def has_file_storage(self) -> bool:
        pass

    def has_data_storage(self) -> bool:
        pass

    def get_arrow_native(self) -> _afs.SubTreeFileSystem:
        pass

    def get_file_storage(self) -> IFileStorage:
        pass

    def get_data_storage(self) -> IDataStorage:
        pass


class IDataFormat:

    @_abc.abstractmethod
    def format_code(self) -> str:
        pass

    @_abc.abstractmethod
    def file_extension(self) -> str:
        pass

    @_abc.abstractmethod
    def read_table(self, source: _tp.BinaryIO, schema: _tp.Optional[_pa.Schema]) -> _pa.Table:
        pass

    @_abc.abstractmethod
    def write_table(self, target: _tp.BinaryIO, table: _pa.Table):
        pass
