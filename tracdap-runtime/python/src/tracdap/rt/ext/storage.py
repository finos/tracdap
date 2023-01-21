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

import abc as _abc
import datetime as _dt
import dataclasses as _dc
import enum as _enum
import typing as _tp

import pyarrow as _pa


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

    file_type: FileType
    size: int

    ctime: _tp.Optional[_dt.datetime] = None
    mtime: _tp.Optional[_dt.datetime] = None
    atime: _tp.Optional[_dt.datetime] = None

    uid: _tp.Optional[int] = None
    gid: _tp.Optional[int] = None
    mode: _tp.Optional[int] = None


class IFileStorage:

    @_abc.abstractmethod
    def exists(self, storage_path: str) -> bool:
        pass

    @_abc.abstractmethod
    def size(self, storage_path: str) -> int:
        pass

    @_abc.abstractmethod
    def stat(self, storage_path: str) -> FileStat:
        pass

    @_abc.abstractmethod
    def ls(self, storage_path: str) -> _tp.List[str]:
        pass

    @_abc.abstractmethod
    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):
        pass

    @_abc.abstractmethod
    def rm(self, storage_path: str, recursive: bool = False):
        pass

    @_abc.abstractmethod
    def read_bytes(self, storage_path: str) -> bytes:
        pass

    @_abc.abstractmethod
    def read_byte_stream(self, storage_path: str) -> _tp.BinaryIO:
        pass

    @_abc.abstractmethod
    def write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):
        pass

    @_abc.abstractmethod
    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> _tp.BinaryIO:
        pass

    @_abc.abstractmethod
    def read_text(self, storage_path: str, encoding: str = 'utf-8') -> str:
        pass

    @_abc.abstractmethod
    def read_text_stream(self, storage_path: str, encoding: str = 'utf-8') -> _tp.TextIO:
        pass

    @_abc.abstractmethod
    def write_text(self, storage_path: str, data: str, encoding: str = 'utf-8', overwrite: bool = False):
        pass

    @_abc.abstractmethod
    def write_text_stream(self, storage_path: str, encoding: str = 'utf-8', overwrite: bool = False) -> _tp.TextIO:
        pass


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
