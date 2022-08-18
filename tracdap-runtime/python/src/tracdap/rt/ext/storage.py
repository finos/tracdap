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

import abc
import datetime as dt
import dataclasses as dc
import enum
import typing as tp

import pyarrow as pa


class FileType(enum.Enum):

    FILE = 1
    DIRECTORY = 2


@dc.dataclass
class FileStat:

    """
    Dataclass to represent some basic  file stat info independent of the storage technology used
    I.e. do not depend on Python stat_result class that refers to locally-mounted filesystems
    Timestamps are held in UTC
    """

    file_type: FileType
    size: int

    ctime: tp.Optional[dt.datetime] = None
    mtime: tp.Optional[dt.datetime] = None
    atime: tp.Optional[dt.datetime] = None

    uid: tp.Optional[int] = None
    gid: tp.Optional[int] = None
    mode: tp.Optional[int] = None


class IFileStorage:

    @abc.abstractmethod
    def exists(self, storage_path: str) -> bool:
        pass

    @abc.abstractmethod
    def size(self, storage_path: str) -> int:
        pass

    @abc.abstractmethod
    def stat(self, storage_path: str) -> FileStat:
        pass

    @abc.abstractmethod
    def ls(self, storage_path: str) -> tp.List[str]:
        pass

    @abc.abstractmethod
    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):
        pass

    @abc.abstractmethod
    def rm(self, storage_path: str, recursive: bool = False):
        pass

    @abc.abstractmethod
    def read_bytes(self, storage_path: str) -> bytes:
        pass

    @abc.abstractmethod
    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:
        pass

    @abc.abstractmethod
    def write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):
        pass

    @abc.abstractmethod
    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:
        pass

    @abc.abstractmethod
    def read_text(self, storage_path: str, encoding: str = 'utf-8') -> str:
        pass

    @abc.abstractmethod
    def read_text_stream(self, storage_path: str, encoding: str = 'utf-8') -> tp.TextIO:
        pass

    @abc.abstractmethod
    def write_text(self, storage_path: str, data: str, encoding: str = 'utf-8', overwrite: bool = False):
        pass

    @abc.abstractmethod
    def write_text_stream(self, storage_path: str, encoding: str = 'utf-8', overwrite: bool = False) -> tp.TextIO:
        pass


class IDataStorage:

    @abc.abstractmethod
    def read_table(
            self,
            storage_path: str, storage_format: str,
            schema: tp.Optional[pa.Schema],
            storage_options: tp.Dict[str, tp.Any] = None) \
            -> pa.Table:
        pass

    @abc.abstractmethod
    def write_table(
            self,
            storage_path: str, storage_format: str,
            table: pa.Table,
            storage_options: tp.Dict[str, tp.Any] = None,
            overwrite: bool = False):
        pass

    @abc.abstractmethod
    def query_table(self):
        pass


class IDataFormat:

    @abc.abstractmethod
    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:
        pass

    @abc.abstractmethod
    def write_table(self, target: tp.BinaryIO, table: pa.Table):
        pass
