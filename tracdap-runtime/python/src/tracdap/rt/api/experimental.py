#  Copyright 2024 Accenture Global Solutions Limited
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
import dataclasses as _dc
import datetime as _dt
import enum as _enum
import typing as _tp

from tracdap.rt.api import *
from .hook import _StaticApiHook


_PROTOCOL = _tp.TypeVar('_PROTOCOL')
DATA_API = _tp.TypeVar('DATA_API')


@_dc.dataclass(frozen=True)
class _Protocol(_tp.Generic[_PROTOCOL]):

    protocol_name: str
    api_type: _tp.Type[_PROTOCOL]

    def __str__(self):
        return self.protocol_name


class DataFramework(_Protocol[DATA_API]):

    def __init__(self, protocol_name: str, api_type: _tp.Type[DATA_API]):
        super().__init__(protocol_name, api_type)

    PANDAS: "DataFramework[DATA_API]"
    POLARS: "DataFramework[DATA_API]"


if _tp.TYPE_CHECKING:

    if pandas:
        DataFramework.PANDAS = DataFramework('pandas', pandas.DataFrame)
        """The original Python dataframe library, most widely used"""
    else:
        DataFramework.PANDAS = DataFramework('pandas', None)
        """Pandas data framework is not installed"""

    if polars:
        DataFramework.POLARS = DataFramework('polars', polars.DataFrame)
        """A modern, fast and simple alternative to Pandas"""
    else:
        DataFramework.POLARS = DataFramework('polars', None)
        """Polars data framework is not installed"""

else:

    DataFramework.PANDAS = DataFramework('pandas', None)
    DataFramework.POLARS = DataFramework('polars', None)


PANDAS = DataFramework.PANDAS
POLARS = DataFramework.POLARS


class TracContext(TracContext):

    @_abc.abstractmethod
    def get_table(self, dataset_name: str, framework: DataFramework[DATA_API]) -> DATA_API:

        pass

    @_abc.abstractmethod
    def put_table(self, dataset_name: str, dataset: DATA_API):

        pass


def init_static():
    import tracdap.rt._impl.static_api as _static_impl  # noqa
    _static_impl.StaticApiImpl.register_impl()


def infer_schema(dataset: _tp.Any) -> SchemaDefinition:
    sa = _StaticApiHook.get_instance()
    return sa.infer_schema(dataset)


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


class TracFileStorage:

    @_abc.abstractmethod
    def get_storage_key(self) -> str:
        pass

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



class TracDataContext(TracContext):

    @_abc.abstractmethod
    def get_file_storage(self, storage_key: str) -> TracFileStorage:
        pass

    @_abc.abstractmethod
    def get_data_storage(self, storage_key: str) -> None:
        pass

    @_abc.abstractmethod
    def add_data_import(self, dataset_key: str):
        pass

    @_abc.abstractmethod
    def set_source_metadata(self, dataset_key: str, storage_key: str, source_info: FileStat):
        pass

    @_abc.abstractmethod
    def set_attribute(self, dataset_key: str, attribute_name: str, value: _tp.Any):
        pass

    @_abc.abstractmethod
    def set_schema(self, dataset_key: str, schema: SchemaDefinition):
        pass


class TracDataImport(TracModel):

    def define_inputs(self) -> _tp.Dict[str, ModelInputSchema]:
        return dict()

    @_abc.abstractmethod
    def run_model(self, ctx: TracDataContext):
        pass


class TracDataExport(TracModel):

    def define_outputs(self) -> _tp.Dict[str, ModelOutputSchema]:
        return dict()

    @_abc.abstractmethod
    def run_model(self, ctx: TracDataContext):
        pass
