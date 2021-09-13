#  Copyright 2021 Accenture Global Solutions Limited
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
import typing as tp
import pathlib
import datetime as dt
import dataclasses as dc
import enum

import pandas as pd

import trac.rt.metadata as _meta
import trac.rt.config as _cfg
import trac.rt.exceptions as _ex
import trac.rt.impl.util as _util


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
    def read_pandas_table(
            self, schema: _meta.TableSchema,
            storage_path: str, storage_format: str,
            storage_options: tp.Dict[str, tp.Any]) \
            -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def write_pandas_table(
            self, schema: _meta.TableSchema, df: pd.DataFrame,
            storage_path: str, storage_format: str,
            storage_options: tp.Dict[str, tp.Any],
            overwrite: bool = False):
        pass

    @abc.abstractmethod
    def query_table(self):
        pass


class StorageManager:

    __file_impls: tp.Dict[str, IFileStorage.__class__] = dict()
    __data_impls: tp.Dict[str, IDataStorage.__class__] = dict()

    @classmethod
    def register_storage_type(
            cls, storage_type: str,
            file_impl: IFileStorage.__class__,
            data_impl: IDataStorage.__class__):

        cls.__file_impls[storage_type] = file_impl
        cls.__data_impls[storage_type] = data_impl

    def __init__(self, sys_config: _cfg.SystemConfig, sys_config_dir: tp.Union[str, pathlib.Path]):

        self.__log = _util.logger_for_object(self)
        self.__file_storage: tp.Dict[str, IFileStorage] = dict()
        self.__data_storage: tp.Dict[str, IDataStorage] = dict()

        storage_options = {"sys_config_dir": sys_config_dir}

        for storage_key, storage_config in sys_config.storage.items():
            self.create_storage(storage_key, storage_config, storage_options)

    def create_storage(self, storage_key: str, storage_config: _cfg.StorageConfig, storage_options: dict = None):

        if storage_config is None:
            err = f"Missing config for storage key [{storage_key}]"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        storage_type = storage_config.storageType

        file_impl = self.__file_impls.get(storage_type)
        data_impl = self.__data_impls.get(storage_type)

        if file_impl is None or data_impl is None:
            err = f"Storage type [{storage_type}] is not available"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        file_storage = file_impl(storage_config, storage_options)
        data_storage = data_impl(storage_config, file_storage)

        self.__file_storage[storage_key] = file_storage
        self.__data_storage[storage_key] = data_storage

    def has_file_storage(self, storage_key: str) -> bool:

        return storage_key in self.__file_storage

    def get_file_storage(self, storage_key: str) -> IFileStorage:

        if not self.has_file_storage(storage_key):
            err = f"File storage is not configured for storage key [{storage_key}]"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        return self.__file_storage[storage_key]

    def has_data_storage(self, storage_key: str) -> bool:

        return storage_key in self.__data_storage

    def get_data_storage(self, storage_key: str) -> IDataStorage:

        if not self.has_data_storage(storage_key):
            err = f"Data storage is not configured for storage key [{storage_key}]"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        return self.__data_storage[storage_key]


# ----------------------------------------------------------------------------------------------------------------------
# COMMON STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class _StorageFormat:

    @abc.abstractmethod
    def read_pandas(self, src, schema: _meta.TableSchema, options: dict) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def write_pandas(self, tgt, schema: _meta.TableSchema, data: pd.DataFrame, options: dict):
        pass


class _CsvStorageFormat(_StorageFormat):

    def read_pandas(self, src, schema: _meta.TableSchema, options: dict):

        columns = list(map(lambda f: f.fieldName, schema.fields)) if schema.fields else None

        return pd.read_csv(src, usecols=columns, **options)

    def write_pandas(self, tgt, schema: _meta.TableSchema, data: pd.DataFrame, options: dict):

        columns = list(map(lambda f: f.fieldName, schema.fields)) if schema.fields else None

        data.to_csv(tgt, columns=columns, **options)


class CommonDataStorage(IDataStorage):

    __formats = {
        'csv': _CsvStorageFormat()
    }

    def __init__(
            self, config: _cfg.StorageConfig, file_storage: IFileStorage,
            pushdown_pandas: bool = False, pushdown_spark: bool = False):

        # TODO: How to handle root path in a generic way, re-using logic from underlying file storage
        self.__root_path = file_storage._get_root()  # noqa

        self.__file_storage = file_storage
        self.__pushdown_pandas = pushdown_pandas
        self.__pushdown_spark = pushdown_spark

    def read_pandas_table(
            self, schema: _meta.TableSchema,
            storage_path: str, storage_format: str,
            storage_options: tp.Dict[str, tp.Any]) \
            -> pd.DataFrame:

        format_impl = self.__formats.get(storage_format.lower())

        if format_impl is None:
            raise _ex.EStorageConfig(f"Requested storage format [{storage_format}] is not available")

        if self.__pushdown_pandas:
            full_path = self.__root_path / storage_path
            return format_impl.read_pandas(full_path, schema, storage_options)

        else:
            with self.__file_storage.read_text_stream(storage_path) as text_stream:
                return format_impl.read_pandas(text_stream, schema, storage_options)

    def write_pandas_table(
            self, schema: _meta.TableSchema, df: pd.DataFrame,
            storage_path: str, storage_format: str,
            storage_options: tp.Dict[str, tp.Any],
            overwrite: bool = False):

        format_impl = self.__formats.get(storage_format.lower())

        if format_impl is None:
            raise _ex.EStorageConfig(f"Requested storage format [{storage_format}] is not available")

        # TODO: Switch between binary and text mode depending on format, also set encoding (default is utf-8)

        if self.__pushdown_pandas:

            full_path = self.__root_path / storage_path
            file_mode = 'wt' if overwrite else 'xt'
            pushdown_options = {**storage_options, 'mode': file_mode}

            return format_impl.write_pandas(full_path, schema, df, pushdown_options)

        else:

            with self.__file_storage.write_text_stream(storage_path, overwrite=overwrite) as text_stream:
                format_impl.write_pandas(text_stream, schema, df, storage_options)

    def read_spark_table(
            self, schema: _meta.TableSchema,
            storage_path: str, storage_format: str,
            storage_options: tp.Dict[str, tp.Any]) \
            -> object:

        pass

    def write_spark_table(self):
        pass

    def query_table(self):
        pass


# ----------------------------------------------------------------------------------------------------------------------
# LOCAL STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class LocalFileStorage(IFileStorage):

    def __init__(self, config: _cfg.StorageConfig, options: dict = None):

        self._log = _util.logger_for_object(self)
        self._options = options or {}

        root_path_config = config.storageConfig.get("rootPath")  # TODO: Config / constants

        if not root_path_config or root_path_config.isspace():
            err = f"Storage root path not set"
            self._log.error(err)
            raise _ex.EStorageRequest(err)

        supplied_root = pathlib.Path(root_path_config)

        if supplied_root.is_absolute():
            absolute_root = supplied_root

        elif "sys_config_dir" in self._options:
            absolute_root = pathlib.Path(self._options["sys_config_dir"]).joinpath(supplied_root).absolute()

        else:
            err = f"Could not resolve relative path for storage root [{supplied_root}]"
            self._log.error(err)
            raise _ex.EStorageConfig(err)

        try:
            self.__root_path = absolute_root.resolve(strict=True)

        except FileNotFoundError as e:
            err = f"Storage root path does not exist: [{absolute_root}]"
            self._log.error(err)
            raise _ex.EStorageRequest(err) from e

    def _get_root(self):
        return self.__root_path

    def exists(self, storage_path: str) -> bool:

        item_path = self.__root_path / storage_path
        return item_path.exists()

    def size(self, storage_path: str) -> int:

        return self.stat(storage_path).size

    def stat(self, storage_path: str) -> FileStat:

        item_path = self.__root_path / storage_path
        os_stat = item_path.stat()

        file_type = FileType.FILE if item_path.is_file() \
            else FileType.DIRECTORY if item_path.is_dir() \
            else None

        return FileStat(
            file_type=file_type,
            size=os_stat.st_size,
            ctime=dt.datetime.fromtimestamp(os_stat.st_ctime, dt.timezone.utc),
            mtime=dt.datetime.fromtimestamp(os_stat.st_mtime, dt.timezone.utc),
            atime=dt.datetime.fromtimestamp(os_stat.st_atime, dt.timezone.utc),
            uid=os_stat.st_uid,
            gid=os_stat.st_gid,
            mode=os_stat.st_mode)

    def ls(self, storage_path: str) -> tp.List[str]:

        item_path = self.__root_path / storage_path
        return [str(x.relative_to(self.__root_path))
                for x in item_path.iterdir()
                if x.is_file() or x.is_dir()]

    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        item_path = self.__root_path / storage_path
        item_path.mkdir(parents=recursive, exist_ok=exists_ok)

    def rm(self, storage_path: str, recursive: bool = False):

        raise NotImplementedError()

    def read_bytes(self, storage_path: str) -> bytes:

        with self.read_byte_stream(storage_path) as stream:
            return stream.read()

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        item_path = self.__root_path / storage_path

        return open(item_path, mode='rb')

    def write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):

        with self.write_byte_stream(storage_path, overwrite) as stream:
            stream.write(data)

    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:

        item_path = self.__root_path / storage_path

        if overwrite:
            return open(item_path, mode='wb')
        else:
            return open(item_path, mode='xb')

    def read_text(self, storage_path: str, encoding: str = 'utf-8') -> str:

        with self.read_text_stream(storage_path, encoding) as stream:
            return stream.read()

    def read_text_stream(self, storage_path: str, encoding: str = 'utf-8') -> tp.TextIO:

        item_path = self.__root_path / storage_path

        return open(item_path, mode='rt')

    def write_text(self, storage_path: str, data: str, encoding: str = 'utf-8', overwrite: bool = False):

        with self.write_text_stream(storage_path, encoding, overwrite) as stream:
            stream.write(data)

    def write_text_stream(self, storage_path: str, encoding: str = 'utf-8', overwrite: bool = False) -> tp.TextIO:

        item_path = self.__root_path / storage_path

        if overwrite:
            return open(item_path, mode='wt')
        else:
            return open(item_path, mode='xt')


class LocalDataStorage(CommonDataStorage):

    def __init__(self, storage_config: _cfg.StorageConfig, file_storage: LocalFileStorage):
        super().__init__(storage_config, file_storage, pushdown_pandas=True, pushdown_spark=True)


StorageManager.register_storage_type("LOCAL_STORAGE", LocalFileStorage, LocalDataStorage)
