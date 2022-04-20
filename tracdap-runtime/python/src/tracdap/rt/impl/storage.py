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

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import pyarrow.feather as pa_ft
import pyarrow.parquet as pa_pq

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.util as _util


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
            storage_options: tp.Dict[str, tp.Any]) \
            -> pa.Table:
        pass

    @abc.abstractmethod
    def write_table(
            self,
            storage_path: str, storage_format: str,
            table: pa.Table,
            storage_options: tp.Dict[str, tp.Any],
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


class FormatManager:

    __formats: tp.Dict[str, IDataFormat.__class__] = dict()
    __extensions: tp.Dict[str, IDataFormat.__class__] = dict()

    __extension_to_format: tp.Dict[str, str] = dict()
    __format_to_extension: tp.Dict[str, str] = dict()

    @classmethod
    def register_data_format(
            cls, format_code: str,
            format_impl: IDataFormat.__class__):

        cls.__formats[format_code.lower()] = format_impl

        for extension, impl in cls.__extensions.items():
            if impl == format_impl:
                cls.__extension_to_format[extension] = format_code
                cls.__format_to_extension[format_code] = extension

    @classmethod
    def register_extension(
            cls, extension: str,
            format_impl: IDataFormat.__class__):

        if extension.startswith("."):
            extension = extension[1:]

        cls.__extensions[extension.lower()] = format_impl

        for format_code, impl in cls.__formats.items():
            if impl == format_impl:
                cls.__extension_to_format[extension] = format_code
                cls.__format_to_extension[format_code] = extension

    @classmethod
    def get_data_format(cls, format_code: str) -> IDataFormat:

        format_impl = cls.__formats.get(format_code.lower())

        if format_impl is None:
            raise _ex.EStorageFormat(f"Unsupported storage format [{format_code}]")

        return format_impl.__call__()

    @classmethod
    def extension_for_format(cls, format_code: str) -> str:

        extension = cls.__format_to_extension.get(format_code.lower())

        if extension is None:
            raise _ex.EStorageFormat(f"Unsupported storage format [{format_code}]")

        return extension

    @classmethod
    def format_for_extension(cls, extension: str) -> str:

        if extension.startswith("."):
            extension = extension[1:]

        format_code = cls.__extension_to_format[extension]

        if format_code is None:
            raise _ex.EStorageFormat(f"No storage format is registered for file extension [{extension}]")

        return extension


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

    def __init__(self, sys_config: _cfg.RuntimeConfig, sys_config_dir: tp.Union[str, pathlib.Path]):

        self.__log = _util.logger_for_object(self)
        self.__file_storage: tp.Dict[str, IFileStorage] = dict()
        self.__data_storage: tp.Dict[str, IDataStorage] = dict()
        self.__settings = sys_config.storageSettings

        storage_options = {"sys_config_dir": sys_config_dir}

        for storage_key, storage_config in sys_config.storage.items():
            self.create_storage(storage_key, storage_config, storage_options)

    def default_storage_key(self):
        return self.__settings.defaultStorage

    def default_storage_format(self):
        return self.__settings.defaultFormat

    def create_storage(self, storage_key: str, storage_config: _cfg.StorageConfig, storage_options: dict = None):

        if storage_config is None:
            err = f"Missing config for storage key [{storage_key}]"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        storage_instance = storage_config.instances[0]  # Just use the first storage instance
        storage_type = storage_instance.storageType

        file_impl = self.__file_impls.get(storage_type)
        data_impl = self.__data_impls.get(storage_type)

        if file_impl is None or data_impl is None:
            err = f"Storage type [{storage_type}] is not available"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        file_storage = file_impl(storage_instance, storage_options)
        data_storage = data_impl(storage_instance, file_storage)

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


class CommonDataStorage(IDataStorage):

    def __init__(
            self, config: _cfg.StorageConfig, file_storage: IFileStorage,
            pushdown_pandas: bool = False, pushdown_spark: bool = False):

        # TODO: How to handle root path in a generic way, re-using logic from underlying file storage
        self.__root_path = file_storage._get_root()  # noqa

        self.__file_storage = file_storage
        self.__pushdown_pandas = pushdown_pandas
        self.__pushdown_spark = pushdown_spark

    def read_table(
            self, storage_path: str, storage_format: str,
            schema: tp.Optional[pa.Schema],
            storage_options: tp.Dict[str, tp.Any]) \
            -> pa.Table:

        format_impl = FormatManager.get_data_format(storage_format)

        stat = self.__file_storage.stat(storage_path)

        if stat.file_type == FileType.DIRECTORY:

            dir_content = self.__file_storage.ls(storage_path)

            if len(dir_content) == 1:
                storage_path = dir_content[0]
            else:
                raise NotImplementedError("Directory storage format not available yet")

        with self.__file_storage.read_byte_stream(storage_path) as byte_stream:
            return format_impl.read_table(byte_stream, schema)

    def write_table(
            self, storage_path: str, storage_format: str,
            table: pa.Table,
            storage_options: tp.Dict[str, tp.Any],
            overwrite: bool = False):

        format_impl = FormatManager.get_data_format(storage_format)
        format_extension = FormatManager.extension_for_format(storage_format)

        # TODO: Full handling of directory storage formats

        if not storage_path.endswith(format_extension):
            storage_path_ = storage_path.rstrip("/\\") + f"/chunk-0.{format_extension}"
            self.__file_storage.mkdir(storage_path, True, False)
        else:
            storage_path_ = storage_path

        with self.__file_storage.write_byte_stream(storage_path_, overwrite=overwrite) as byte_stream:
            format_impl.write_table(byte_stream, table)

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
# DATA FORMATS
# ----------------------------------------------------------------------------------------------------------------------


class _CsvStorageFormat(IDataFormat):

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        # For CSV data, if there is no schema then type inference will do unpredictable things!

        if schema is None or len(schema.names) == 0 or len(schema.types) == 0:
            raise _ex.EStorageFormat("An explicit schema is required to load CSV data")

        read_options = pa_csv.ReadOptions()
        read_options.encoding = 'utf-8'
        read_options.use_threads = False

        parse_options = pa_csv.ParseOptions()
        parse_options.newlines_in_values = True

        convert_options = pa_csv.ConvertOptions()
        convert_options.strings_can_be_null = True

        if schema is not None:
            convert_options.include_columns = schema.names
            convert_options.column_types = {n: t for (n, t) in zip(schema.names, schema.types)}

        return pa_csv.read_csv(source, read_options, parse_options, convert_options)

    def write_table(self, target: tp.Union[str, pathlib.Path, tp.BinaryIO], table: pa.Table):

        write_options = pa_csv.WriteOptions()
        write_options.include_header = True

        if any(map(pa.types.is_decimal, table.schema.types)):
            table = self._format_decimals(table)

        pa_csv.write_csv(table, target, write_options)

    @staticmethod
    def _format_decimals(table: pa.Table):

        # PyArrow does not support output of decimals as text, the cast (format) function is not implemented
        # So, explicitly format any decimal fields as strings before trying to save them

        for column_index in range(len(table.schema.names)):

            column_type: pa.Decimal128Type = table.schema.types[column_index]

            if not pa.types.is_decimal(column_type):
                continue

            # Format string using the scale of the column as the maximum d.p.
            column_format = f".{column_type.scale}g"

            raw_column: pa.Array = table.column(column_index)
            str_column = pa.array(map(lambda d: format(d.as_py(), column_format), raw_column), pa.utf8())
            str_field = pa.field(table.schema.names[column_index], pa.utf8())

            table = table \
                .remove_column(column_index) \
                .add_column(column_index, str_field, str_column)

        return table


class _JsonStorageFormat(IDataFormat):

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        return pa_json.read_json(source)

    def write_table(self, target: tp.BinaryIO, table: pa.Table):

        raise _ex.EStorageFormat("Output to JSON format is not currently supported")


class _ArrowFileFormat(IDataFormat):

    def read_table(self, source: tp.Union[str, pathlib.Path, tp.BinaryIO], schema: tp.Optional[pa.Schema]) -> pa.Table:

        return pa_ft.read_table(source)

    def write_table(self, target: tp.Union[str, pathlib.Path, tp.BinaryIO], table: pa.Table):

        # Compression support in Java is limited
        # For now, let's get Arrow format working without compression or dictionaries

        pa_ft.write_feather(table, target, compression="uncompressed")


class _ParquetStorageFormat(IDataFormat):

    def read_table(self, source: tp.Union[str, pathlib.Path, tp.BinaryIO], schema: tp.Optional[pa.Schema]) -> pa.Table:

        return pa_pq.read_table(source)

    def write_table(self, target: tp.Union[str, pathlib.Path, tp.BinaryIO], table: pa.Table):

        pa_pq.write_table(table, target)


FormatManager.register_data_format("CSV", _CsvStorageFormat)
FormatManager.register_data_format("text/csv", _CsvStorageFormat)
FormatManager.register_extension(".csv", _CsvStorageFormat)

FormatManager.register_data_format("JSON", _JsonStorageFormat)
FormatManager.register_data_format("text/json", _JsonStorageFormat)
FormatManager.register_extension(".json", _JsonStorageFormat)

FormatManager.register_data_format("ARROW_FILE", _ArrowFileFormat)
FormatManager.register_data_format("application/vnd.apache.arrow.file", _ArrowFileFormat)
FormatManager.register_data_format("application/x-apache-arrow-file", _ArrowFileFormat)
FormatManager.register_extension(".arrow", _ArrowFileFormat)

# Mime type for Parquet is not registered yet! But there is an issue open to register one:
# https://issues.apache.org/jira/browse/PARQUET-1889
FormatManager.register_data_format("PARQUET", _ParquetStorageFormat)
FormatManager.register_data_format("application/vnd.apache.parquet", _ParquetStorageFormat)
FormatManager.register_extension(".parquet", _ParquetStorageFormat)


# ----------------------------------------------------------------------------------------------------------------------
# LOCAL STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class LocalFileStorage(IFileStorage):

    def __init__(self, config: _cfg.StorageInstance, options: dict = None):

        self._log = _util.logger_for_object(self)
        self._options = options or {}

        root_path_config = config.storageProps.get("rootPath")  # TODO: Config / constants

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


StorageManager.register_storage_type("LOCAL", LocalFileStorage, LocalDataStorage)
