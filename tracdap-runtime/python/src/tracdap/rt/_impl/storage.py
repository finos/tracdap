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

import datetime as dt
import pathlib
import dataclasses as dc
import typing as tp

import pyarrow as pa
import pyarrow.feather as pa_ft
import pyarrow.parquet as pa_pq

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.data as _data
import tracdap.rt._impl.util as _util
import tracdap.rt._impl.csv_codec as csv_codec

# Import storage interfaces
from tracdap.rt.ext.storage import IDataFormat, IDataStorage, IFileStorage, FileType, FileStat


class FormatManager:

    @dc.dataclass
    class _FormatSpec:
        format_class: IDataFormat.__class__
        format_code: str
        format_ext: str

    __formats: tp.Dict[str, _FormatSpec] = dict()
    __extensions: tp.Dict[str, _FormatSpec] = dict()

    @classmethod
    def register_data_format(
            cls, format_impl: IDataFormat.__class__,
            format_code: str, format_ext: str,
            extra_codes: tp.List[str] = None,
            extra_ext: tp.List[str] = None):

        if format_ext.startswith("."):
            format_ext = format_ext[1:]

        spec = cls._FormatSpec(format_impl, format_code, format_ext)

        cls.__formats[format_code.lower()] = spec
        cls.__extensions[format_ext.lower()] = spec

        if extra_codes:
            for code in extra_codes:
                cls.__formats[code.lower()] = spec

        if extra_ext:
            for ext in extra_ext:
                cls.__extensions[ext.lower()] = spec

    @classmethod
    def get_data_format(cls, format_code: str, format_options: tp.Dict[str, tp.Any]) -> IDataFormat:

        spec = cls.__formats.get(format_code.lower())

        if spec is None:
            raise _ex.EStorageConfig(f"Unsupported storage format [{format_code}]")

        return spec.format_class.__call__(format_options)

    @classmethod
    def primary_format_code(cls, format_code: str) -> str:

        spec = cls.__formats.get(format_code.lower())

        if spec is None:
            raise _ex.EStorageConfig(f"Unsupported storage format [{format_code}]")

        return spec.format_code.lower()

    @classmethod
    def extension_for_format(cls, format_code: str) -> str:

        spec = cls.__formats.get(format_code.lower())

        if spec is None:
            raise _ex.EStorageConfig(f"Unsupported storage format [{format_code}]")

        return spec.format_ext

    @classmethod
    def format_for_extension(cls, extension: str) -> str:

        if extension.startswith("."):
            extension = extension[1:]

        spec = cls.__extensions.get(extension)

        if spec is None:
            raise _ex.EStorageConfig(f"No storage format is registered for file extension [{extension}]")

        return spec.format_code


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

    def __init__(self, sys_config: _cfg.RuntimeConfig):

        self.__log = _util.logger_for_object(self)
        self.__file_storage: tp.Dict[str, IFileStorage] = dict()
        self.__data_storage: tp.Dict[str, IDataStorage] = dict()
        self.__settings = sys_config.storage

        storage_options = dict()

        for storage_key, storage_config in sys_config.storage.buckets.items():
            self.create_storage(storage_key, storage_config, storage_options)

    def default_storage_key(self):
        return self.__settings.defaultBucket

    def default_storage_format(self):
        return self.__settings.defaultFormat

    def create_storage(self, storage_key: str, storage_config: _cfg.PluginConfig, storage_options: dict = None):

        if storage_config is None:
            err = f"Missing config for storage key [{storage_key}]"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        storage_type = storage_config.protocol

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


class CommonDataStorage(IDataStorage):

    def __init__(
            self, config: _cfg.PluginConfig, file_storage: IFileStorage,
            pushdown_pandas: bool = False, pushdown_spark: bool = False):

        self.__log = _util.logger_for_object(self)

        self.__config = config
        self.__file_storage = file_storage
        self.__pushdown_pandas = pushdown_pandas
        self.__pushdown_spark = pushdown_spark

    def read_table(
            self, storage_path: str, storage_format: str,
            schema: tp.Optional[pa.Schema],
            storage_options: tp.Dict[str, tp.Any] = None) \
            -> pa.Table:

        try:

            format_code = FormatManager.primary_format_code(storage_format)
            format_options = storage_options.copy() if storage_options else dict()

            for prop_key, prop_value in self.__config.properties.items():
                if prop_key.startswith(f"{format_code}."):
                    format_prop_key = prop_key[len(format_code) + 1:]
                    format_options[format_prop_key] = prop_value

            format_impl = FormatManager.get_data_format(storage_format, format_options)

            stat = self.__file_storage.stat(storage_path)

            if stat.file_type == FileType.DIRECTORY:

                dir_content = self.__file_storage.ls(storage_path)

                if len(dir_content) == 1:
                    storage_path = storage_path.rstrip("/\\") + "/" + dir_content[0]
                else:
                    raise NotImplementedError("Directory storage format not available yet")

            with self.__file_storage.read_byte_stream(storage_path) as byte_stream:
                table = format_impl.read_table(byte_stream, schema)

            if schema is not None:
                # Apply conformance, in case the format was not able to apply it fully on read
                # It is fine to silently ignore extra columns of an input
                return _data.DataConformance.conform_to_schema(table, schema, warn_extra_columns=False)
            else:
                return table

        except (_ex.EStorage, _ex.EData) as e:
            err = f"Failed to read table [{storage_path}]: {str(e)}"
            self.__log.error(err)
            raise type(e)(err) from e

        except Exception as e:
            err = f"Failed to read table [{storage_path}]: An unexpected error occurred"
            self.__log.error(err)
            self.__log.exception(str(e))
            raise _ex.ETracInternal(err) from e

    def write_table(
            self, storage_path: str, storage_format: str,
            table: pa.Table,
            storage_options: tp.Dict[str, tp.Any] = None,
            overwrite: bool = False):

        try:

            format_impl = FormatManager.get_data_format(storage_format, storage_options)
            format_extension = FormatManager.extension_for_format(storage_format)

            # TODO: Full handling of directory storage formats

            if not storage_path.endswith(format_extension):
                parent_dir_ = storage_path
                storage_path_ = storage_path.rstrip("/\\") + f"/chunk-0.{format_extension}"
                self.__file_storage.mkdir(parent_dir_, True, exists_ok=overwrite)
            else:
                parent_dir_ = str(pathlib.PurePath(storage_path).parent)
                storage_path_ = storage_path
                self.__file_storage.mkdir(parent_dir_, True, True)

            with self.__file_storage.write_byte_stream(storage_path_, overwrite=overwrite) as byte_stream:
                format_impl.write_table(byte_stream, table)

        except (_ex.EStorage, _ex.EData) as e:
            err = f"Failed to write table [{storage_path}]: {str(e)}"
            self.__log.error(err)
            raise type(e)(err) from e

        except Exception as e:
            err = f"Failed to write table [{storage_path}]: An unexpected error occurred"
            self.__log.error(err)
            self.__log.exception(str(e))
            raise _ex.ETracInternal(err) from e

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


class ArrowFileFormat(IDataFormat):

    def __init__(self, format_options: tp.Dict[str, tp.Any] = None):
        self._format_options = format_options
        self._log = _util.logger_for_object(self)

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        try:
            columns = schema.names if schema else None
            return pa_ft.read_table(source, columns)

        except pa.ArrowInvalid as e:
            err = f"Arrow file decoding failed, content is garbled"
            self._log.exception(err)
            raise _ex.EDataCorruption(err) from e

    def write_table(self, target: tp.BinaryIO, table: pa.Table):

        # Compression support in Java is limited
        # For now, let's get Arrow format working without compression or dictionaries

        pa_ft.write_feather(table, target, compression="uncompressed")  # noqa


class ParquetStorageFormat(IDataFormat):

    def __init__(self, format_options: tp.Dict[str, tp.Any] = None):
        self._format_options = format_options
        self._log = _util.logger_for_object(self)

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        try:
            columns = schema.names if schema else None
            return pa_pq.read_table(source, columns=columns)

        except pa.ArrowInvalid as e:
            err = f"Parquet file decoding failed, content is garbled"
            self._log.exception(err)
            raise _ex.EDataCorruption(err) from e

    def write_table(self, target: tp.BinaryIO, table: pa.Table):

        pa_pq.write_table(table, target)


FormatManager.register_data_format(
    ArrowFileFormat, "ARROW_FILE", ".arrow",
    extra_codes=[
        "application/vnd.apache.arrow.file",
        "application/x-apache-arrow-file"])

# Mime type for Parquet is not registered yet! But there is an issue open to register one:
# https://issues.apache.org/jira/browse/PARQUET-1889
FormatManager.register_data_format(
    ParquetStorageFormat, "PARQUET", ".parquet",
    extra_codes=["application/vnd.apache.parquet"])

FormatManager.register_data_format(
    csv_codec.CsvStorageFormat, "CSV", ".csv",
    extra_codes=["text/csv"])


# ----------------------------------------------------------------------------------------------------------------------
# LOCAL STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class LocalFileStorage(IFileStorage):

    def __init__(self, config: _cfg.PluginConfig, options: dict = None):

        self._log = _util.logger_for_object(self)
        self._options = options or {}

        root_path_config = config.properties.get("rootPath")  # TODO: Config / constants

        if not root_path_config or root_path_config.isspace():
            err = f"Storage root path not set"
            self._log.error(err)
            raise _ex.EStorageRequest(err)

        supplied_root = pathlib.Path(root_path_config)

        if supplied_root.is_absolute():
            absolute_root = supplied_root

        else:
            err = f"Relative path not allowed for storage root [{supplied_root}]"
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

        operation = f"EXISTS [{storage_path}]"
        return self._error_handling(operation, lambda: self._exists(storage_path))

    def _exists(self, storage_path: str) -> bool:

        item_path = self.__root_path / storage_path
        return item_path.exists()

    def size(self, storage_path: str) -> int:

        operation = f"SIZE [{storage_path}]"
        return self._error_handling(operation, lambda: self._stat(storage_path).size)

    def stat(self, storage_path: str) -> FileStat:

        operation = f"STAT [{storage_path}]"
        return self._error_handling(operation, lambda: self._stat(storage_path))

    def _stat(self, storage_path: str) -> FileStat:

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

        operation = f"LS [{storage_path}]"
        return self._error_handling(operation, lambda: self._ls(storage_path))

    def _ls(self, storage_path: str) -> tp.List[str]:

        item_path = self.__root_path / storage_path
        return [str(x.relative_to(item_path))
                for x in item_path.iterdir()
                if x.is_file() or x.is_dir()]

    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        operation = f"MKDIR [{storage_path}]"
        self._error_handling(operation, lambda: self._mkdir(storage_path, recursive, exists_ok))

    def _mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        item_path = self.__root_path / storage_path
        item_path.mkdir(parents=recursive, exist_ok=exists_ok)

    def rm(self, storage_path: str, recursive: bool = False):

        operation = f"MKDIR [{storage_path}]"
        self._error_handling(operation, lambda: self._rm(storage_path, recursive))

    def _rm(self, storage_path: str, recursive: bool = False):

        raise NotImplementedError()

    def read_bytes(self, storage_path: str) -> bytes:

        operation = f"READ BYTES [{storage_path}]"
        return self._error_handling(operation, lambda: self._read_bytes(storage_path))

    def _read_bytes(self, storage_path: str) -> bytes:

        with self.read_byte_stream(storage_path) as stream:
            return stream.read()

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (READ) [{storage_path}]"
        return self._error_handling(operation, lambda: self._read_byte_stream(storage_path))

    def _read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        operation = f"CLOSE BYTE STREAM (READ) [{storage_path}]"
        item_path = self.__root_path / storage_path
        stream = open(item_path, mode='rb')

        return _util.log_close(stream, self._log, operation)

    def write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):

        operation = f"WRITE BYTES [{storage_path}]"
        self._error_handling(operation, lambda: self._write_bytes(storage_path, data, overwrite))

    def _write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):

        with self.write_byte_stream(storage_path, overwrite) as stream:
            stream.write(data)

    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (WRITE) [{storage_path}]"
        return self._error_handling(operation, lambda: self._write_byte_stream(storage_path, overwrite))

    def _write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:

        operation = f"CLOSE BYTE STREAM (WRITE) [{storage_path}]"
        item_path = self.__root_path / storage_path

        if overwrite:
            stream = open(item_path, mode='wb')
        else:
            stream = open(item_path, mode='xb')

        return _util.log_close(stream, self._log, operation)

    def read_text(self, storage_path: str, encoding: str = 'utf-8') -> str:

        operation = f"READ TEXT [{storage_path}]"
        return self._error_handling(operation, lambda: self._read_text(storage_path, encoding))

    def _read_text(self, storage_path: str, encoding: str = 'utf-8') -> str:

        with self.read_text_stream(storage_path, encoding) as stream:
            return stream.read()

    def read_text_stream(self, storage_path: str, encoding: str = 'utf-8') -> tp.TextIO:

        operation = f"OPEN TEXT STREAM (READ) [{storage_path}]"
        return self._error_handling(operation, lambda: self._read_text_stream(storage_path, encoding))

    def _read_text_stream(self, storage_path: str, encoding: str = 'utf-8') -> tp.TextIO:

        operation = f"CLOSE TEXT STREAM (READ) [{storage_path}]"
        item_path = self.__root_path / storage_path
        stream = open(item_path, mode='rt', encoding=encoding)

        return _util.log_close(stream, self._log, operation)

    def write_text(self, storage_path: str, data: str, encoding: str = 'utf-8', overwrite: bool = False):

        operation = f"WRITE TEXT [{storage_path}]"
        self._error_handling(operation, lambda: self._write_text(storage_path, data, encoding, overwrite))

    def _write_text(self, storage_path: str, data: str, encoding: str = 'utf-8', overwrite: bool = False):

        with self.write_text_stream(storage_path, encoding, overwrite) as stream:
            stream.write(data)

    def write_text_stream(self, storage_path: str, encoding: str = 'utf-8', overwrite: bool = False) -> tp.TextIO:

        operation = f"OPEN TEXT STREAM (WRITE) [{storage_path}]"
        return self._error_handling(operation, lambda: self._write_text_stream(storage_path, encoding, overwrite))

    def _write_text_stream(self, storage_path: str, encoding: str = 'utf-8', overwrite: bool = False) -> tp.TextIO:

        operation = f"CLOSE TEXT STREAM (WRITE) [{storage_path}]"
        item_path = self.__root_path / storage_path

        if overwrite:
            stream = open(item_path, mode='wt', encoding=encoding)
        else:
            stream = open(item_path, mode='xt', encoding=encoding)

        return _util.log_close(stream, self._log, operation)

    __T = tp.TypeVar("__T")

    def _error_handling(self, operation: str, func: tp.Callable[[], __T]) -> __T:

        try:
            self._log.info(operation)
            return func()

        except FileNotFoundError as e:
            msg = "File not found"
            self._log.exception(f"{operation}: {msg}")
            raise _ex.EStorageRequest(msg) from e

        except FileExistsError as e:
            msg = "File already exists"
            self._log.exception(f"{operation}: {msg}")
            raise _ex.EStorageRequest(msg) from e

        except PermissionError as e:
            msg = "Access denied"
            self._log.exception(f"{operation}: {msg}")
            raise _ex.EStorageAccess(msg) from e

        except OSError as e:
            msg = "Filesystem error"
            self._log.exception(f"{operation}: {msg}")
            raise _ex.EStorageAccess(msg) from e


class LocalDataStorage(CommonDataStorage):

    def __init__(self, storage_config: _cfg.PluginConfig, file_storage: LocalFileStorage):
        super().__init__(storage_config, file_storage, pushdown_pandas=True, pushdown_spark=True)


StorageManager.register_storage_type("LOCAL", LocalFileStorage, LocalDataStorage)
