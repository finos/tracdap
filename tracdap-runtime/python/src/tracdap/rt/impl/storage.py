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
import decimal
import typing as tp
import pathlib
import datetime as dt
import dataclasses as dc
import enum

import codecs
import csv

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.feather as pa_ft
import pyarrow.parquet as pa_pq
import pyarrow.csv as pa_csv

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.data as _data
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
    def get_data_format(cls, format_code: str, format_options: tp.Dict[str, tp.Any]) -> IDataFormat:

        format_impl = cls.__formats.get(format_code.lower())

        if format_impl is None:
            raise _ex.EStorageConfig(f"Unsupported storage format [{format_code}]")

        return format_impl.__call__(format_options)

    @classmethod
    def extension_for_format(cls, format_code: str) -> str:

        extension = cls.__format_to_extension.get(format_code.lower())

        if extension is None:
            raise _ex.EStorageConfig(f"Unsupported storage format [{format_code}]")

        return extension

    @classmethod
    def format_for_extension(cls, extension: str) -> str:

        if extension.startswith("."):
            extension = extension[1:]

        format_code = cls.__extension_to_format[extension]

        if format_code is None:
            raise _ex.EStorageConfig(f"No storage format is registered for file extension [{extension}]")

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

            format_impl = FormatManager.get_data_format(storage_format, storage_options)

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
                return _data.DataConformance.conform_to_schema(table, schema)
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
                self.__file_storage.mkdir(parent_dir_, True, exists_ok=overwrite)  # Allow exists_ok when overwrite == True
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


class _ArrowFileFormat(IDataFormat):

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


class _ParquetStorageFormat(IDataFormat):

    def __init__(self, format_options: tp.Dict[str, tp.Any] = None):
        self._format_options = format_options
        self._log = _util.logger_for_object(self)

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        try:
            columns = schema.names if schema else None
            return pa_pq.read_table(source, columns)

        except pa.ArrowInvalid as e:
            err = f"Parquet file decoding failed, content is garbled"
            self._log.exception(err)
            raise _ex.EDataCorruption(err) from e

    def write_table(self, target: tp.BinaryIO, table: pa.Table):

        pa_pq.write_table(table, target)


class _CsvStorageFormat(IDataFormat):

    __LENIENT_CSV_PARSER = "lenient_csv_parser"

    __TRUE_VALUES = ['true', 't', 'yes' 'y', '1']
    __FALSE_VALUES = ['false', 'f', 'no' 'n', '0']

    def __init__(self, format_options: tp.Dict[str, tp.Any] = None):

        self._log = _util.logger_for_object(self)

        self._format_options = format_options
        self._use_lenient_parser = False

        if format_options:
            if format_options.get(self.__LENIENT_CSV_PARSER) is True:
                self._use_lenient_parser = True

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        # For CSV data, if there is no schema then type inference will do unpredictable things!

        if schema is None or len(schema.names) == 0 or len(schema.types) == 0:
            raise _ex.EDataConformance("An explicit schema is required to load CSV data")

        if self._use_lenient_parser:
            return self._read_table_lenient(source, schema)
        else:
            return self._read_table_arrow(source, schema)

    def write_table(self, target: tp.Union[str, pathlib.Path, tp.BinaryIO], table: pa.Table):

        write_options = pa_csv.WriteOptions()
        write_options.include_header = True

        # Arrow cannot yet apply the required output formatting for all data types
        # For types that require extra formatting, explicitly format them in code and output the string values

        formatted_table = self._format_outputs(table)

        pa_csv.write_csv(formatted_table, target, write_options)

    def _read_table_arrow(self, source: tp.BinaryIO, schema: pa.Schema) -> pa.Table:

        try:

            read_options = pa_csv.ReadOptions()
            read_options.encoding = 'utf-8'
            read_options.use_threads = False

            parse_options = pa_csv.ParseOptions()
            parse_options.newlines_in_values = True

            convert_options = pa_csv.ConvertOptions()
            convert_options.include_columns = schema.names
            convert_options.column_types = {n: t for (n, t) in zip(schema.names, schema.types)}
            convert_options.strings_can_be_null = True
            convert_options.quoted_strings_can_be_null = False

            return pa_csv.read_csv(source, read_options, parse_options, convert_options)

        except pa.ArrowInvalid as e:
            err = f"CSV file decoding failed, content is garbled"
            self._log.exception(err)
            raise _ex.EDataCorruption(err) from e

        except pa.ArrowKeyError as e:
            err = f"CSV file decoding failed, one or more columns is missing"
            self._log.error(err)
            self._log.exception(str(e))
            raise _ex.EDataCorruption(err) from e

    @classmethod
    def _format_outputs(cls, table: pa.Table) -> pa.Table:

        for column_index in range(table.num_columns):

            column: pa.Array = table.column(column_index)
            format_applied = False

            if pa.types.is_floating(column.type):

                # Arrow outputs NaN as null
                # If a float column contains NaN, use our own formatter to distinguish between them

                has_nan = pac.any(pac.and_not(  # noqa
                    column.is_null(nan_is_null=True),
                    column.is_null(nan_is_null=False)))

                if has_nan.as_py():
                    column = cls._format_float(column)
                    format_applied = True

            if pa.types.is_decimal(column.type):
                column = cls._format_decimal(column)
                format_applied = True

            if pa.types.is_timestamp(column.type):
                column = cls._format_timestamp(column)
                format_applied = True

            if format_applied:
                field = pa.field(table.schema.names[column_index], pa.utf8())
                table = table \
                    .remove_column(column_index) \
                    .add_column(column_index, field, column)

        return table

    @classmethod
    def _format_float(cls, column: pa.Array) -> pa.Array:

        def format_float(f: pa.FloatScalar):

            if not f.is_valid:
                return None

            return str(f.as_py())

        return pa.array(map(format_float, column), pa.utf8())

    @classmethod
    def _format_decimal(cls, column: pa.Array) -> pa.Array:

        # PyArrow does not support output of decimals as text, the cast (format) function is not implemented
        # So, explicitly format any decimal fields as strings before trying to save them

        column_type = column.type

        # Ensure the full information from the column is recorded in text form
        # Use the scale of the column as the maximum d.p., then strip away any unneeded trailing chars

        def format_decimal(d: pa.Scalar):

            if not d.is_valid:
                return None

            decimal_format = f".{column_type.scale}f"
            decimal_str = format(d.as_py(), decimal_format)

            return decimal_str.rstrip('0').rstrip('.')

        return pa.array(map(format_decimal, column), pa.utf8())

    @classmethod
    def _format_timestamp(cls, column: pa.Array) -> pa.Array:

        # PyArrow outputs timestamps with a space (' ') separator between date and time
        # ISO format requires a 'T' between date and time

        column_type: pa.TimestampType = column.type

        if column_type.unit == "s":
            timespec = 'seconds'
        elif column_type.unit == "ms":
            timespec = "milliseconds"
        else:
            timespec = "microseconds"

        def format_timestamp(t: pa.Scalar):

            if not t.is_valid:
                return None

            return t.as_py().isoformat(timespec=timespec)

        return pa.array(map(format_timestamp, column), pa.utf8())

    def _read_table_lenient(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        try:

            stream_reader = codecs.getreader('utf-8')
            text_source = stream_reader(source)

            csv_params = {
                "skipinitialspace": True,
                "doublequote": True
            }

            csv_reader = csv.reader(text_source, **csv_params)
            header = next(csv_reader)

            for col in schema.names:
                if col not in header:
                    raise _ex.EDataConformance(f"Missing column {col}")  # TODO

            schema_columns = dict(zip(schema.names, range(len(schema.names))))
            col_mapping = [schema_columns.get(col) for col in header]
            python_types = list(map(_types.arrow_to_python_type, schema.types))

            data = [[] for _ in range(len(schema.names))]

            for row in csv_reader:

                csv_col = 0

                for raw_value in row:

                    output_col = col_mapping[csv_col]

                    if output_col is None:
                        continue

                    python_type = python_types[output_col]
                    python_value = self._convert_python_value(raw_value, python_type)

                    data[output_col].append(python_value)

                    csv_col += 1

            data_dict = dict(zip(schema.names, data))
            table = pa.Table.from_pydict(data_dict, schema)  # noqa

            return table

        except UnicodeDecodeError as e:
            err = f"CSV file decoding failed, content is garbled"
            self._log.exception(err)
            raise _ex.EDataCorruption(err) from e

    @classmethod
    def _convert_python_value(cls, raw_value: tp.Any, python_type: type) -> tp.Any:

        try:

            if raw_value is None:
                return None

            if isinstance(raw_value, python_type):
                return raw_value

            if python_type == bool:
                if isinstance(raw_value, str):
                    if raw_value.lower() in cls.__TRUE_VALUES:
                        return True
                    if raw_value.lower() in cls.__FALSE_VALUES:
                        return False
                if isinstance(raw_value, int) or isinstance(raw_value, float):
                    if raw_value == 1:
                        return True
                    if raw_value == 0:
                        return False

            if python_type == int:
                if isinstance(raw_value, float):
                    return int(raw_value)
                if isinstance(raw_value, str):
                    return int(raw_value)

            if python_type == float:
                if isinstance(raw_value, int):
                    return float(raw_value)
                if isinstance(raw_value, str):
                    return float(raw_value)

            if python_type == decimal.Decimal:
                if isinstance(raw_value, int):
                    return decimal.Decimal.from_float(float(raw_value))
                if isinstance(raw_value, float):
                    return decimal.Decimal.from_float(raw_value)
                if isinstance(raw_value, str):
                    return decimal.Decimal(raw_value)

            if python_type == str:
                return str(raw_value)

            if python_type == dt.date:
                if isinstance(raw_value, str):
                    return dt.date.fromisoformat(raw_value)

            if python_type == dt.datetime:
                if isinstance(raw_value, str):
                    return dt.datetime.fromisoformat(raw_value)

            raise _ex.EDataConformance("No data conversion available for input type")  # TODO

        except Exception as e:

            raise _ex.EDataConformance("Could not convert input data") from e  # TODO


FormatManager.register_data_format("ARROW_FILE", _ArrowFileFormat)
FormatManager.register_data_format("application/vnd.apache.arrow.file", _ArrowFileFormat)
FormatManager.register_data_format("application/x-apache-arrow-file", _ArrowFileFormat)
FormatManager.register_extension(".arrow", _ArrowFileFormat)

# Mime type for Parquet is not registered yet! But there is an issue open to register one:
# https://issues.apache.org/jira/browse/PARQUET-1889
FormatManager.register_data_format("PARQUET", _ParquetStorageFormat)
FormatManager.register_data_format("application/vnd.apache.parquet", _ParquetStorageFormat)
FormatManager.register_extension(".parquet", _ParquetStorageFormat)

FormatManager.register_data_format("CSV", _CsvStorageFormat)
FormatManager.register_data_format("text/csv", _CsvStorageFormat)
FormatManager.register_extension(".csv", _CsvStorageFormat)


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

    def __init__(self, storage_config: _cfg.StorageConfig, file_storage: LocalFileStorage):
        super().__init__(storage_config, file_storage, pushdown_pandas=True, pushdown_spark=True)


StorageManager.register_storage_type("LOCAL", LocalFileStorage, LocalDataStorage)
