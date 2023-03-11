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
import enum
import pathlib
import re
import typing as tp

import pyarrow as pa
import pyarrow.fs as afs

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.plugins as plugins
import tracdap.rt._impl.data as _data
import tracdap.rt._impl.util as _util

# Import storage interfaces
from tracdap.rt.ext.storage import *


class FormatManager:

    @classmethod
    def get_data_format(cls, format_code: str, format_options: tp.Dict[str, tp.Any]) -> IDataFormat:

        try:
            config = _cfg.PluginConfig(format_code, format_options)
            return plugins.PluginManager.load_plugin(IDataFormat, config)

        except _ex.EPluginNotAvailable as e:
            raise _ex.EStorageConfig(f"Unsupported storage format [{format_code}]") from e

    @classmethod
    def primary_format_code(cls, format_code: str) -> str:

        codec = cls.get_data_format(format_code, format_options={})
        return codec.format_code()


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

        for storage_key, storage_config in sys_config.storage.buckets.items():
            self.create_storage(storage_key, storage_config)

    def default_storage_key(self):
        return self.__settings.defaultBucket

    def default_storage_format(self):
        return self.__settings.defaultFormat

    def create_storage(self, storage_key: str, storage_config: _cfg.PluginConfig):

        if plugins.PluginManager.is_plugin_available(IStorageProvider, storage_config.protocol):
            self._create_storage_from_provider(storage_key, storage_config)
        else:
            self._create_storage_from_impl(storage_key, storage_config)

    def _create_storage_from_provider(self, storage_key: str, storage_config: _cfg.PluginConfig):

        provider = plugins.PluginManager.load_plugin(IStorageProvider, storage_config)

        if provider.has_file_storage():
            file_storage = provider.get_file_storage()
        elif provider.has_arrow_native():
            fs = provider.get_arrow_native()
            file_storage = CommonFileStorage(storage_key, storage_config, fs)
        else:
            file_storage = None

        if provider.has_data_storage():
            data_storage = provider.get_data_storage()
        elif file_storage is not None:
            data_storage = CommonDataStorage(storage_config, file_storage)
        else:
            data_storage = None

        if file_storage is None and data_storage is None:
            err = f"Storage type [{storage_config.protocol}] is not available"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        if file_storage is not None:
            self.__file_storage[storage_key] = file_storage

        if data_storage is not None:
            self.__data_storage[storage_key] = data_storage

    def _create_storage_from_impl(self, storage_key: str, storage_config: _cfg.PluginConfig):

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

        # Unused
        storage_options = dict()

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
# COMMON FILE STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class CommonFileStorage(IFileStorage):

    def __init__(self, storage_key: str, storage_config: _cfg.PluginConfig, fs_impl: afs.SubTreeFileSystem):

        self._log = _util.logger_for_object(self)
        self._key = storage_key
        self._config = storage_config
        self._fs = fs_impl

        fs_type = fs_impl.base_fs.type_name
        fs_root = fs_impl.base_path

        self._log.info(f"INIT [{self._key}]: Common file storage, fs = [{fs_type}], root = [{fs_root}]")

    def exists(self, storage_path: str) -> bool:

        operation = f"EXISTS [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._exists, storage_path)

    def _exists(self, storage_path: str) -> bool:

        resolved_path = self._resolve_path(storage_path, "EXISTS", True)

        file_info: afs.FileInfo = self._fs.get_file_info(resolved_path)
        return file_info.type != afs.FileType.NotFound

    def size(self, storage_path: str) -> int:

        operation = f"SIZE [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._size, storage_path)

    def _size(self, storage_path: str) -> int:

        resolved_path = self._resolve_path(storage_path, "SIZE", True)
        file_info: afs.FileInfo = self._fs.get_file_info(resolved_path)

        if file_info.type == afs.FileType.NotFound:
            raise self._explicit_error(self.ExplicitError.NO_SUCH_FILE_EXCEPTION, storage_path, "SIZE")

        if file_info.type == afs.FileType.Directory:
            raise self._explicit_error(self.ExplicitError.SIZE_OF_DIR, storage_path, "SIZE")

        if not file_info.is_file:
            raise self._explicit_error(self.ExplicitError.UNKNOWN_ERROR, storage_path, "SIZE")

        return file_info.size

    def stat(self, storage_path: str) -> FileStat:

        operation = f"STAT [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._stat, storage_path)

    def _stat(self, storage_path: str) -> FileStat:

        resolved_path = self._resolve_path(storage_path, "STAT", True)

        file_info: afs.FileInfo = self._fs.get_file_info(resolved_path)

        if file_info.type != afs.FileType.File and file_info.type != afs.FileType.Directory:
            raise self._explicit_error(self.ExplicitError.STAT_NOT_FILE_OR_DIR, storage_path, "STAT")

        file_type = FileType.FILE if file_info.is_file else FileType.DIRECTORY
        file_size = file_info.size if file_info.is_file else 0

        return FileStat(
            file_info.base_name,
            file_type,
            file_info.path,
            file_size,
            mtime=file_info.mtime.astimezone(dt.timezone.utc),
            atime=None)

    def ls(self, storage_path: str) -> tp.List[str]:

        operation = f"LS [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._ls, storage_path)

    def _ls(self, storage_path: str) -> tp.List[str]:

        resolved_path = self._resolve_path(storage_path, "LS", True)

        selector = afs.FileSelector(resolved_path, recursive=False)  # noqa
        file_infos: tp.List[afs.FileInfo] = self._fs.get_file_info(selector)

        return list(map(lambda fi: fi.base_name, file_infos))

    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        operation = f"MKDIR [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._mkdir, storage_path, recursive)

    def _mkdir(self, storage_path: str, recursive: bool):

        resolved_path = self._resolve_path(storage_path, "MKDIR", False)

        self._fs.create_dir(resolved_path, recursive=recursive)

    def rm(self, storage_path: str, recursive: bool = False):

        operation = f"RM [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._rm, storage_path, recursive)

    def _rm(self, storage_path: str, recursive: bool = False):

        resolved_path = self._resolve_path(storage_path, "RM", False)

        if recursive:
            self._fs.delete_dir(resolved_path)
        else:
            self._fs.delete_file(resolved_path)

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (READ) [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._read_byte_stream, storage_path)

    def _read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        resolved_path = self._resolve_path(storage_path, "OPEN BYTE STREAM (READ)", False)
        stream: tp.BinaryIO = self._fs.open_input_file(resolved_path)

        stream.seek(0, 2)
        file_size = _util.format_file_size(stream.tell())
        stream.seek(0, 0)

        self._log.info(f"File size [{self._key}]: {file_size} [{storage_path}]")

        return stream

    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (WRITE) [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._write_byte_stream, storage_path)

    def _write_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        resolved_path = self._resolve_path(storage_path, "OPEN BYTE STREAM (WRITE)", False)

        return self._fs.open_output_stream(resolved_path)

    def close_byte_stream(self, storage_path: str, stream: tp.BinaryIO):

        if stream.writable():
            file_size = _util.format_file_size(stream.tell())
            self._log.info(f"File size [{self._key}]: {file_size} [{storage_path}]")
            self._log.info(f"CLOSE BYTE STREAM (WRITE) [{self._key}]: [{storage_path}]")

        else:
            self._log.info(f"CLOSE BYTE STREAM (READ) [{self._key}]: [{storage_path}]")

        stream.close()

    def _wrap_operation(self, operation: str, func: tp.Callable, *args, **kwargs) -> tp.Any:

        try:
            self._log.info(operation)
            return func(*args, **kwargs)

        # ETrac means the error is already handled, log the message as-is
        except _ex.ETrac as e:
            self._log.exception(f"{operation} {str(e)}")
            raise

        # TODO: We don't know what exception types Arrow FS will throw
        # More specialized handling would be good, if that information can be found out

        except Exception as e:
            msg = f"There was a problem in the storage layer: {str(e)}"
            self._log.exception(f"{operation} {msg}")
            raise _ex.EStorageRequest(msg) from e
        
    def _resolve_path(self, storage_path: str, operation_name: str, allow_root_dir: bool) -> str:

        try:

            if storage_path is None or len(storage_path.strip()) == 0:
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_NULL_OR_BLANK, storage_path, operation_name)
    
            relative_path = pathlib.Path(storage_path)
    
            if relative_path.is_absolute():
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_NOT_RELATIVE, storage_path, operation_name)

            root_path = pathlib.Path("/root")
            absolute_path = root_path.joinpath(storage_path).resolve(False)
    
            if len(absolute_path.parts) < len(root_path.parts) or not absolute_path.is_relative_to(root_path):
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_OUTSIDE_ROOT, storage_path, operation_name)

            if absolute_path == root_path and not allow_root_dir:
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_IS_ROOT, storage_path, operation_name)
    
            return str(absolute_path.relative_to(root_path).as_posix())

        except ValueError:

            raise self._explicit_error(self.ExplicitError.STORAGE_PATH_INVALID, storage_path, operation_name)

    def _explicit_error(self, error, storage_path, operation_name):

        message_template = self._ERROR_MESSAGE_MAP.get(error)
        message = message_template.format(operation_name, self._key, storage_path)

        err_type = self._ERROR_TYPE_MAP.get(error)
        err = err_type(message)

        return err

    class ExplicitError(enum.Enum):
    
        # Validation failures
        STORAGE_PATH_NULL_OR_BLANK = 1
        STORAGE_PATH_NOT_RELATIVE = 2
        STORAGE_PATH_OUTSIDE_ROOT = 3
        STORAGE_PATH_IS_ROOT = 4
        STORAGE_PATH_INVALID = 5

        # Explicit errors file in operations
        SIZE_OF_DIR = 10
        STAT_NOT_FILE_OR_DIR = 11
        RM_DIR_NOT_RECURSIVE = 12

        # Exceptions
        NO_SUCH_FILE_EXCEPTION = 20
        FILE_ALREADY_EXISTS_EXCEPTION = 21
        DIRECTORY_NOT_FOUND_EXCEPTION = 22
        NOT_DIRECTORY_EXCEPTION = 23
        ACCESS_DENIED_EXCEPTION = 24
        SECURITY_EXCEPTION = 25
        IO_EXCEPTION = 26

        # Errors in stream (Flow pub/sub) implementation
        DUPLICATE_SUBSCRIPTION = 30

        # These errors have special parameterization for their error messages
        CHUNK_NOT_FULLY_WRITTEN = 31

        # Unhandled / unexpected error
        UNKNOWN_ERROR = 40

    _ERROR_MESSAGE_MAP = {

        ExplicitError.STORAGE_PATH_NULL_OR_BLANK: "Requested storage path is null or blank: {} {} [{}]",
        ExplicitError.STORAGE_PATH_NOT_RELATIVE: "Requested storage path is not a relative path: {} {} [{}]",
        ExplicitError.STORAGE_PATH_OUTSIDE_ROOT: "Requested storage path is outside the storage root directory: {} {} [{}]",  # noqa
        ExplicitError.STORAGE_PATH_IS_ROOT: "Requested operation not allowed on the storage root directory: {} {} [{}]",
        ExplicitError.STORAGE_PATH_INVALID: "Requested storage path is invalid: {} {} [{}]",

        ExplicitError.SIZE_OF_DIR: "Size operation is not available for directories: {} {} [{}]",
        ExplicitError.STAT_NOT_FILE_OR_DIR: "Object is not a file or directory: {} {} [{}]",
        ExplicitError.RM_DIR_NOT_RECURSIVE: "Regular delete operation not available for directories (use recursive delete): {} {} [{}]",  # noqa

        ExplicitError.NO_SUCH_FILE_EXCEPTION: "File not found in storage layer: {} {} [{}]",
        ExplicitError.FILE_ALREADY_EXISTS_EXCEPTION: "File already exists in storage layer: {} {} [{}]",
        ExplicitError.DIRECTORY_NOT_FOUND_EXCEPTION: "Directory not found in storage layer: {} {} [{}]",
        ExplicitError.NOT_DIRECTORY_EXCEPTION: "Path is not a directory in storage layer: {} {} [{}]",
        ExplicitError.ACCESS_DENIED_EXCEPTION: "Access denied in storage layer: {} {} [{}]",
        ExplicitError.SECURITY_EXCEPTION: "Access denied in storage layer: {} {} [{}]",
        ExplicitError.IO_EXCEPTION: "An IO error occurred in the storage layer: {} {} [{}]",

        ExplicitError.DUPLICATE_SUBSCRIPTION: "Duplicate subscription detected in the storage layer: {} {} [{}]",
        ExplicitError.CHUNK_NOT_FULLY_WRITTEN: "Chunk was not fully written, chunk size = {} B, written = {} B",

        ExplicitError.UNKNOWN_ERROR: "An unexpected error occurred in the storage layer: {} {} [{}]",
    }

    _ERROR_TYPE_MAP = {

        ExplicitError.STORAGE_PATH_NULL_OR_BLANK: _ex.EStorageRequest,
        ExplicitError.STORAGE_PATH_NOT_RELATIVE: _ex.EStorageRequest,
        ExplicitError.STORAGE_PATH_OUTSIDE_ROOT: _ex.EStorageRequest,
        ExplicitError.STORAGE_PATH_IS_ROOT: _ex.EStorageRequest,
        ExplicitError.STORAGE_PATH_INVALID: _ex.EStorageRequest,
    
        ExplicitError.SIZE_OF_DIR: _ex.EStorageRequest,
        ExplicitError.RM_DIR_NOT_RECURSIVE: _ex.EStorageRequest,
        ExplicitError.STAT_NOT_FILE_OR_DIR: _ex.EStorageRequest,
    
        ExplicitError.NO_SUCH_FILE_EXCEPTION: _ex.EStorageRequest,
        ExplicitError.FILE_ALREADY_EXISTS_EXCEPTION: _ex.EStorageRequest,
        ExplicitError.DIRECTORY_NOT_FOUND_EXCEPTION: _ex.EStorageRequest,
        ExplicitError.NOT_DIRECTORY_EXCEPTION: _ex.EStorageRequest,
        ExplicitError.ACCESS_DENIED_EXCEPTION: _ex.EStorageAccess,
        ExplicitError.SECURITY_EXCEPTION: _ex.EStorageAccess,
        ExplicitError.IO_EXCEPTION: _ex.EStorage,
    
        ExplicitError.DUPLICATE_SUBSCRIPTION: _ex.ETracInternal,
        ExplicitError.CHUNK_NOT_FULLY_WRITTEN: _ex.EStorageCommunication,

        ExplicitError.UNKNOWN_ERROR: _ex.ETracInternal
    }
        

# ----------------------------------------------------------------------------------------------------------------------
# COMMON DATA STORAGE IMPLEMENTATION
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

            codec_properties = storage_options.copy() if storage_options else dict()

            # Codec properties can be specified in the storage config
            # Properties starting with the codec format code are considered codec properties

            format_code = FormatManager.primary_format_code(storage_format)
            format_code_pattern = re.compile(f"^{format_code}.", re.IGNORECASE)

            for prop_key, prop_value in self.__config.properties.items():
                if format_code_pattern.match(prop_key):
                    format_prop_key = prop_key[len(format_code) + 1:]
                    codec_properties[format_prop_key] = prop_value

            codec = FormatManager.get_data_format(storage_format, codec_properties)

            stat = self.__file_storage.stat(storage_path)

            if stat.file_type == FileType.DIRECTORY:

                dir_content = self.__file_storage.ls(storage_path)

                if len(dir_content) == 1:
                    storage_path = storage_path.rstrip("/\\") + "/" + dir_content[0]
                else:
                    raise NotImplementedError("Directory storage format not available yet")

            with self.__file_storage.read_byte_stream(storage_path) as byte_stream:
                table = codec.read_table(byte_stream, schema)
                self.__file_storage.close_byte_stream(storage_path, byte_stream)

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

            codec = FormatManager.get_data_format(storage_format, storage_options)
            extension = codec.file_extension()

            # TODO: Full handling of directory storage formats

            if not storage_path.endswith(extension):
                parent_dir_ = storage_path
                storage_path_ = storage_path.rstrip("/\\") + f"/chunk-0.{extension}"
                self.__file_storage.mkdir(parent_dir_, True, exists_ok=overwrite)
            else:
                parent_dir_ = str(pathlib.PurePath(storage_path).parent)
                storage_path_ = storage_path
                self.__file_storage.mkdir(parent_dir_, True, True)

            with self.__file_storage.write_byte_stream(storage_path_, overwrite=overwrite) as byte_stream:
                codec.write_table(byte_stream, table)
                self.__file_storage.close_byte_stream(storage_path, byte_stream)

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
