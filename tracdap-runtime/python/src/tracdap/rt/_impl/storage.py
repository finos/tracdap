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

        file_info: afs.FileInfo = self._fs.get_file_info(storage_path)
        return file_info.type != afs.FileType.NotFound

    def size(self, storage_path: str) -> int:

        operation = f"SIZE [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._size, storage_path)

    def _size(self, storage_path: str) -> int:

        file_info: afs.FileInfo = self._fs.get_file_info(storage_path)
        return file_info.size

    def stat(self, storage_path: str) -> FileStat:

        operation = f"STAT [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._stat, storage_path)

    def _stat(self, storage_path: str) -> FileStat:

        file_info: afs.FileInfo = self._fs.get_file_info(storage_path)
        file_type = FileType.FILE if file_info.is_file else FileType.DIRECTORY

        return FileStat(
            file_type, file_info.size,
            ctime=file_info.mtime,
            mtime=file_info.mtime,
            atime=None)

    def ls(self, storage_path: str) -> tp.List[str]:

        operation = f"LS [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._ls, storage_path)

    def _ls(self, storage_path: str) -> tp.List[str]:

        selector = afs.FileSelector(storage_path, recursive=False)  # noqa
        file_infos: tp.List[afs.FileInfo] = self._fs.get_file_info(selector)

        return list(map(lambda fi: fi.base_name, file_infos))

    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        operation = f"MKDIR [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._mkdir, storage_path, recursive)

    def _mkdir(self, storage_path: str, recursive: bool):

        self._fs.create_dir(storage_path, recursive=recursive)

    def rm(self, storage_path: str, recursive: bool = False):

        operation = f"RM [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._rm, storage_path, recursive)

    def _rm(self, storage_path: str, recursive: bool = False):

        if recursive:
            self._fs.delete_dir(storage_path)
        else:
            self._fs.delete_file(storage_path)

    def read_bytes(self, storage_path: str) -> bytes:

        stream = self.read_byte_stream(storage_path)

        try:
            return stream.read()
        finally:
            self.close_byte_stream(storage_path, stream)

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (READ) [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._read_byte_stream, storage_path)

    def _read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        stream: tp.BinaryIO = self._fs.open_input_file(storage_path)

        stream.seek(0, 2)
        file_size = _util.format_file_size(stream.tell())
        stream.seek(0, 0)

        self._log.info(f"File size [{self._key}]: {file_size} [{storage_path}]")

        return stream

    def write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):

        stream = self.write_byte_stream(storage_path, overwrite)

        try:
            stream.write(data)
        finally:
            self.close_byte_stream(storage_path, stream)

    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (WRITE) [{self._key}]: [{storage_path}]"
        return self._wrap_operation(operation, self._write_byte_stream, storage_path)

    def _write_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        return self._fs.open_output_stream(storage_path)

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

        # TODO: We don't know what exception types Arrow FS will throw
        # More specialized handling would be good, if that information can be found out

        except Exception as e:
            msg = f"There was a problem in the storage layer: {str(e)}"
            self._log.exception(f"{operation}: {msg}")
            raise _ex.EStorageRequest(msg) from e


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
