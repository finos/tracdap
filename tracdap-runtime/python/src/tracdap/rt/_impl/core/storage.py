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

import datetime as dt
import enum
import pathlib
import re
import sys
import typing as tp
import traceback as tb

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.lib as pa_lib

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.plugins as plugins
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.util as _util
import tracdap.rt._impl.core.validation as _val

# Import storage interfaces (using the internal version, it has extra bits that are not public)
from tracdap.rt._impl.ext.storage import *


class FormatManager:

    @classmethod
    def get_data_format(cls, format_code: str, format_options: tp.Dict[str, tp.Any]) -> IDataFormat:

        try:

            config = _cfg.PluginConfig(
                protocol=format_code,
                properties=format_options)

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

        self.__log = _logging.logger_for_object(self)
        self.__file_storage: tp.Dict[str, IFileStorage] = dict()
        self.__data_storage: tp.Dict[str, IDataStorage] = dict()
        self.__external: tp.List[str] = list()
        self.__sys_config = sys_config

        for storage_key, storage_config in sys_config.storage.buckets.items():
            self.create_storage(storage_key, storage_config)

        for storage_key, storage_config in sys_config.storage.external.items():
            if storage_key in self.__file_storage or storage_key in self.__data_storage:
                raise _ex.EConfig(f"Storage key [{storage_key}] is defined as both internal and external storage")
            self.__external.append(storage_key)
            self.create_storage(storage_key, storage_config)

    def default_storage_key(self):
        return self.__sys_config.storage.defaultBucket

    def default_storage_format(self):
        return self.__sys_config.storage.defaultFormat

    def create_storage(self, storage_key: str, storage_config: _cfg.PluginConfig):

        # Add global properties related to the storage protocol
        related_props = {
            k: v for (k, v) in self.__sys_config.properties.items()
            if k.startswith(f"{storage_config.protocol}.")}

        storage_config.properties.update(related_props)

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

    def has_file_storage(self, storage_key: str, external: bool = False) -> bool:

        if external ^ (storage_key in self.__external):
            return False

        return storage_key in self.__file_storage

    def get_file_storage(self, storage_key: str, external: bool = False) -> IFileStorage:

        if not self.has_file_storage(storage_key, external):
            err = f"File storage is not configured for storage key [{storage_key}]"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        return self.__file_storage[storage_key]

    def has_data_storage(self, storage_key: str, external: bool = False) -> bool:

        if external ^ (storage_key in self.__external):
            return False

        return storage_key in self.__data_storage

    def get_data_storage(self, storage_key: str, external: bool = False) -> IDataStorage:

        if not self.has_data_storage(storage_key, external):
            err = f"Data storage is not configured for storage key [{storage_key}]"
            self.__log.error(err)
            raise _ex.EStorageConfig(err)

        return self.__data_storage[storage_key]


# ----------------------------------------------------------------------------------------------------------------------
# COMMON FILE STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class _NativeFileContext(tp.ContextManager[tp.BinaryIO]):

    def __init__(self, nf: pa_lib.NativeFile, close_func: tp.Callable):
        super().__init__()
        self.__nf = nf
        self.__close_func = close_func

    def __enter__(self):
        return self.__nf

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.__close_func()
        finally:
            self.__nf.close()


class CommonFileStorage(IFileStorage):

    _TRAC_DIR_MARKER = "/.trac_dir"

    FILE_SEMANTICS_FS_TYPES = ["local"]
    BUCKET_SEMANTICS_FS_TYPES = ["s3", "gcs", "abfs"]

    def __init__(self, storage_key: str, storage_config: _cfg.PluginConfig, fs: pa_fs.SubTreeFileSystem):

        self._log = _logging.logger_for_object(self)
        self._key = storage_key
        self._config = storage_config
        self._fs = fs

        fs_type = fs.base_fs.type_name
        fs_impl = "arrow"
        fs_root = fs.base_path

        # On Windows, sanitise UNC root paths for logging
        if _util.is_windows() and fs_root.startswith("//?/"):
            fs_root = fs_root[4:]

        # If this is an FSSpec implementation, take the protocol from FSSpec as the FS type
        base_fs = fs.base_fs
        if isinstance(base_fs, pa_fs.PyFileSystem):
            handler = base_fs.handler
            if isinstance(handler, pa_fs.FSSpecHandler):
                fs_type = handler.fs.protocol[0] if isinstance(handler.fs.protocol, tuple) else handler.fs.protocol
                fs_impl = "fsspec"

        # Some optimization is possible if the underlying storage semantics are known
        self._file_semantics = True if fs_type in self.FILE_SEMANTICS_FS_TYPES else False
        self._bucket_semantics = True if fs_type in self.BUCKET_SEMANTICS_FS_TYPES else False
        self._explicit_dir_semantics = True if self._bucket_semantics and fs_impl == "fsspec" else False

        self._log.info(
            f"INIT [{self._key}]: Common file storage, " +
            f"fs = [{fs_type}], " +
            f"impl = [{fs_impl}], " +
            f"root = [{fs_root}]")

    def exists(self, storage_path: str) -> bool:

        return self._wrap_operation(self._exists, "EXISTS", storage_path)

    def _exists(self, operation_name: str, storage_path: str) -> bool:

        resolved_path = self._resolve_path(operation_name, storage_path, True)

        file_info: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)
        return file_info.type != pa_fs.FileType.NotFound

    def size(self, storage_path: str) -> int:

        return self._wrap_operation(self._size, "SIZE", storage_path)

    def _size(self, operation_name: str, storage_path: str) -> int:

        resolved_path = self._resolve_path(operation_name, storage_path, True)
        file_info: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)

        if file_info.type == pa_fs.FileType.NotFound:
            raise self._explicit_error(self.ExplicitError.OBJECT_NOT_FOUND, operation_name, storage_path)

        if not file_info.is_file:
            raise self._explicit_error(self.ExplicitError.NOT_A_FILE, operation_name, storage_path)

        return file_info.size

    def stat(self, storage_path: str) -> FileStat:

        return self._wrap_operation(self._stat, "STAT", storage_path)

    def _stat(self, operation_name: str, storage_path: str) -> FileStat:

        resolved_path = self._resolve_path(operation_name, storage_path, True)

        file_info: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)

        if file_info.type == pa_fs.FileType.NotFound:
            raise self._explicit_error(self.ExplicitError.OBJECT_NOT_FOUND, operation_name, storage_path)

        if file_info.type != pa_fs.FileType.File and file_info.type != pa_fs.FileType.Directory:
            raise self._explicit_error(self.ExplicitError.NOT_A_FILE_OR_DIRECTORY, operation_name, storage_path)

        return self._info_to_stat(file_info)

    @staticmethod
    def _info_to_stat(file_info: pa_fs.FileInfo):

        if file_info.path == "":
            file_name = "."
            storage_path = "."
        elif file_info.path.startswith("./"):
            file_name = file_info.base_name
            storage_path = file_info.path[2:]
        else:
            file_name = file_info.base_name
            storage_path = file_info.path

        file_type = FileType.FILE if file_info.is_file else FileType.DIRECTORY
        file_size = file_info.size if file_info.is_file else 0

        # Normalization in case the impl gives back directory entries with a trailing slash
        if file_type == FileType.DIRECTORY and storage_path.endswith("/"):
            storage_path = storage_path[:-1]
            separator = storage_path.rfind("/")
            file_name = storage_path[separator+1:]

        mtime = file_info.mtime.astimezone(dt.timezone.utc) if file_info.mtime is not None else None

        return FileStat(
            file_name,
            file_type,
            storage_path,
            file_size,
            mtime=mtime,
            atime=None)

    def ls(self, storage_path: str, recursive: bool = False) -> tp.List[FileStat]:

        return self._wrap_operation(self._ls, "LS", storage_path, recursive)

    def _ls(self, operation_name: str, storage_path: str, recursive: bool) -> tp.List[FileStat]:

        resolved_path = self._resolve_path(operation_name, storage_path, True)

        # _stat() will fail for file not found, or if the path is not a file/directory
        stat = self._stat(operation_name, storage_path)

        # Calling LS on a file should return a list with one entry for just that file
        if stat.file_type == FileType.FILE:
            return [stat]

        # Otherwise do a normal directory listing
        else:
            # A trailing slash prevents some implementations including the directory in its own listing
            selector = pa_fs.FileSelector(resolved_path + "/", recursive=recursive)  # noqa
            file_infos = self._fs.get_file_info(selector)
            file_infos = filter(lambda fi: not fi.path.endswith(self._TRAC_DIR_MARKER), file_infos)
            return list(map(self._info_to_stat, file_infos))

    def mkdir(self, storage_path: str, recursive: bool = False):

        return self._wrap_operation(self._mkdir, "MKDIR", storage_path, recursive)

    def _mkdir(self, operation_name: str, storage_path: str, recursive: bool):

        resolved_path = self._resolve_path(operation_name, storage_path, False)

        # Try to prevent MKDIR if a file or file-like object already exists
        # In cloud bucket semantics a file and dir can both exist with the same name - very confusing!
        # There is a race condition here because a file could be created by another process
        # But, given the very structured way TRAC uses file storage, this is extremely unlikely

        prior_stat: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)
        if prior_stat.type == pa_fs.FileType.File or prior_stat.type == pa_fs.FileType.Unknown:
            raise self._explicit_error(self.ExplicitError.OBJECT_ALREADY_EXISTS, operation_name, storage_path)

        # For most FS types, it is fine to use the Arrow create_dir() method
        # For bucket-like storage, this will normally create an empty blob with a name like "my_dir/"

        if not self._explicit_dir_semantics:
            self._fs.create_dir(resolved_path, recursive=recursive)
            return

        # Some FS backends for bucket-like storage do not allow empty blobs as directories
        # For these backends, we have to create an explicit marker file inside the directory
        # In this case it is also necessary to check parents explicitly for non-recursive requests

        if not recursive and prior_stat.type == pa_fs.FileType.NotFound:
            parent_path = self._resolve_parent(resolved_path)
            if parent_path is not None:
                parent_stat: pa_fs.FileInfo = self._fs.get_file_info(parent_path)
                if parent_stat.type != pa_fs.FileType.Directory:
                    raise FileNotFoundError

        dir_marker = resolved_path + self._TRAC_DIR_MARKER
        with self._fs.open_output_stream(dir_marker) as stream:
            stream.write(b"")

    def rm(self, storage_path: str):

        return self._wrap_operation(self._rm, "RM", storage_path)

    def _rm(self, operation_name: str, storage_path: str):

        resolved_path = self._resolve_path(operation_name, storage_path, False)

        file_info: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)
        if file_info.type == pa_fs.FileType.Directory:
            raise self._explicit_error(self.ExplicitError.NOT_A_FILE, operation_name, storage_path)

        self._fs.delete_file(resolved_path)

    def rmdir(self, storage_path: str):

        return self._wrap_operation(self._rmdir, "RMDIR", storage_path)

    def _rmdir(self, operation_name: str, storage_path: str):

        resolved_path = self._resolve_path(operation_name, storage_path, False)

        file_info: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)
        if file_info.type == pa_fs.FileType.File:
            raise self._explicit_error(self.ExplicitError.NOT_A_DIRECTORY, operation_name, storage_path)

        self._fs.delete_dir(resolved_path)

    def read_byte_stream(self, storage_path: str) -> tp.ContextManager[tp.BinaryIO]:

        return self._wrap_operation(self._read_byte_stream, "OPEN BYTE STREAM (READ)", storage_path)

    def _read_byte_stream(self, operation_name: str, storage_path: str) -> tp.ContextManager[tp.BinaryIO]:

        resolved_path = self._resolve_path(operation_name, storage_path, False)

        # Check some information about the file before attempting the read
        # There is a race condition here so open_input_file() can still fail
        # Even so, prior_stat gives more meaningful error information in the common case
        # If the file is changed before open_input_file, errors will be raised but might be less meaningful
        prior_stat: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)
        if prior_stat.type == pa_fs.FileType.NotFound:
            raise self._explicit_error(self.ExplicitError.OBJECT_NOT_FOUND, operation_name, storage_path)
        if prior_stat.type != pa_fs.FileType.File:
            raise self._explicit_error(self.ExplicitError.NOT_A_FILE, operation_name, storage_path)

        # Since the size is known, log it now rather than calling stream.seek() and stream.tell()
        self._log.info(f"File size [{self._key}]: {prior_stat.size} [{storage_path}]")

        # Open the stream
        stream = self._fs.open_input_file(resolved_path)

        # Return impl of PyArrow NativeFile instead of BinaryIO - this is the same thing PyArrow does
        return _NativeFileContext(stream, lambda: self._close_byte_stream(storage_path, stream, False))  # noqa

    def write_byte_stream(self, storage_path: str) -> tp.ContextManager[tp.BinaryIO]:

        return self._wrap_operation(self._write_byte_stream, "OPEN BYTE STREAM (WRITE)", storage_path)

    def _write_byte_stream(self, operation_name: str, storage_path: str) -> tp.ContextManager[tp.BinaryIO]:

        resolved_path = self._resolve_path(operation_name, storage_path, False)

        # Make sure the parent directory exists
        # In bucket semantics this is not needed and creating a 0-byte object for every real object is a bad idea
        # For file semantics, or if semantics are not known, create the parent dir to avoid failures
        if not self._bucket_semantics:
            parent_path = self._resolve_parent(resolved_path)
            if parent_path is not None:
                self._mkdir(operation_name, parent_path, recursive=True)

        # Try to prevent WRITE if the object is already defined as a directory or other non-file object
        # In cloud bucket semantics a file and dir can both exist with the same name - very confusing!
        # There is a race condition here because a directory could be created by another process
        # But, given the very structured way TRAC uses file storage, this is extremely unlikely
        prior_stat: pa_fs.FileInfo = self._fs.get_file_info(resolved_path)
        if prior_stat.type != pa_fs.FileType.NotFound and prior_stat.type != pa_fs.FileType.File:
            raise self._explicit_error(self.ExplicitError.OBJECT_ALREADY_EXISTS, operation_name, storage_path)

        # If the file does not already exist and the write operation fails, try to clean it up
        delete_on_error = prior_stat.type == pa_fs.FileType.NotFound

        # Open the stream
        stream = self._fs.open_output_stream(resolved_path)

        # Return impl of  PyArrow NativeFile instead of BinaryIO - this is the same thing PyArrow does
        return _NativeFileContext(stream, lambda: self._close_byte_stream(storage_path, stream, True, delete_on_error))  # noqa

    def _close_byte_stream(self, storage_path: str, stream: tp.BinaryIO, is_write: bool, delete_on_error: bool = False):

        # If there has been an error, log it
        exc_info = sys.exc_info()
        error = exc_info[1] if exc_info is not None else None

        if error is not None:
            self._log.exception(str(error))

        # For successful write streams, log the total size written
        if is_write and not error:
            if not stream.closed:
                file_size = _util.format_file_size(stream.tell())
            else:
                file_size = self._fs.get_file_info(storage_path).size
            self._log.info(f"File size [{self._key}]: {file_size} [{storage_path}]")

        # Close the stream - this may take time for write streams that are not flushed
        # Closing here gives better logs, because any pause is before the close message
        # As a fail-safe, _NativeFileResource always calls close() in a "finally" block
        if not stream.closed:
            stream.close()

        # Log closing of the stream
        if is_write:
            self._log.info(f"CLOSE BYTE STREAM (WRITE) [{self._key}]: [{storage_path}]")

        else:
            self._log.info(f"CLOSE BYTE STREAM (READ) [{self._key}]: [{storage_path}]")

        # If there is an error and cleanup is requested, try to remove the partially written file
        # This is best-efforts, don't blow up if the cleanup fails
        if error is not None and delete_on_error:
            try:
                file_info = self._fs.get_file_info(storage_path)
                if file_info.type != pa_fs.FileType.NotFound:
                    self._fs.delete_file(storage_path)
            # different implementations can throw different errors here
            except Exception:  # noqa
                pass

        # Stream implementations can raise various types of error during stream operations
        # Errors can have different causes (access, communication, missing / duplicate files etc.)
        # Also, other errors can occur inside the stream context manager, unrelated to IO

        # In the case of an IO error we want to raise EStorage, other errors should propagate as they are
        # This handler tries to spot IO errors from inside the PyArrow library, it is probably not fail-safe
        # If an IO error is not spotted, the original error will propagate and get reported as EUnexpected
        # Anyway this handler is only for errors that happen after the stream is opened

        # The alternative is to override every method in _NativeFileResource and try to catch there
        # However, different implementations raise different error types, so we still need some kind of inspection

        if error is not None:

            if isinstance(error, OSError):
                raise _ex.EStorage from error

            stack = tb.extract_tb(exc_info[2])
            stack = filter(lambda frame: frame.filename is not None, stack)

            if any(filter(lambda frame: frame.filename.startswith("pyarrow/"), stack)):
                raise _ex.EStorage from error

    def _wrap_operation(self, func: tp.Callable, operation_name: str, storage_path: str, *args, **kwargs) -> tp.Any:

        operation = f"{operation_name} {self._key} [{storage_path}]"

        try:
            self._log.info(operation)
            return func(operation_name, storage_path, *args, **kwargs)

        # ETrac means the error is already handled, log the message as-is

        except _ex.ETrac as e:
            self._log.exception(f"{operation}: {str(e)}")
            raise

        # Arrow maps filesystem errors into native Python OS errors

        except FileNotFoundError as e:
            error = self._explicit_error(self.ExplicitError.OBJECT_NOT_FOUND, operation_name, storage_path)
            self._log.exception(f"{operation}: {str(error)}")
            raise error from e

        except FileExistsError as e:
            error = self._explicit_error(self.ExplicitError.OBJECT_ALREADY_EXISTS, operation_name, storage_path)
            self._log.exception(f"{operation}: {str(error)}")
            raise error from e

        except IsADirectoryError as e:
            error = self._explicit_error(self.ExplicitError.NOT_A_FILE, operation_name, storage_path)
            self._log.exception(f"{operation}: {str(error)}")
            raise error from e

        except NotADirectoryError as e:
            error = self._explicit_error(self.ExplicitError.NOT_A_DIRECTORY, operation_name, storage_path)
            self._log.exception(f"{operation}: {str(error)}")
            raise error from e

        except PermissionError as e:
            error = self._explicit_error(self.ExplicitError.ACCESS_DENIED, operation_name, storage_path)
            self._log.exception(f"{operation}: {str(error)}")
            raise error from e

        # OSError is the top-level error for IO exceptions
        # This is raised on some platforms if there is not a recognized errno from the low-level operation

        except OSError as e:
            error = self._explicit_error(self.ExplicitError.IO_ERROR, operation_name, storage_path)
            self._log.error(f"{operation}: {str(e)}")
            self._log.exception(f"{operation}: {str(error)}")
            raise error from e

        # Other types of exception are not expected - report these as internal errors

        except Exception as e:
            error = self._explicit_error(self.ExplicitError.UNKNOWN_ERROR, operation_name, storage_path)
            self._log.exception(f"{operation}: {str(error)}")
            raise error from e
        
    def _resolve_path(self, operation_name: str, storage_path: str, allow_root_dir: bool) -> str:

        try:

            if _val.StorageValidator.storage_path_is_empty(storage_path):
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_NULL_OR_BLANK, operation_name, storage_path)

            if _val.StorageValidator.storage_path_invalid(storage_path):
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_INVALID, operation_name, storage_path)

            if _val.StorageValidator.storage_path_not_relative(storage_path):
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_NOT_RELATIVE, operation_name, storage_path)

            if _val.StorageValidator.storage_path_outside_root(storage_path):
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_OUTSIDE_ROOT, operation_name, storage_path)

            if not allow_root_dir and _val.StorageValidator.storage_path_is_root(storage_path):
                raise self._explicit_error(self.ExplicitError.STORAGE_PATH_IS_ROOT, operation_name, storage_path)

            root_path = pathlib.Path("C:\\root") if _util.is_windows() else pathlib.Path("/root")
            relative_path = pathlib.Path(storage_path)
            absolute_path = root_path.joinpath(relative_path).resolve(False)

            if absolute_path == root_path:
                return ""
            else:
                return absolute_path.relative_to(root_path).as_posix()

        except ValueError as e:

            raise self._explicit_error(self.ExplicitError.STORAGE_PATH_INVALID, operation_name, storage_path) from e

    @staticmethod
    def _resolve_parent(storage_path: str) -> tp.Optional[str]:

        root_path = pathlib.Path("C:\\root") if _util.is_windows() else pathlib.Path("/root")
        absolute_path = root_path.joinpath(storage_path).resolve(False)

        if absolute_path == root_path or absolute_path.parent == root_path:
            return None

        else:
            return pathlib.Path(storage_path).parent.as_posix()

    def _explicit_error(self, error, operation_name, storage_path):

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

        # Exceptions
        OBJECT_NOT_FOUND = 10
        OBJECT_ALREADY_EXISTS = 11
        NOT_A_FILE = 12
        NOT_A_DIRECTORY = 13
        NOT_A_FILE_OR_DIRECTORY = 14
        IO_ERROR = 15

        # Permissions
        ACCESS_DENIED = 20

        # Unhandled / unexpected error
        UNKNOWN_ERROR = 30

    _ERROR_MESSAGE_MAP = {

        ExplicitError.STORAGE_PATH_NULL_OR_BLANK: "Requested storage path is null or blank: {} {} [{}]",
        ExplicitError.STORAGE_PATH_NOT_RELATIVE: "Requested storage path is not a relative path: {} {} [{}]",
        ExplicitError.STORAGE_PATH_OUTSIDE_ROOT: "Requested storage path is outside the storage root directory: {} {} [{}]",  # noqa
        ExplicitError.STORAGE_PATH_IS_ROOT: "Requested operation not allowed on the storage root directory: {} {} [{}]",
        ExplicitError.STORAGE_PATH_INVALID: "Requested storage path is invalid: {} {} [{}]",

        ExplicitError.OBJECT_NOT_FOUND: "Object not found in storage layer: {} {} [{}]",
        ExplicitError.OBJECT_ALREADY_EXISTS: "Object already exists in storage layer: {} {} [{}]",
        ExplicitError.NOT_A_FILE: "Object is not a file: {} {} [{}]",
        ExplicitError.NOT_A_DIRECTORY: "Object is not a directory: {} {} [{}]",
        ExplicitError.NOT_A_FILE_OR_DIRECTORY: "Object is not a file or directory: {} {} [{}]",
        ExplicitError.IO_ERROR: "An IO error occurred in the storage layer: {} {} [{}]",

        ExplicitError.ACCESS_DENIED: "Access denied in storage layer: {} {} [{}]",

        ExplicitError.UNKNOWN_ERROR: "An unexpected error occurred in the storage layer: {} {} [{}]",
    }

    _ERROR_TYPE_MAP = {

        ExplicitError.STORAGE_PATH_NULL_OR_BLANK: _ex.EStorageValidation,
        ExplicitError.STORAGE_PATH_NOT_RELATIVE: _ex.EStorageValidation,
        ExplicitError.STORAGE_PATH_OUTSIDE_ROOT: _ex.EStorageValidation,
        ExplicitError.STORAGE_PATH_IS_ROOT: _ex.EStorageValidation,
        ExplicitError.STORAGE_PATH_INVALID: _ex.EStorageValidation,

        ExplicitError.OBJECT_NOT_FOUND: _ex.EStorageRequest,
        ExplicitError.OBJECT_ALREADY_EXISTS: _ex.EStorageRequest,
        ExplicitError.NOT_A_FILE: _ex.EStorageRequest,
        ExplicitError.NOT_A_DIRECTORY: _ex.EStorageRequest,
        ExplicitError.NOT_A_FILE_OR_DIRECTORY: _ex.EStorageRequest,
        ExplicitError.IO_ERROR: _ex.EStorageRequest,

        ExplicitError.ACCESS_DENIED: _ex.EStorageAccess,

        ExplicitError.UNKNOWN_ERROR: _ex.ETracInternal
    }


# ----------------------------------------------------------------------------------------------------------------------
# COMMON DATA STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class CommonDataStorage(IDataStorage):

    def __init__(
            self, config: _cfg.PluginConfig, file_storage: IFileStorage,
            pushdown_pandas: bool = False, pushdown_spark: bool = False):

        self.__log = _logging.logger_for_object(self)

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
                    storage_path = dir_content[0].storage_path
                else:
                    raise NotImplementedError("Directory storage format not available yet")

            with self.__file_storage.read_byte_stream(storage_path) as byte_stream:
                table = codec.read_table(byte_stream, schema)

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
                self.__file_storage.mkdir(parent_dir_, True)
            else:
                parent_dir_ = str(pathlib.PurePath(storage_path).parent)
                storage_path_ = storage_path
                self.__file_storage.mkdir(parent_dir_, True)

            if not overwrite and self.__file_storage.exists(storage_path_):
                raise _ex.EStorageRequest(f"File already exists: [{storage_path_}]")

            with self.__file_storage.write_byte_stream(storage_path_) as byte_stream:
                codec.write_table(byte_stream, table)

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
