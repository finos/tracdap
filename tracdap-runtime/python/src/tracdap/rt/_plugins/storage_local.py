#  Copyright 2023 Accenture Global Solutions Limited
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
import os
import re
import sys
import typing as tp
import pathlib

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex

# Import storage interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

import pyarrow.fs as afs

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class LocalStorageProvider(IStorageProvider):

    ROOT_PATH_PROPERTY = "rootPath"
    ARROW_NATIVE_FS_PROPERTY = "arrowNativeFs"
    ARROW_NATIVE_FS_DEFAULT = False

    def __init__(self, properties: tp.Dict[str, str]):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

        self._root_path = self.check_root_path(self._properties, self._log)

        self._arrow_native = _helpers.get_plugin_property_boolean(
            properties, self.ARROW_NATIVE_FS_PROPERTY, self.ARROW_NATIVE_FS_DEFAULT)

    def has_arrow_native(self) -> bool:
        return True if self._arrow_native else False

    def has_file_storage(self) -> bool:
        return False if self._arrow_native else True

    def get_arrow_native(self) -> afs.SubTreeFileSystem:
        root_fs = afs.LocalFileSystem()
        return afs.SubTreeFileSystem(str(self._root_path), root_fs)

    def get_file_storage(self) -> IFileStorage:

        config = cfg.PluginConfig()
        config.protocol = "LOCAL"
        config.properties = self._properties

        options = dict()

        return LocalFileStorage(config, options)

    @classmethod
    def check_root_path(cls, properties, log):

        root_path_config = _helpers.get_plugin_property(properties, cls.ROOT_PATH_PROPERTY)

        if not root_path_config or root_path_config.isspace():
            err = f"Storage root path not set"
            log.error(err)
            raise ex.EStorageRequest(err)

        supplied_root = pathlib.Path(root_path_config)

        if supplied_root.is_absolute():
            absolute_root = supplied_root

        else:
            err = f"Relative path not allowed for storage root [{supplied_root}]"
            log.error(err)
            raise ex.EStorageConfig(err)

        try:
            return absolute_root.resolve(strict=True)

        except FileNotFoundError as e:
            err = f"Storage root path does not exist: [{absolute_root}]"
            log.error(err)
            raise ex.EStorageRequest(err) from e


plugins.PluginManager.register_plugin(IStorageProvider, LocalStorageProvider, ["LOCAL", "file"])


# ----------------------------------------------------------------------------------------------------------------------
# CUSTOM IMPLEMENTATION FOR LOCAL STORAGE
# ----------------------------------------------------------------------------------------------------------------------

# This is the old implementation that was used before Arrow native was made available
# It is likely to be removed in a future release


class _StreamResource(tp.BinaryIO):  # noqa

    def __init__(self, ctx_mgr, close_func):
        self.__ctx_mgr = ctx_mgr
        self.__close_func = close_func

    def __getitem__(self, item):
        return self.__ctx_mgr.__getitem__(item)

    def __enter__(self):
        return self.__ctx_mgr.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.__close_func()
        finally:
            self.__ctx_mgr.__exit__(exc_type, exc_val, exc_tb)


class LocalFileStorage(IFileStorage):

    def __init__(self, config: cfg.PluginConfig, options: dict = None):

        self._log = _helpers.logger_for_object(self)
        self._properties = config.properties
        self._options = options  # Not used

        self._root_path = LocalStorageProvider.check_root_path(self._properties, self._log)

    def _get_root(self):
        return self._root_path

    def exists(self, storage_path: str) -> bool:

        operation = f"EXISTS [{storage_path}]"
        return self._error_handling(operation, lambda: self._exists(storage_path))

    def _exists(self, storage_path: str) -> bool:

        item_path = self._resolve_path(storage_path, "EXISTS", True)
        return item_path.exists()

    def size(self, storage_path: str) -> int:

        operation = f"SIZE [{storage_path}]"
        return self._error_handling(operation, lambda: self._size(storage_path))

    def _size(self, storage_path: str) -> int:

        item_path = self._resolve_path(storage_path, "SIZE", True)

        if not item_path.exists():
            raise ex.EStorageRequest(f"Storage path does not exist: SIZE [{storage_path}]")

        if not item_path.is_file():
            raise ex.EStorageRequest(f"Storage path is not a file: SIZE [{storage_path}]")

        os_stat = item_path.stat()
        return os_stat.st_size

    def stat(self, storage_path: str) -> FileStat:

        operation = f"STAT [{storage_path}]"
        return self._error_handling(operation, lambda: self._stat(storage_path))

    def _stat(self, storage_path: str) -> FileStat:

        item_path = self._resolve_path(storage_path, "STAT", True)
        os_stat = item_path.stat()

        return self._os_to_trac_stat(item_path, os_stat)

    def _os_to_trac_stat(self, item_path: pathlib.Path, os_stat: os.stat_result):

        file_name = "." if item_path == self._root_path else item_path.name

        file_type = FileType.FILE if item_path.is_file() \
            else FileType.DIRECTORY if item_path.is_dir() \
            else None

        return FileStat(
            file_name=file_name,
            file_type=file_type,
            storage_path=str(item_path.relative_to(self._root_path).as_posix()),
            size=os_stat.st_size,
            mtime=dt.datetime.fromtimestamp(os_stat.st_mtime, dt.timezone.utc),
            atime=dt.datetime.fromtimestamp(os_stat.st_atime, dt.timezone.utc))

    def ls(self, storage_path: str, recursive: bool = False) -> tp.List[FileStat]:

        operation = f"LS [{storage_path}]"
        return self._error_handling(operation, lambda: self._ls(storage_path, recursive))

    def _ls(self, storage_path: str, recursive: bool = False) -> tp.List[FileStat]:

        item_path = self._resolve_path(storage_path, "LS", True)

        if not item_path.exists():
            raise ex.EStorageRequest(f"Storage path does not exist: STAT [{storage_path}]")

        if not item_path.is_dir():
            raise ex.EStorageRequest(f"Storage path is not a directory: STAT [{storage_path}]")

        pattern = "**/*" if recursive else "*"
        paths = list(item_path.glob(pattern))

        return list(map(lambda p: self._os_to_trac_stat(p, p.stat()), paths))

    def mkdir(self, storage_path: str, recursive: bool = False):

        operation = f"MKDIR [{storage_path}]"
        self._error_handling(operation, lambda: self._mkdir(storage_path, recursive))

    def _mkdir(self, storage_path: str, recursive: bool = False):

        item_path = self._resolve_path(storage_path, "MKDIR", False)
        item_path.mkdir(parents=recursive, exist_ok=True)

    def rm(self, storage_path: str):

        operation = f"RM [{storage_path}]"
        self._error_handling(operation, lambda: self._rm(storage_path))

    def _rm(self, storage_path: str):

        item_path = self._resolve_path(storage_path, "RM", False)

        if not item_path.is_file():
            raise ex.EStorageRequest(f"Storage path is not a file: RM [{storage_path}]")

        item_path.unlink()

    def rmdir(self, storage_path: str):

        operation = f"RMDIR [{storage_path}]"
        self._error_handling(operation, lambda: self._rmdir(storage_path))

    def _rmdir(self, storage_path: str):

        item_path = self._resolve_path(storage_path, "RMDIR", False)

        if not item_path.is_dir():
            raise ex.EStorageRequest(f"Storage path is not a directory: RMDIR [{storage_path}]")

        self._rmdir_inner(item_path)

    def _rmdir_inner(self, item_path):

        for item in item_path.iterdir():
            if item.is_dir():
                self._rmdir_inner(item)
            else:
                item.unlink()

        item_path.rmdir()

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (READ) [{storage_path}]"
        return self._error_handling(operation, lambda: self._read_byte_stream(storage_path))

    def _read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        item_path = self._resolve_path(storage_path, "OPEN BYTE STREAM (READ)", False)
        stream = open(item_path, mode='rb')

        return _StreamResource(stream, lambda: self._close_byte_stream(storage_path, stream))

    def write_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        operation = f"OPEN BYTE STREAM (WRITE) [{storage_path}]"
        return self._error_handling(operation, lambda: self._write_byte_stream(storage_path))

    def _write_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        item_path = self._resolve_path(storage_path, "OPEN BYTE STREAM (WRITE)", False)

        delete_on_error = not item_path.exists()

        stream = open(item_path, mode='wb')

        return _StreamResource(stream, lambda: self._close_byte_stream(storage_path, stream, delete_on_error))

    def _close_byte_stream(self, storage_path: str, stream: tp.BinaryIO, delete_on_error: bool = False):

        if stream.closed:
            return

        try:
            read_write = "WRITE" if stream.writable() else "READ"
            self._log.info(f"CLOSE BYTE STREAM ({read_write}) [{storage_path}]")

        finally:
            stream.close()

        exc_info = sys.exc_info()
        error = exc_info[1] if exc_info is not None else None

        if error is not None and delete_on_error:
            try:
                item_path = self._resolve_path(storage_path, "CLOSE BYTE STREAM (WRITE)", False)
                if item_path.exists():
                    item_path.unlink()
            except OSError:
                pass

    __T = tp.TypeVar("__T")

    def _resolve_path(self, storage_path: str, operation_name: str, allow_root_dir: bool) -> pathlib.Path:

        try:

            if storage_path is None or len(storage_path.strip()) == 0:
                raise ex.EStorageValidation(f"Storage path is null or blank: {operation_name} [{storage_path}]")

            if self._ILLEGAL_PATH_CHARS.match(storage_path):
                raise ex.EStorageValidation(f"Storage path is invalid: {operation_name} [{storage_path}]")

            relative_path = pathlib.Path(storage_path)

            if relative_path.is_absolute():
                raise ex.EStorageValidation(f"Storage path is not relative: {operation_name} [{storage_path}]")

            root_path = self._root_path
            absolute_path = self._root_path.joinpath(relative_path).resolve(False)

            if len(absolute_path.parts) < len(root_path.parts) or not absolute_path.is_relative_to(root_path):
                raise ex.EStorageValidation(f"Path is outside storage root: {operation_name} [{storage_path}]")

            if absolute_path == root_path and not allow_root_dir:
                raise ex.EStorageValidation(f"Illegal operation for storage root: {operation_name} [{storage_path}]")

            return absolute_path

        except ValueError as e:

            raise ex.EStorageValidation(f"Storage path is invalid: {operation_name} [{storage_path}]") from e

    def _error_handling(self, operation: str, func: tp.Callable[[], __T]) -> __T:

        try:
            self._log.info(operation)
            return func()

        except FileNotFoundError as e:
            msg = "File not found"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageRequest(msg) from e

        except FileExistsError as e:
            msg = "File already exists"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageRequest(msg) from e

        except IsADirectoryError as e:
            msg = "Path is a directory, not a file"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageRequest(msg) from e

        except NotADirectoryError as e:
            msg = "Path is not a directory"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageRequest(msg) from e

        except PermissionError as e:
            msg = "Access denied"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageAccess(msg) from e

        except OSError as e:
            msg = "Filesystem error"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageAccess(msg) from e

    _ILLEGAL_PATH_CHARS = re.compile(r".*[\x00<>:\"\'\\|?*].*")
