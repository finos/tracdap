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
import typing as tp
import pathlib

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex

# Import storage interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers

# TODO: Remove dependencies on internal implementation details
import tracdap.rt._impl.storage as _storage


class LocalFileStorage(IFileStorage):

    ROOT_PATH_PROPERTY = "rootPath"

    def __init__(self, config: cfg.PluginConfig, options: dict = None):

        self._log = _helpers.logger_for_object(self)
        self._properties = config.properties

        root_path_config = _helpers.get_plugin_property(self._properties, self.ROOT_PATH_PROPERTY)

        if not root_path_config or root_path_config.isspace():
            err = f"Storage root path not set"
            self._log.error(err)
            raise ex.EStorageRequest(err)

        supplied_root = pathlib.Path(root_path_config)

        if supplied_root.is_absolute():
            absolute_root = supplied_root

        else:
            err = f"Relative path not allowed for storage root [{supplied_root}]"
            self._log.error(err)
            raise ex.EStorageConfig(err)

        try:
            self._root_path = absolute_root.resolve(strict=True)

        except FileNotFoundError as e:
            err = f"Storage root path does not exist: [{absolute_root}]"
            self._log.error(err)
            raise ex.EStorageRequest(err) from e

    def _get_root(self):
        return self._root_path

    def exists(self, storage_path: str) -> bool:

        operation = f"EXISTS [{storage_path}]"
        return self._error_handling(operation, lambda: self._exists(storage_path))

    def _exists(self, storage_path: str) -> bool:

        item_path = self._root_path / storage_path
        return item_path.exists()

    def size(self, storage_path: str) -> int:

        operation = f"SIZE [{storage_path}]"
        return self._error_handling(operation, lambda: self._stat(storage_path).size)

    def stat(self, storage_path: str) -> FileStat:

        operation = f"STAT [{storage_path}]"
        return self._error_handling(operation, lambda: self._stat(storage_path))

    def _stat(self, storage_path: str) -> FileStat:

        item_path = self._root_path / storage_path
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

        item_path = self._root_path / storage_path
        return [str(x.relative_to(item_path))
                for x in item_path.iterdir()
                if x.is_file() or x.is_dir()]

    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        operation = f"MKDIR [{storage_path}]"
        self._error_handling(operation, lambda: self._mkdir(storage_path, recursive, exists_ok))

    def _mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        item_path = self._root_path / storage_path
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
        item_path = self._root_path / storage_path
        stream = open(item_path, mode='rb')

        return _helpers.log_close(stream, self._log, operation)

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
        item_path = self._root_path / storage_path

        if overwrite:
            stream = open(item_path, mode='wb')
        else:
            stream = open(item_path, mode='xb')

        return _helpers.log_close(stream, self._log, operation)

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
        item_path = self._root_path / storage_path
        stream = open(item_path, mode='rt', encoding=encoding)

        return _helpers.log_close(stream, self._log, operation)

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
        item_path = self._root_path / storage_path

        if overwrite:
            stream = open(item_path, mode='wt', encoding=encoding)
        else:
            stream = open(item_path, mode='xt', encoding=encoding)

        return _helpers.log_close(stream, self._log, operation)

    __T = tp.TypeVar("__T")

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

        except PermissionError as e:
            msg = "Access denied"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageAccess(msg) from e

        except OSError as e:
            msg = "Filesystem error"
            self._log.exception(f"{operation}: {msg}")
            raise ex.EStorageAccess(msg) from e


_storage.StorageManager.register_storage_type("LOCAL", LocalFileStorage, _storage.CommonDataStorage)
