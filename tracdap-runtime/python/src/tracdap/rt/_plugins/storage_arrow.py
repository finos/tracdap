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

import typing as tp

# Import storage interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

import pyarrow.fs as afs


class ArrowFileSystem(IFileStorage):

    def __init__(self, fs_impl: afs.FileSystem, root_path: str):
        self._fs = afs.SubTreeFileSystem(root_path, fs_impl)

    def exists(self, storage_path: str) -> bool:
        absolute_path = storage_path
        file_info: afs.FileInfo = self._fs.get_file_info(absolute_path)
        return file_info.type != afs.FileType.NotFound

    def size(self, storage_path: str) -> int:
        absolute_path = storage_path
        file_info: afs.FileInfo = self._fs.get_file_info(absolute_path)
        return file_info.size

    def stat(self, storage_path: str) -> FileStat:
        absolute_path = storage_path
        file_info: afs.FileInfo = self._fs.get_file_info(absolute_path)

        file_type = FileType.FILE if file_info.is_file else FileType.DIRECTORY

        return FileStat(
            file_type, file_info.size,
            ctime=file_info.mtime,
            mtime=file_info.mtime,
            atime=None)

    def ls(self, storage_path: str) -> tp.List[str]:
        absolute_path = storage_path
        selector = afs.FileSelector(absolute_path, recursive=True)  # noqa
        file_infos: tp.List[afs.FileInfo] = self._fs.get_file_info(selector)
        return list(map(lambda fi: fi.base_name, file_infos))

    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):
        absolute_path = storage_path
        self._fs.create_dir(absolute_path, recursive or exists_ok)

    def rm(self, storage_path: str, recursive: bool = False):
        absolute_path = storage_path
        if recursive:
            self._fs.delete_dir(absolute_path)
        else:
            self._fs.delete_file(absolute_path)

    def read_bytes(self, storage_path: str) -> bytes:

        with self.read_byte_stream(storage_path) as stream:
            return stream.read()

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:
        absolute_path = storage_path
        return self._fs.open_input_stream(absolute_path)

    def write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):

        with self.write_byte_stream(storage_path, overwrite) as stream:
            stream.write(data)

    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:
        absolute_path = storage_path
        return self._fs.open_output_stream(absolute_path)

    def read_text(self, storage_path: str, encoding: str = 'utf-8') -> str:
        raise RuntimeError("READ (text mode) not available")

    def read_text_stream(self, storage_path: str, encoding: str = 'utf-8') -> tp.TextIO:
        raise RuntimeError("READ (text mode) not available")

    def write_text(self, storage_path: str, data: str, encoding: str = 'utf-8', overwrite: bool = False):
        raise RuntimeError("WRITE (text mode) not available")

    def write_text_stream(self, storage_path: str, encoding: str = 'utf-8', overwrite: bool = False) -> tp.TextIO:
        raise RuntimeError("WRITE (text mode) not available")


class LocalFileSystemProvider(IFileSystemProvider):

    def __init__(self, properties: tp.Dict[str, str]):
        pass

    def prepare_file_system(self) -> afs.FileSystem:
        return afs.LocalFileSystem()


class S3FileSystemProvider(IFileSystemProvider):

    def __init__(self, properties: tp.Dict[str, str]):
        pass

    def prepare_file_system(self) -> afs.FileSystem:
        return afs.S3FileSystem()


class GcsFileSystemProvider(IFileSystemProvider):

    def __init__(self, properties: tp.Dict[str, str]):
        pass

    def prepare_file_system(self) -> afs.FileSystem:
        return afs.GcsFileSystem()


plugins.PluginManager.register_plugin(IFileSystemProvider, LocalFileSystemProvider, ["LOCAL", "file"])
plugins.PluginManager.register_plugin(IFileSystemProvider, S3FileSystemProvider, ["S3"])
plugins.PluginManager.register_plugin(IFileSystemProvider, GcsFileSystemProvider, ["GCS"])
