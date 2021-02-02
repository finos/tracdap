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

import trac.rt.config as _cfg


class IFileStorage:

    @abc.abstractmethod
    def exists(self, relative_path: str) -> bool:
        pass

    @abc.abstractmethod
    def stat(self, relative_path: str) -> object:  # TODO: Structure of stat return object
        pass

    @abc.abstractmethod
    def ls(self, relative_path: str) -> tp.List[str]:
        pass

    @abc.abstractmethod
    def mkdir(self, relative_path: str, recursive: bool = False):
        pass

    @abc.abstractmethod
    def rm(self, relative_path: str, recursive: bool = False):
        pass

    @abc.abstractmethod
    def read_bytes(self, relative_path: str):
        pass

    @abc.abstractmethod
    def write_bytes(self, relative_path: str):
        pass


class IDataStorage:

    @abc.abstractmethod
    def read_table(self):
        pass

    @abc.abstractmethod
    def write_table(self):
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

    def __init__(self, sys_config: _cfg.RuntimeConfig):

        self.__file_storage: tp.Dict[str, IFileStorage] = dict()
        self.__data_storage: tp.Dict[str, IDataStorage] = dict()

        for storage_key, storage_config in sys_config.storage.items():
            self.create_storage(storage_key, storage_config)

    def create_storage(self, storage_key: str, storage_config: _cfg.StorageConfig):

        storage_type = storage_config.storageType

        file_impl = self.__file_impls.get(storage_type)
        data_impl = self.__data_impls.get(storage_type)

        file_storage = file_impl(storage_config)
        data_storage = data_impl(storage_config)

        self.__file_storage[storage_key] = file_storage
        self.__data_storage[storage_key] = data_storage

    def get_file_storage(self, storage_key: str) -> IFileStorage:

        return self.__file_storage[storage_key]

    def get_data_storage(self, storage_key: str) -> IDataStorage:

        return self.__data_storage[storage_key]


# ----------------------------------------------------------------------------------------------------------------------
# LOCAL STORAGE IMPLEMENTATION
# ----------------------------------------------------------------------------------------------------------------------


class LocalFileStorage(IFileStorage):

    def __init__(self, config: _cfg.StorageConfig):

        root_path = config.storageConfig.get("rootPath")  # TODO: Config / constants
        self.__root_path = pathlib.Path(root_path).resolve(strict=True)

    def exists(self, relative_path: str) -> bool:

        item_path = self.__root_path / relative_path
        return item_path.exists()

    def stat(self, relative_path: str) -> object:

        item_path = self.__root_path / relative_path
        return item_path.stat()

    def ls(self, relative_path: str) -> tp.List[str]:

        item_path = self.__root_path / relative_path
        return [str(x.relative_to(self.__root_path))
                for x in item_path.iterdir()
                if x.is_file() or x.is_dir()]

    def mkdir(self, relative_path: str, recursive: bool = False):

        item_path = self.__root_path / relative_path
        item_path.mkdir(parents=recursive, exist_ok=recursive)

    def rm(self, relative_path: str, recursive: bool = False):

        raise NotImplementedError()

    def read_bytes(self, relative_path: str):
        pass

    def write_bytes(self, relative_path: str):
        pass


class LocalDataStorage(IDataStorage):

    def __init__(self, config: _cfg.StorageConfig):
        pass

    def read_table(self):
        pass

    def write_table(self):
        pass

    def query_table(self):
        pass


StorageManager.register_storage_type("LOCAL_STORAGE", LocalFileStorage, LocalDataStorage)
