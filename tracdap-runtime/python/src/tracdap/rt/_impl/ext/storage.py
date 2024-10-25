#  Copyright 2024 Accenture Global Solutions Limited
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

from tracdap.rt.ext.storage import *  # noqa


T_DATA = tp.TypeVar("T_DATA")
T_SCHEMA = tp.TypeVar("T_SCHEMA")


class IDataStorageBase(tp.Generic[T_DATA, T_SCHEMA], abc.ABC):

    @abc.abstractmethod
    def data_type(self) -> tp.Type[T_DATA]:
        pass

    @abc.abstractmethod
    def schema_type(self) -> tp.Type[T_SCHEMA]:
        pass

    @abc.abstractmethod
    def has_table(self, table_name: str) -> bool:
        pass

    @abc.abstractmethod
    def list_tables(self) -> tp.List[str]:
        pass

    @abc.abstractmethod
    def create_table(self, table_name: str, schema: T_SCHEMA):
        pass

    @abc.abstractmethod
    def read_table(self, table_name: str) -> T_DATA:
        pass

    @abc.abstractmethod
    def write_table(self, table_name: str, records: T_DATA):
        pass

    @abc.abstractmethod
    def native_read_query(self, query: str, **parameters) -> T_DATA:
        pass
