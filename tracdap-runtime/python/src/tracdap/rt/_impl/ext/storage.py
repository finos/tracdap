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

import abc as _abc
import typing as _tp

from tracdap.rt.ext.storage import *  # noqa


T_DATA = _tp.TypeVar("T_DATA")
T_SCHEMA = _tp.TypeVar("T_SCHEMA")


class IDataStorageBase(_tp.Generic[T_DATA, T_SCHEMA], _abc.ABC):

    @_abc.abstractmethod
    def data_type(self) -> _tp.Type[T_DATA]:
        pass

    @_abc.abstractmethod
    def schema_type(self) -> _tp.Type[T_SCHEMA]:
        pass

    @_abc.abstractmethod
    def has_table(self, table_name: str) -> bool:
        pass

    @_abc.abstractmethod
    def list_tables(self) -> _tp.List[str]:
        pass

    @_abc.abstractmethod
    def create_table(self, table_name: str, schema: T_SCHEMA):
        pass

    @_abc.abstractmethod
    def read_table(self, table_name: str) -> T_DATA:
        pass

    @_abc.abstractmethod
    def write_table(self, table_name: str, records: T_DATA):
        pass

    @_abc.abstractmethod
    def native_read_query(self, query: str, **parameters) -> T_DATA:
        pass
