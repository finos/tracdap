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
import enum
import typing as _tp
import contextlib as _cm

import pyarrow as _pa



class ISqlDialect:

    @_abc.abstractmethod
    def arrow_to_sql_type(self, arrow_type: _pa.DataType) -> str:
        pass


class ISqlDriver:

    @_abc.abstractmethod
    def param_style(self) -> "DbApiWrapper.ParamStyle":
        pass

    @_abc.abstractmethod
    def connect(self, **kwargs) -> "DbApiWrapper.Connection":
        pass

    @_abc.abstractmethod
    def has_table(self, table_name: str) -> bool:
        pass

    @_abc.abstractmethod
    def list_tables(self) -> _tp.List[str]:
        pass

    @_abc.abstractmethod
    def error_handling(self) -> _cm.contextmanager:
        pass

    @_abc.abstractmethod
    def encode_sql_value(self, py_value: _tp.Any) -> _tp.Any:
        pass

    @_abc.abstractmethod
    def decode_sql_value(self, sql_value: _tp.Any, python_type: _tp.Type) -> _tp.Any:
        pass


class DbApiWrapper:

    class ThreadSafety(enum.Enum):
        NOT_THREAD_SAFE = 0
        MODULE_SAFE = 1
        CONNECTION_SAFE = 2
        CURSOR_SAFE = 3

    class ParamStyle(enum.Enum):
        QMARK = "qmark"
        NUMERIC = "numeric"
        NAMED = "named"
        FORMAT = "format"
        PYFORMAT = "pyformat"

    class Connection(_tp.Protocol):

        def close(self):
            pass

        def commit(self):
            pass

        def rollback(self):
            pass

        def cursor(self) -> "DbApiWrapper.Cursor":
            pass

    class Cursor(_tp.Protocol):

        arraysize: int = 1

        @property
        def description(self) -> tuple:
            pass

        @property
        def rowcount(self) -> int:
            pass

        def execute(self, operation: str, parameters: _tp.Union[_tp.Dict, _tp.Sequence]):
            pass

        def executemany(self, operation: str, parameters: _tp.Iterable[_tp.Union[_tp.Dict, _tp.Sequence]]):
            pass

        def fetchone(self) -> _tp.Tuple:
            pass

        def fetchmany(self, size: int = arraysize) -> _tp.Sequence[_tp.Tuple]:
            pass

        def close(self):
            pass
