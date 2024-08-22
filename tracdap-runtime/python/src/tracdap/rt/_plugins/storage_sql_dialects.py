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

import datetime as dt
import decimal
import math
import typing as tp

import pyarrow as pa

import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins

from tracdap.rt.ext.sql import *


try:
    import sqlite3

    sqlite3.register_adapter(dt.date, lambda d: d.isoformat())
    sqlite3.register_adapter(dt.datetime, lambda d: d.isoformat())

    class SqliteDialect(ISqlDialect):

        __CONNECT_PROPERTIES = {
            "database": str,
            "timeout": float,
            "cached_statements": int,
            "uri": bool
        }

        def __init__(self, properties: tp.Dict[str, str]):
            self._properties = properties

        def connect(self) -> DbApiWrapper.Connection:

            # TODO: Typed properties common function

            allowed_props = filter(lambda kv: kv[0] in self.__CONNECT_PROPERTIES, self._properties.items())
            typed_props = map(lambda kv: (kv[0], self.__CONNECT_PROPERTIES[kv[0]](kv[1])), allowed_props)

            return sqlite3.connect(**dict(typed_props))

        def arrow_to_sql_type(self, arrow_type: pa.DataType):

            if pa.types.is_boolean(arrow_type):
                return "INTEGER"

            if pa.types.is_integer(arrow_type):
                return "INTEGER"

            if pa.types.is_floating(arrow_type):
                return "REAL"

            if pa.types.is_decimal(arrow_type):
                return "TEXT"

            if pa.types.is_string(arrow_type):
                return "TEXT"

            if pa.types.is_date(arrow_type):
                return "TEXT"

            if pa.types.is_timestamp(arrow_type):
                return "TEXT"

            raise ex.EUnexpected()

        def encode_sql_value(self, py_value: tp.Any) -> tp.Any:

            if isinstance(py_value, float):
                if math.isnan(py_value):
                    return str(math.nan)
                else:
                    return py_value

            if isinstance(py_value, decimal.Decimal):
                return str(py_value)

            return py_value

        def decode_sql_value(self, sql_value: tp.Any, python_type: tp.Type) -> tp.Any:

            if sql_value is None:
                return None

            elif isinstance(sql_value, python_type):
                return sql_value

            elif python_type == bool:
                if sql_value == 1:
                    return True
                if sql_value == 0:
                    return False

            elif python_type == float:
                if isinstance(sql_value, str):
                    return float(sql_value)

            elif python_type == decimal.Decimal:
                return decimal.Decimal(sql_value)

            elif python_type == dt.date:
                return dt.date.fromisoformat(sql_value)

            elif python_type == dt.datetime:
                return dt.datetime.fromisoformat(sql_value)

            return sql_value

    plugins.PluginManager.register_plugin(ISqlDialect, SqliteDialect, ["sqlite"])

except ImportError:
    pass
