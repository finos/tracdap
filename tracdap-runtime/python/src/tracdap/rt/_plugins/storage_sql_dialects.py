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

import typing as tp

import pyarrow as pa

import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins

from tracdap.rt._impl.ext.sql import *  # noqa


class MySqlDialect(ISqlDialect):

    def __init__(self, properties: tp.Dict[str, str]):
        self._properties = properties

    def arrow_to_sql_type(self, arrow_type: pa.DataType) -> str:

        if pa.types.is_boolean(arrow_type):
            return "bit"

        if pa.types.is_integer(arrow_type):
            return "bigint"

        if pa.types.is_floating(arrow_type):
            return "double"

        if pa.types.is_decimal(arrow_type):
            return "decimal (31, 10)"

        if pa.types.is_string(arrow_type):
            return "varchar(65535)"

        if pa.types.is_date(arrow_type):
            return "date"

        if pa.types.is_timestamp(arrow_type):
            return "timestamp (6)"

        raise ex.ETracInternal(f"Unsupported data type [{str(arrow_type)}] in SQL dialect [{self.__class__.__name__}]")


class MariaDbDialect(MySqlDialect):

    def __init__(self, properties: tp.Dict[str, str]):
        super().__init__(properties)

    # Inherit MySQL implementation
    pass


class PostgresqlDialect(ISqlDialect):

    def __init__(self, properties: tp.Dict[str, str]):
        self._properties = properties

    def arrow_to_sql_type(self, arrow_type: pa.DataType) -> str:

        if pa.types.is_boolean(arrow_type):
            return "boolean"

        if pa.types.is_integer(arrow_type):
            return "bigint"

        if pa.types.is_floating(arrow_type):
            return "double precision"

        if pa.types.is_decimal(arrow_type):
            return "decimal (31, 10)"

        if pa.types.is_string(arrow_type):
            return "varchar"

        if pa.types.is_date(arrow_type):
            return "date"

        if pa.types.is_timestamp(arrow_type):
            return "timestamp (6)"

        raise ex.ETracInternal(f"Unsupported data type [{str(arrow_type)}] in SQL dialect [{self.__class__.__name__}]")


class SqlServerDialect(ISqlDialect):

    def __init__(self, properties: tp.Dict[str, str]):
        self._properties = properties

    def arrow_to_sql_type(self, arrow_type: pa.DataType) -> str:

        if pa.types.is_boolean(arrow_type):
            return "bit"

        if pa.types.is_integer(arrow_type):
            return "bigint"

        if pa.types.is_floating(arrow_type):
            return "float(53)"

        if pa.types.is_decimal(arrow_type):
            return "decimal (31, 10)"

        if pa.types.is_string(arrow_type):
            return "varchar(8000)"

        if pa.types.is_date(arrow_type):
            return "date"

        if pa.types.is_timestamp(arrow_type):
            return "datetime2"

        raise ex.ETracInternal(f"Unsupported data type [{str(arrow_type)}] in SQL dialect [{self.__class__.__name__}]")


plugins.PluginManager.register_plugin(ISqlDialect, MySqlDialect, ["mysql"])
plugins.PluginManager.register_plugin(ISqlDialect, MariaDbDialect, ["mariadb"])
plugins.PluginManager.register_plugin(ISqlDialect, PostgresqlDialect, ["postgresql"])
plugins.PluginManager.register_plugin(ISqlDialect, SqlServerDialect, ["sqlserver"])