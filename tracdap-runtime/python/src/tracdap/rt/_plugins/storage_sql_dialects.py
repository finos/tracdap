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

import contextlib
import typing as tp
import urllib.parse as urlp

import pyarrow as pa

import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins

from tracdap.rt._impl.ext.sql import *  # noqa
from . import _helpers


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


try:
    import sqlalchemy as sqla
    import sqlalchemy.exc as sqla_exc

    class AlchemySqlDriver(ISqlDriver):

        def __init__(self, properties: tp.Dict[str, str]):

            self._log = _helpers.logger_for_object(self)

            raw_url = properties.get('url')

            if raw_url is None or raw_url.strip() == '':
                raise ex.EConfigLoad("Missing required property [url] for SQL driver [alchemy]")

            url = urlp.urlparse(raw_url)
            credentials = _helpers.get_http_credentials(url, properties)
            url = _helpers.apply_http_credentials(url, credentials)

            filtered_keys = ["url", "username", "password", "token"]
            filtered_props = dict(kv for kv in properties.items() if kv[0] not in filtered_keys)

            self._log.info("Connecting: %s", _helpers.log_safe_url(url))

            try:
                self.__engine = sqla.create_engine(url.geturl(), **filtered_props)
            except ModuleNotFoundError as e:
                raise ex.EPluginNotAvailable("SQL driver is not available: " + str(e)) from e

        def param_style(self) -> "DbApiWrapper.ParamStyle":
            return DbApiWrapper.ParamStyle.NAMED

        def connect(self, **kwargs) -> "DbApiWrapper.Connection":

            return AlchemySqlDriver.ConnectionWrapper(self.__engine.connect())

        def has_table(self, table_name: str):

            with self.__engine.connect() as conn:
                inspection = sqla.inspect(conn)
                return inspection.has_table(table_name)

        def list_tables(self):

            with self.__engine.connect() as conn:
                inspection = sqla.inspect(conn)
                return inspection.get_table_names()

        def get_result_schema(self, cursor: "DbApiWrapper.Cursor") -> tp.List[tp.Tuple[str, pa.DataType]]:

            if not isinstance(cursor, self.CursorWrapper):
                raise ex.EUnexpected()

            return cursor._get_result_schema()  # noqa

        def encode_sql_value(self, py_value: tp.Any) -> tp.Any:

            return py_value

        def decode_sql_value(self, sql_value: tp.Any, python_type: tp.Type) -> tp.Any:

            return sql_value

        @contextlib.contextmanager
        def error_handling(self) -> contextlib.contextmanager:

            try:
                yield
            except (sqla_exc.OperationalError, sqla_exc.ProgrammingError, sqla_exc.StatementError) as e:
                raise ex.EStorageRequest(*e.args) from e
            except sqla_exc.SQLAlchemyError as e:
                raise ex.EStorage() from e

        class ConnectionWrapper(DbApiWrapper.Connection):

            def __init__(self, conn: sqla.Connection):
                self.__conn = conn

            def close(self):
                self.__conn.close()

            def commit(self):
                self.__conn.commit()

            def rollback(self):
                self.__conn.rollback()

            def cursor(self) -> "DbApiWrapper.Cursor":
                return AlchemySqlDriver.CursorWrapper(self.__conn)

        class CursorWrapper(DbApiWrapper.Cursor):

            arraysize: int = 1000

            def __init__(self, conn: sqla.Connection):
                self.__conn = conn
                self.__result: tp.Optional[sqla.CursorResult] = None

            def execute(self, statement: str, parameters: tp.Union[tp.Dict, tp.Sequence]):

                self.__result = self.__conn.execute(sqla.text(statement), parameters)

            def executemany(self, statement: str, parameters: tp.Iterable[tp.Union[tp.Dict, tp.Sequence]]):

                if not isinstance(parameters, tp.List):
                    parameters = list(parameters)

                self.__result = self.__conn.execute(sqla.text(statement), parameters)

            def fetchone(self) -> tp.Tuple:

                return self.__result.fetchone().tuple()

            def fetchmany(self, size: int = arraysize) -> tp.List[tp.Tuple]:

                sqla_rows = self.__result.fetchmany(self.arraysize)
                return list(map(sqla.Row.tuple, sqla_rows))  # noqa

            def _get_result_schema(self) -> pa.Schema:

                column_names = self.__result.keys()
                fields = list(map(lambda c: pa.field(c, pa.null(), nullable=True), column_names))

                return pa.schema(fields)

            def close(self):

                if self.__result is not None:
                    self.__result.close()

    plugins.PluginManager.register_plugin(ISqlDriver, AlchemySqlDriver, ["alchemy"])

except ModuleNotFoundError:
    pass
