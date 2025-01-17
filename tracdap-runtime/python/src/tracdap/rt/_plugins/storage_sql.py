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

import contextlib
import typing as tp
import urllib.parse as urlp

import pyarrow as pa

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins

# Import storage interfaces (private extension API)
from tracdap.rt._impl.ext.storage import *  # noqa
from tracdap.rt._impl.ext.sql import *  # noqa

import tracdap.rt._plugins._helpers as _helpers

# TODO: Remove internal references
import tracdap.rt._impl.core.data as _data


class SqlDataStorage(IDataStorageBase[pa.Table, pa.Schema]):

    DIALECT_PROPERTY = "dialect"
    DRIVER_PROPERTY = "driver.python"

    __DQL_KEYWORDS = ["select"]
    __DML_KEYWORDS = ["insert", "update", "delete", "merge"]
    __DDL_KEYWORDS = ["create", "alter", "drop", "grant"]

    def __init__(self, properties: tp.Dict[str, str]):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

        dialect_name = _helpers.get_plugin_property(self._properties, self.DIALECT_PROPERTY)

        if dialect_name is None:
            raise ex.EConfigLoad(f"Missing required property [{self.DIALECT_PROPERTY}]")

        if not plugins.PluginManager.is_plugin_available(ISqlDialect, dialect_name.lower()):
            raise ex.EPluginNotAvailable(f"SQL dialect [{dialect_name}] is not supported")

        driver_name = _helpers.get_plugin_property(self._properties, self.DRIVER_PROPERTY)
        if driver_name is None:
            driver_name = dialect_name.lower()

        if not plugins.PluginManager.is_plugin_available(ISqlDriver, driver_name):
            raise ex.EPluginNotAvailable(f"SQL driver [{driver_name}] is not available")

        driver_props = self._driver_props(driver_name)
        driver_cfg = cfg.PluginConfig(protocol=driver_name.lower(), properties=driver_props)
        dialect_cfg = cfg.PluginConfig(protocol=dialect_name.lower(), properties={})

        self._log.info(f"Loading SQL driver [{driver_name}] for dialect [{dialect_name}]")

        self._driver = plugins.PluginManager.load_plugin(ISqlDriver, driver_cfg)
        self._dialect = plugins.PluginManager.load_plugin(ISqlDialect, dialect_cfg)

        # Test connectivity
        with self._connection():
            pass

    def _driver_props(self, driver_name: str) -> tp.Dict[str, str]:

        driver_props = dict()
        driver_filter = f"{driver_name}."

        for key, value in self._properties.items():
            if key.startswith(driver_filter):
                dialect_key = key[len(driver_filter):]
                driver_props[dialect_key] = value

        return driver_props

    def _connection(self) -> DbApiWrapper.Connection:

        return contextlib.closing(self._driver.connect())  # noqa

    def _cursor(self, conn: DbApiWrapper.Connection) -> DbApiWrapper.Cursor:

        return contextlib.closing(conn.cursor())  # noqa

    def data_type(self) -> tp.Type[pa.Table]:
        return pa.Table

    def schema_type(self) -> tp.Type[pa.Schema]:
        return pa.Schema

    def has_table(self, table_name: str):

        with self._driver.error_handling():
            return self._driver.has_table(table_name)

    def list_tables(self):

        with self._driver.error_handling():
            return self._driver.list_tables()

    def create_table(self, table_name: str, schema: pa.Schema):

        with self._driver.error_handling():

            def type_decl(field: pa.Field):
                sql_type = self._dialect.arrow_to_sql_type(field.type)
                null_qualifier = " NULL" if field.nullable else " NOT NULL"
                return f"{field.name} {sql_type}{null_qualifier}"

            create_fields = map(lambda i: type_decl(schema.field(i)), range(len(schema.names)))
            create_stmt = f"create table {table_name} (" + ", ".join(create_fields) + ")"

            with self._connection() as conn, self._cursor(conn) as cur:
                cur.execute(create_stmt, [])
                conn.commit()  # Some drivers / dialects (Postgres) require commit for create table

    def read_table(self, table_name: str) -> pa.Table:

        select_stmt = f"select * from {table_name}"  # noqa

        return self.native_read_query(select_stmt)

    def native_read_query(self, query: str, **parameters) -> pa.Table:

        # Real restrictions are enforced in deployment, by permissions granted to service accounts
        # This is a sanity check to catch common errors before sending a query to the backend
        self._check_read_query(query)

        with self._driver.error_handling():

            with self._connection() as conn, self._cursor(conn) as cur:

                cur.execute(query, parameters)
                sql_batch = cur.fetchmany()

                # Read queries should always return a result set, even if it is empty
                if not cur.description:
                    raise ex.EStorage(f"Query did not return a result set: {query}")

                arrow_schema = self._decode_sql_schema(cur.description)
                arrow_batches: tp.List[pa.RecordBatch] = []

                while len(sql_batch) > 0:

                    arrow_batch = self._decode_sql_batch(arrow_schema, sql_batch)
                    arrow_batches.append(arrow_batch)

                    # Sometimes the schema is not fully defined up front (because cur.description is not sufficient)
                    # If type information has been inferred from the batch, update the schema accordingly
                    arrow_schema = arrow_batch.schema

                    sql_batch = cur.fetchmany()

                return pa.Table.from_batches(arrow_batches, arrow_schema)  # noqa

    def write_table(self, table_name: str, table: pa.Table):

        with self._driver.error_handling():

            insert_fields = ", ".join(table.schema.names)
            insert_markers = ", ".join(f":{name}" for name in table.schema.names)
            insert_stmt = f"insert into {table_name}({insert_fields}) values ({insert_markers})"  # noqa

            with self._connection() as conn:

                # Use execute many to perform a batch write
                with self._cursor(conn) as cur:
                    if table.num_rows > 0:
                        # Provider converts rows on demand, to optimize for memory
                        row_provider = self._encode_sql_rows_dict(table)
                        cur.executemany(insert_stmt, row_provider)
                    else:
                        # Do not try to insert if there are now rows to bind
                        pass

                conn.commit()

    def _check_read_query(self, query):

        if not any(map(lambda keyword: keyword in query.lower(), self.__DQL_KEYWORDS)):
            raise ex.EStorageRequest(f"Query is not a read query: {query}")

        if any(map(lambda keyword: keyword in query.lower(), self.__DML_KEYWORDS)):
            raise ex.EStorageRequest(f"Query is not a read query: {query}")

        if any(map(lambda keyword: keyword in query.lower(), self.__DDL_KEYWORDS)):
            raise ex.EStorageRequest(f"Query is not a read query: {query}")

    @staticmethod
    def _decode_sql_schema(description: tp.List[tp.Tuple]):

        # TODO: Infer Python / Arrow type using DB API type code
        # These codes are db-specific so decoding would probably be on a best effort basis
        # However the information is public for many popular db engines
        # The current logic can be kept as a fallback (set type info on reading first non-null value)

        def _decode_sql_field(field_desc: tp.Tuple):
            field_name, type_code, _, _, precision, scale, null_ok = field_desc
            return pa.field(field_name, pa.null(), null_ok)

        fields = map(_decode_sql_field, description)

        return pa.schema(fields)

    def _decode_sql_batch(self, schema: pa.Schema, sql_batch: tp.List[tp.Tuple]) -> pa.RecordBatch:

        py_dict: tp.Dict[str, pa.Array] = {}

        for i, col in enumerate(schema.names):

            arrow_type = schema.types[i]

            if pa.types.is_null(arrow_type):
                values = list(map(lambda row: row[i], sql_batch))
                concrete_value = next(v for v in values if v is not None)
                if concrete_value is not None:
                    arrow_type = _data.DataMapping.python_to_arrow_type(type(concrete_value))
                    arrow_field = pa.field(schema.names[i], arrow_type, nullable=True)
                    schema = schema.remove(i).insert(i, arrow_field)
            else:
                python_type = _data.DataMapping.arrow_to_python_type(arrow_type)
                values = map(lambda row: self._driver.decode_sql_value(row[i], python_type), sql_batch)

            py_dict[col] = pa.array(values, type=arrow_type)

        return pa.RecordBatch.from_pydict(py_dict, schema)

    def _encode_sql_rows_tuple(self, table: pa.Table) -> tp.Iterator[tp.Tuple]:

        for row in range(0, table.num_rows):
            row_values = map(lambda col: self._driver.encode_sql_value(col[row].as_py()), table.columns)
            yield tuple(row_values)

    def _encode_sql_rows_dict(self, table: pa.Table) -> tp.Iterator[tp.Tuple]:

        for row in range(0, table.num_rows):
            row_values = map(lambda col: self._driver.encode_sql_value(col[row].as_py()), table.columns)
            yield dict(zip(table.column_names, row_values))


class SqlStorageProvider(IStorageProvider):

    def __init__(self, properties: tp.Dict[str, str]):
        self._properties = properties

    def has_data_storage(self) -> bool:
        return True

    def get_data_storage(self) -> IDataStorageBase:
        return SqlDataStorage(self._properties)


# Register with the plugin manager
plugins.PluginManager.register_plugin(IStorageProvider, SqlStorageProvider, ["SQL"])


try:
    import sqlalchemy as sqla  # noqa
    import sqlalchemy.exc as sqla_exc  # noqa

    # Only 2.x versions of SQL Alchemy are currently supported
    sqla_supported = sqla.__version__.startswith("2.")

except ModuleNotFoundError:
    sqla = None
    sqla_supported = False

if sqla_supported:

    class SqlAlchemyDriver(ISqlDriver):

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

            return SqlAlchemyDriver.ConnectionWrapper(self.__engine.connect())

        def has_table(self, table_name: str):

            with self.__engine.connect() as conn:
                inspection = sqla.inspect(conn)
                return inspection.has_table(table_name)

        def list_tables(self):

            with self.__engine.connect() as conn:
                inspection = sqla.inspect(conn)
                return inspection.get_table_names()

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

            def __init__(self, conn: "sqla.Connection"):
                self.__conn = conn

            def close(self):
                self.__conn.close()

            def commit(self):
                self.__conn.commit()

            def rollback(self):
                self.__conn.rollback()

            def cursor(self) -> "DbApiWrapper.Cursor":
                return SqlAlchemyDriver.CursorWrapper(self.__conn)

        class CursorWrapper(DbApiWrapper.Cursor):

            arraysize: int = 1000

            def __init__(self, conn: "sqla.Connection"):
                self.__conn = conn
                self.__result: tp.Optional[sqla.CursorResult] = None

            @property
            def description(self):

                # Prefer description from the underlying cursor if available
                if self.__result.cursor is not None and self.__result.cursor.description:
                    return self.__result.cursor.description

                if not self.__result.returns_rows:
                    return None

                # SQL Alchemy sometimes closes the cursor and the description is lost
                # Fall back on using the Result API to generate a description with field names only

                def name_only_field_desc(field_name):
                    return field_name, None, None, None, None, None, None

                return list(map(name_only_field_desc, self.__result.keys()))

            @property
            def rowcount(self) -> int:

                # Prefer the value from the underlying cursor if it is available
                if self.__result.cursor is not None:
                    return self.__result.cursor.rowcount

                return self.__result.rowcount  # noqa

            def execute(self, statement: str, parameters: tp.Union[tp.Dict, tp.Sequence]):

                self.__result = self.__conn.execute(sqla.text(statement), parameters)

            def executemany(self, statement: str, parameters: tp.Iterable[tp.Union[tp.Dict, tp.Sequence]]):

                if not isinstance(parameters, tp.List):
                    parameters = list(parameters)

                self.__result = self.__conn.execute(sqla.text(statement), parameters)

            def fetchone(self) -> tp.Tuple:

                row = self.__result.fetchone()
                return row.tuple() if row is not None else None

            def fetchmany(self, size: int = arraysize) -> tp.Sequence[tp.Tuple]:

                sqla_rows = self.__result.fetchmany(self.arraysize)
                return list(map(sqla.Row.tuple, sqla_rows))  # noqa

            def close(self):

                if self.__result is not None:
                    self.__result.close()

    plugins.PluginManager.register_plugin(ISqlDriver, SqlAlchemyDriver, ["alchemy"])


