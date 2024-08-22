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

import pyarrow as pa

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins

import tracdap.rt._impl.data as data

# Import storage interfaces
from tracdap.rt.ext.storage import *
from tracdap.rt.ext.sql import *

import tracdap.rt._plugins._helpers as _helpers


class SqlDataStorage(IDataStorage):

    BETA_PROPERTY = "trac_beta_sql_storage"
    DIALECT_PROPERTY = "dialect"

    def __init__(self, properties: tp.Dict[str, str], options: dict = None):

        trac_beta_sql_storage = _helpers.get_plugin_property_boolean(properties, self.BETA_PROPERTY)
        if not trac_beta_sql_storage:
            raise ex.EPluginNotAvailable(f"SQL storage is currently in beta, set [{self.BETA_PROPERTY}] to enable")

        self._properties = properties

        dialect = self._properties.get(self.DIALECT_PROPERTY) or ""
        dialect_cfg = cfg.PluginConfig(protocol=dialect, properties=self._dialect_properties())

        if not plugins.PluginManager.is_plugin_available(ISqlDialect, dialect):
            raise ex.EPluginNotAvailable(f"SQL dialect [{dialect}] is not supported")

        self._dialect = plugins.PluginManager.load_plugin(ISqlDialect, dialect_cfg)

        # Test connectivity
        with self._connection():
            pass

    def read_table(
            self, storage_path: str, storage_format: str,
            schema: tp.Optional[pa.Schema],
            storage_options: tp.Dict[str, tp.Any] = None) -> pa.Table:

        field_names = ", ".join(schema.names)
        select_stmt = f"select {field_names} from {storage_path}"

        arrow_batches: tp.List[pa.RecordBatch] = []

        with self._connection() as conn, self._cursor(conn) as cur:

            cur.execute(select_stmt, [])
            sql_batch = cur.fetchmany()

            while len(sql_batch) > 0:

                arrow_batch = self._decode_sql_batch(schema, sql_batch)
                arrow_batches.append(arrow_batch)

                sql_batch = cur.fetchmany()

        return pa.Table.from_batches(arrow_batches, schema)  # noqa

    def write_table(
            self, storage_path: str, storage_format: str,
            table: pa.Table,
            storage_options: tp.Dict[str, tp.Any] = None,
            overwrite: bool = False):

        insert_fields = ", ".join(table.schema.names)
        insert_markers = ", ".join("?" for _ in table.schema.names)
        insert_stmt = f"insert into {storage_path}({insert_fields}) values ({insert_markers})"

        with self._connection() as conn:

            self._create_table_if_missing(conn, storage_path, table, storage_options)

            # Provider converts rows on demand, to optimize for memory
            row_provider = self._encode_sql_rows_iter(table)

            # Use execute many to perform a batch write
            with self._cursor(conn) as cur:
                cur.executemany(insert_stmt, row_provider)

            conn.commit()

    def _decode_sql_batch(self, schema: pa.Schema, sql_batch: tp.List[tp.Tuple]) -> pa.RecordBatch:

        py_dict: tp.Dict[str, pa.Array] = {}

        for i, col in enumerate(schema.names):

            arrow_type = schema.types[i]
            python_type = data.DataMapping.arrow_to_python_type(arrow_type)
            values = map(lambda row: self._dialect.decode_sql_value(row[i], python_type), sql_batch)

            py_dict[col] = pa.array(values, type=arrow_type)

        return pa.RecordBatch.from_pydict(py_dict, schema)

    def _encode_sql_rows_iter(self, table: pa.Table) -> tp.Iterator[tp.Tuple]:

        for row in range(0, table.num_rows):
            yield tuple(map(lambda col: self._dialect.encode_sql_value(col[row].as_py()), table.columns))

    def _create_table_if_missing(
            self, conn: DbApiWrapper.Connection,
            storage_path: str, table: pa.Table,
            storage_options: tp.Dict[str, tp.Any] = None):

        try:
            with self._cursor(conn) as cur:
                cur.execute(f"select 0 from {storage_path} where 0 = 1")
            return

        except Exception:
            pass

        def type_decl(field: pa.Field):
            sql_type = self._dialect.arrow_to_sql_type(field.type)
            null_qualifier = "NULL" if field.nullable else "NOT NULL"
            return f"{field.name} {sql_type} {null_qualifier}"

        create_fields = map(lambda i: type_decl(table.field(i)), range(table.num_columns))
        create_stmt = f"create table {storage_path} (\n" + ",\n".join(create_fields) + "\n)"

        with self._cursor(conn) as cur:
            cur.execute(create_stmt, [])

    def _connection(self) -> DbApiWrapper.Connection:

        return contextlib.closing(self._dialect.connect())  # noqa

    def _cursor(self, conn: DbApiWrapper.Connection) -> DbApiWrapper.Cursor:

        return contextlib.closing(conn.cursor())  # noqa

    def _dialect_properties(self) -> tp.Dict[str, str]:

        dialect_props = dict()

        dialect = self._properties.get(self.DIALECT_PROPERTY)
        dialect_filter = dialect + "."

        for key, value in self._properties.items():
            if key.startswith(dialect_filter):
                dialect_key = key[len(dialect_filter):]
                dialect_props[dialect_key] = value

        return dialect_props


class SqlStorageProvider(IStorageProvider):

    def __init__(self, properties: tp.Dict[str, str]):
        self._properties = properties

    def has_data_storage(self) -> bool:

        return True

        # dialect = self._properties.get(self.DIALECT_PROPERTY) or ""
        # dbapi = self.__DIALECTS.get(dialect)
        # return dbapi is not None

    def get_data_storage(self) -> IDataStorage:
        return SqlDataStorage(self._properties)


# Register with the plugin manager
plugins.PluginManager.register_plugin(IStorageProvider, SqlStorageProvider, ["SQL"])
