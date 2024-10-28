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

# Import storage interfaces (private extension API)
from tracdap.rt._impl.ext.storage import *  # noqa
from tracdap.rt._impl.ext.sql import *  # noqa

import tracdap.rt._plugins._helpers as _helpers

# TODO: Remove internal references
import tracdap.rt._impl.data as _data


class SqlDataStorage(IDataStorageBase[pa.Table, pa.Schema]):

    DIALECT_PROPERTY = "dialect"
    DRIVER_PROPERTY = "driver.python"

    def __init__(self, properties: tp.Dict[str, str], options: dict = None):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

        dialect_name = _helpers.get_plugin_property(self._properties, self.DIALECT_PROPERTY)

        if dialect_name is None:
            raise ex.EConfigLoad(f"Missing required property [{self.DIALECT_PROPERTY}]")

        if not plugins.PluginManager.is_plugin_available(ISqlDialect, dialect_name.lower()):
            raise ex.EPluginNotAvailable(f"SQL dialect [{dialect_name}] is not supported")

        driver_name = _helpers.get_plugin_property(self._properties, self.DRIVER_PROPERTY)
        if driver_name is None:
            driver_name = dialect_name

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
                null_qualifier = "NULL" if field.nullable else "NOT NULL"
                return f"{field.name} {sql_type} {null_qualifier}"

            create_fields = map(lambda i: type_decl(schema.field(i)), range(len(schema.names)))
            create_stmt = f"create table {table_name} (\n" + ",\n".join(create_fields) + "\n)"

            with self._connection() as conn, self._cursor(conn) as cur:
                cur.execute(create_stmt, [])

    def read_table(self, table_name: str) -> pa.Table:

        select_stmt = f"select * from {table_name}"

        return self.native_read_query(select_stmt)

    def native_read_query(self, query: str, **parameters) -> pa.Table:

        with self._driver.error_handling():

            with self._connection() as conn, self._cursor(conn) as cur:

                cur.execute(query, parameters)

                result_schema = self._driver.get_result_schema(cur)

                arrow_batches: tp.List[pa.RecordBatch] = []
                sql_batch = cur.fetchmany()

                while len(sql_batch) > 0:

                    arrow_batch = self._decode_sql_batch(result_schema, sql_batch)
                    arrow_batches.append(arrow_batch)

                    result_schema = arrow_batch.schema

                    sql_batch = cur.fetchmany()

            return pa.Table.from_batches(arrow_batches, result_schema)  # noqa

    def write_table(self, table_name: str, table: pa.Table):

        with self._driver.error_handling():

            insert_fields = ", ".join(table.schema.names)
            insert_markers = ", ".join(f":{name}" for name in table.schema.names)
            insert_stmt = f"insert into {table_name}({insert_fields}) values ({insert_markers})"

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
