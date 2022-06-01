#  Copyright 2022 Accenture Global Solutions Limited
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

import pathlib
import typing as tp
import types as ts

import tracdap.rt.metadata as _meta
import tracdap.rt.impl.data as _data
import tracdap.rt.impl.storage as _storage
import tracdap.rt.impl.shim as _shim


class SchemaLoader:

    __SCHEMA_OF_SCHEMA = _meta.SchemaDefinition(
        schemaType=_meta.SchemaType.TABLE,
        table=_meta.TableSchema([
            _meta.FieldSchema("field_name", 0, _meta.BasicType.STRING, "Field name", businessKey=True),
            _meta.FieldSchema("field_type", 1, _meta.BasicType.STRING, "Field type", categorical=True),
            _meta.FieldSchema("label", 2, _meta.BasicType.STRING, "Label"),
            _meta.FieldSchema("business_key", 3, _meta.BasicType.BOOLEAN, "Business key flag"),
            _meta.FieldSchema("categorical", 4, _meta.BasicType.BOOLEAN, "Categorical flag"),
            _meta.FieldSchema("format_code", 5, _meta.BasicType.STRING, "Format code"),
        ])
    )

    @classmethod
    def load_schema(cls, package: tp.Union[ts.ModuleType, str], schema_file: tp.Union[str, pathlib.Path]):

        if not isinstance(package, ts.ModuleType) and not isinstance(package, str):
            raise RuntimeError()  # TODO package not a module

        csv_format = _storage.FormatManager.get_data_format("text/csv", {"lenient_csv_parser": True})

        with _shim.ShimLoader.open_resource(package, schema_file) as schema_io:

            schema_of_schema = _data.DataMapping.trac_to_arrow_schema(cls.__SCHEMA_OF_SCHEMA)
            schema_data = csv_format.read_table(schema_io, schema_of_schema)

        print(schema_data.num_columns)
        print(schema_data.num_rows)
        print(schema_data)

        raise RuntimeError("not done yet")

