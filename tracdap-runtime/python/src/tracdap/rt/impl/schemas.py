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

import pyarrow as pa

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
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
    def load_schema(cls, package: tp.Union[ts.ModuleType, str], schema_file: tp.Union[str, pathlib.Path]) \
            -> _meta.SchemaDefinition:

        if not isinstance(package, ts.ModuleType) and not isinstance(package, str):
            raise RuntimeError()  # TODO package not a module

        csv_format = _storage.FormatManager.get_data_format("text/csv", {"lenient_csv_parser": True})

        with _shim.ShimLoader.open_resource(package, schema_file) as schema_io:  # TODO: err not found

            schema_of_schema = _data.DataMapping.trac_to_arrow_schema(cls.__SCHEMA_OF_SCHEMA)
            schema_data = csv_format.read_table(schema_io, schema_of_schema)

        return cls.decode_schema_data(schema_data)

    @classmethod
    def decode_schema_data(cls, schema_data: pa.Table) -> _meta.SchemaDefinition:

        name_vec: pa.StringArray = schema_data.column(0)
        type_vec: pa.StringArray = schema_data.column(1)
        label_vec: pa.StringArray = schema_data.column(2)
        business_key_vec: pa.BooleanArray = schema_data.column(3)
        categorical_vec: pa.BooleanArray = schema_data.column(4)
        format_code_vec: pa.StringArray = schema_data.column(5)

        field_list = []

        for field_index in range(schema_data.num_rows):

            field_name_val = name_vec[field_index]
            type_name_val: pa.StringScalar = type_vec[field_index]
            label_val = label_vec[field_index]
            business_key_val = business_key_vec[field_index]
            categorical_val = categorical_vec[field_index]
            format_code_val = format_code_vec[field_index]

            field_name = cls.arrow_to_py_string(field_name_val, required=True)
            type_name = cls.arrow_to_py_string(type_name_val, required=True)
            label = cls.arrow_to_py_string(label_val, required=True)
            business_key = cls.arrow_to_py_boolean(business_key_val, default=False)
            categorical = cls.arrow_to_py_boolean(categorical_val, default=False)
            format_code = cls.arrow_to_py_string(format_code_val)

            try:
                if type_name:
                    field_type = _meta.BasicType[type_name.upper()]
                else:
                    field_type = _meta.BasicType.BASIC_TYPE_NOT_SET
            except KeyError:
                field_type = _meta.BasicType.BASIC_TYPE_NOT_SET

            field_schema = _meta.FieldSchema(
                field_name, field_index, field_type, label,
                business_key, categorical, format_code)

            field_list.append(field_schema)

        return _meta.SchemaDefinition(
            schemaType=_meta.SchemaType.TABLE,
            table=_meta.TableSchema(field_list))

    @staticmethod
    def arrow_to_py_string(arrow_value: pa.StringScalar, required: bool = False):

        if arrow_value is not None:
            return arrow_value.as_py()

        if not required:
            return None

        raise _ex.ETrac("Missing required field")  # todo err

    @staticmethod
    def arrow_to_py_boolean(arrow_value: pa.BooleanValue, required: bool = False, default: tp.Optional[bool] = None):

        if arrow_value is not None:
            return arrow_value.as_py()

        if not required:
            return default

        raise _ex.ETrac("Missing required field")  # todo err
