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

import logging
import typing as tp
import types as ts

import pyarrow as pa

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.data as _data
import tracdap.rt._impl.storage as _storage
import tracdap.rt._impl.shim as _shim
import tracdap.rt._impl.util as _util


class SchemaLoader:

    _log: logging.Logger

    __SCHEMA_OF_SCHEMA = _meta.SchemaDefinition(
        schemaType=_meta.SchemaType.TABLE,
        table=_meta.TableSchema([
            _meta.FieldSchema("field_name", 0, _meta.BasicType.STRING, "Field name", businessKey=True, notNull=True),
            _meta.FieldSchema("field_type", 1, _meta.BasicType.STRING, "Field type", categorical=True, notNull=True),
            _meta.FieldSchema("label", 2, _meta.BasicType.STRING, "Label", notNull=True),
            _meta.FieldSchema("business_key", 3, _meta.BasicType.BOOLEAN, "Business key flag"),
            _meta.FieldSchema("categorical", 4, _meta.BasicType.BOOLEAN, "Categorical flag"),
            _meta.FieldSchema("not_null", 5, _meta.BasicType.BOOLEAN, "Not null flag"),
            _meta.FieldSchema("format_code", 6, _meta.BasicType.STRING, "Format code"),
        ])
    )

    @classmethod
    def load_schema(cls, package: tp.Union[ts.ModuleType, str], schema_file: str) \
            -> _meta.SchemaDefinition:

        try:

            csv_options = {"lenient_csv_parser": True, "lenient_missing_columns": True}
            csv_format = _storage.FormatManager.get_data_format("text/csv", csv_options)

            # Any resource loading failure will raise EModelRepoResource
            with _shim.ShimLoader.open_resource(package, schema_file) as schema_io:

                schema_of_schema = _data.DataMapping.trac_to_arrow_schema(cls.__SCHEMA_OF_SCHEMA)
                schema_data = csv_format.read_table(schema_io, schema_of_schema)
                schema_data = _data.DataConformance.conform_to_schema(schema_data, schema_of_schema)

            return cls._decode_schema_data(schema_data)

        except _ex.EData as e:

            err = f"Invalid schema file [{schema_file}]: {str(e)}"
            cls._log.exception(err)
            raise _ex.ERuntimeValidation(err) from e

    @classmethod
    def _decode_schema_data(cls, schema_data: pa.Table) -> _meta.SchemaDefinition:

        name_vec: pa.StringArray = schema_data.column(0)
        type_vec: pa.StringArray = schema_data.column(1)
        label_vec: pa.StringArray = schema_data.column(2)
        business_key_vec: pa.BooleanArray = schema_data.column(3)
        categorical_vec: pa.BooleanArray = schema_data.column(4)
        not_null_vec: pa.BooleanArray = schema_data.column(5)
        format_code_vec: pa.StringArray = schema_data.column(6)

        field_list = []

        for field_index in range(schema_data.num_rows):

            field_name_val = name_vec[field_index]
            type_name_val: pa.StringScalar = type_vec[field_index]
            label_val = label_vec[field_index]
            business_key_val = business_key_vec[field_index]
            categorical_val = categorical_vec[field_index]
            not_null_val = not_null_vec[field_index]
            format_code_val = format_code_vec[field_index]

            field_name = cls._arrow_to_py_string("field_name", None, field_index, field_name_val, required=True)
            type_name = cls._arrow_to_py_string("field_type", field_name, field_index, type_name_val, required=True)
            label = cls._arrow_to_py_string("label", field_name, field_index, label_val, required=True)
            business_key = cls._arrow_to_py_boolean("business_key", field_name, field_index, business_key_val, default=False)  # noqa
            categorical = cls._arrow_to_py_boolean("categorical", field_name, field_index, categorical_val, default=False)  # noqa
            not_null = cls._arrow_to_py_boolean("not_null", field_name, field_index, not_null_val, default=business_key)  # noqa
            format_code = cls._arrow_to_py_string("format_code", field_name, field_index, format_code_val)

            if field_name is None or len(field_name.strip()) == 0:
                err = f"Field name cannot be blank for field at index [{field_index}]"
                cls._log.error(err)
                raise _ex.EDataConformance(err)

            if label is None or len(label.strip()) == 0:
                err = f"Label cannot be blank for field [{field_name}] at index [{field_index}]"
                cls._log.error(err)
                raise _ex.EDataConformance(err)

            try:
                if type_name:
                    field_type = _meta.BasicType[type_name.upper()]
                else:
                    field_type = _meta.BasicType.BASIC_TYPE_NOT_SET
            except KeyError:
                field_type = _meta.BasicType.BASIC_TYPE_NOT_SET

            if field_type == _meta.BasicType.BASIC_TYPE_NOT_SET:
                display_type_name = type_name or str(None)
                err = f"Unknown field type [{display_type_name}] for field [{field_name}] at index [{field_index}]"
                cls._log.error(err)
                raise _ex.EDataConformance(err)

            field_schema = _meta.FieldSchema(
                field_name, field_index, field_type, label,
                business_key, categorical, not_null, format_code)

            field_list.append(field_schema)

        return _meta.SchemaDefinition(
            schemaType=_meta.SchemaType.TABLE,
            table=_meta.TableSchema(field_list))

    @classmethod
    def _arrow_to_py_string(
            cls, schema_field_name: str, field_name: tp.Optional[str], field_index: int,
            arrow_value: pa.StringScalar, required: bool = False):

        if arrow_value is not None and arrow_value.is_valid:
            return arrow_value.as_py()

        if not required:
            return None

        if field_name:
            err_suffix = f" for field [{field_name}] at index [{field_index}]"
        else:
            err_suffix = f" for field at index [{field_index}]"

        err = f"Missing required value {schema_field_name}" + err_suffix
        cls._log.error(err)
        raise _ex.EDataConformance(err)

    @classmethod
    def _arrow_to_py_boolean(
            cls, schema_field_name: str, field_name: str, field_index: int,
            arrow_value: pa.BooleanScalar, required: bool = False, default: tp.Optional[bool] = None):

        if arrow_value is not None and arrow_value.is_valid:
            return arrow_value.as_py()

        if not required:
            return default

        err = f"Missing required value {schema_field_name} for field [{field_name}] at index [{field_index}]"
        cls._log.error(err)
        raise _ex.EDataConformance(err)


SchemaLoader._log = _util.logger_for_class(SchemaLoader)
