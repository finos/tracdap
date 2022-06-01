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
import importlib
import importlib.resources as resources
import typing as _tp
import types as _ts
import pathlib as _path

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.schemas as _schemas
import tracdap.rt.impl.util as _util

# Import hook interfaces into this module namespace
from tracdap.rt.api.hook import *


class RuntimeHookImpl(RuntimeHook):  # noqa

    @classmethod
    def register_impl(cls):

        log = _util.logger_for_class(cls)

        if not RuntimeHook._is_registered():

            log.info("Registering runtime API hook...")
            RuntimeHook._register(RuntimeHookImpl())

        else:

            log.warning("Runtime API hook is already registered")

    def define_parameter(
            self, param_name: str, param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
            label: str, default_value: _tp.Optional[_tp.Any] = None) \
            -> Named[_meta.ModelParameter]:

        if isinstance(param_type, _meta.TypeDescriptor):
            param_type_descriptor = param_type
        else:
            param_type_descriptor = _meta.TypeDescriptor(param_type, None, None)

        return Named(param_name, _meta.ModelParameter(param_type_descriptor, label, default_value))

    def define_parameters(
            self, *params: _tp.Union[Named[_meta.ModelParameter], _tp.List[Named[_meta.ModelParameter]]]) \
            -> _tp.Dict[str, _meta.ModelParameter]:

        if len(params) == 1 and isinstance(params[0], list):
            return {p.itemName: p.item for p in params[0]}
        else:
            return {p.itemName: p.item for p in params}

    def define_field(
            self, field_name: str, field_type: _meta.BasicType, label: str,
            business_key: bool = False, categorical: bool = False,
            format_code: _tp.Optional[str] = None, field_order: _tp.Optional[int] = None) \
            -> _meta.FieldSchema:

        return _meta.FieldSchema(
            field_name,
            field_order,
            field_type,
            label,
            businessKey=business_key,
            categorical=categorical,
            formatCode=format_code)

    def define_schema(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]],
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) \
            -> _meta.SchemaDefinition:

        if schema_type == _meta.SchemaType.TABLE:

            table_schema = self._build_table_schema(*fields)
            return _meta.SchemaDefinition(_meta.SchemaType.TABLE, table=table_schema)

        raise _ex.ERuntimeValidation(f"Invalid schema type [{schema_type.name}]")

    def load_schema(
            self, package: _tp.Union[_ts.ModuleType, str], schema_file: _tp.Union[str, _path.Path],
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) \
            -> _meta.SchemaDefinition:

        return _schemas.SchemaLoader.load_schema(package, schema_file)

    def define_input_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelInputSchema:

        schema_def = self.define_schema(*fields, schema_type=_meta.SchemaType.TABLE)
        return _meta.ModelInputSchema(schema=schema_def)

    def define_output_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelOutputSchema:

        schema_def = self.define_schema(*fields, schema_type=_meta.SchemaType.TABLE)
        return _meta.ModelOutputSchema(schema=schema_def)

    @staticmethod
    def _build_table_schema(
            *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.TableSchema:

        if len(fields) == 1 and isinstance(fields[0], list):
            fields_ = fields[0]
        else:
            fields_ = fields

        if all(map(lambda f: f.fieldOrder is None, fields_)):
            for index, field in enumerate(fields_):
                field.fieldOrder = index

        return _meta.TableSchema([*fields_])
