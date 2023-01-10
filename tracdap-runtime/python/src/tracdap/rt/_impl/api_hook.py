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

import typing as _tp
import types as _ts

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.schemas as _schemas
import tracdap.rt._impl.type_system as _type_system
import tracdap.rt._impl.util as _util
import tracdap.rt._impl.validation as _val

# Import hook interfaces into this module namespace
from tracdap.rt.api.hook import _RuntimeHook  # noqa
from tracdap.rt.api.hook import _Named  # noqa


class ApiGuard:

    @classmethod
    def validate_signature(cls, method: _tp.Callable, *args, **kwargs):

        _val.validate_signature(method, *args, **kwargs)

    @classmethod
    def check_type(cls, expected_type: _tp.Type, value: _tp.Any) -> bool:

        return _val.check_type(expected_type, value)


class RuntimeHookImpl(_RuntimeHook):

    @classmethod
    def register_impl(cls):

        log = _util.logger_for_class(cls)

        if not _RuntimeHook._is_registered():

            log.info("Registering runtime API hook...")
            _RuntimeHook._register(RuntimeHookImpl())

        else:

            log.warning("Runtime API hook is already registered")

    def define_attributes(
            self, *attrs: _tp.Union[_meta.TagUpdate, _tp.List[_meta.TagUpdate]]) \
            -> _tp.List[_meta.TagUpdate]:

        ApiGuard.validate_signature(self.define_attributes, *attrs)

        if len(attrs) == 1 and isinstance(attrs[0], list):
            return attrs[0]
        else:
            return [*attrs]

    def define_attribute(
            self, attr_name: str, attr_value: _tp.Any,
            attr_type: _tp.Optional[_meta.BasicType] = None,
            categorical: bool = False) \
            -> _meta.TagUpdate:

        ApiGuard.validate_signature(self.define_attribute, attr_name, attr_value, attr_type, categorical)

        if isinstance(attr_value, list) and attr_type is None:
            raise _ex.EModelValidation(f"Attribute type must be specified for multi-valued attribute [{attr_name}]")

        if categorical and not (isinstance(attr_name, str) or attr_type == _meta.BasicType.STRING):
            raise _ex.EModelValidation("Categorical flag is only allowed for STRING attributes")

        if attr_type is None:
            trac_value = _type_system.MetadataCodec.encode_value(attr_value)
        elif isinstance(attr_value, list):
            type_desc = _meta.TypeDescriptor(_meta.BasicType.ARRAY, arrayType=_meta.TypeDescriptor(attr_type))
            trac_value = _type_system.MetadataCodec.convert_value(attr_value, type_desc)
        else:
            type_desc = _meta.TypeDescriptor(attr_type)
            trac_value = _type_system.MetadataCodec.convert_value(attr_value, type_desc)

        return _meta.TagUpdate(_meta.TagOperation.CREATE_OR_APPEND_ATTR, attr_name, trac_value)

    def define_parameter(
            self, param_name: str, param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
            label: str, default_value: _tp.Optional[_tp.Any] = None) \
            -> _Named[_meta.ModelParameter]:

        ApiGuard.validate_signature(self.define_parameter, param_name, param_type, label, default_value)

        if isinstance(param_type, _meta.TypeDescriptor):
            param_type_descriptor = param_type
        else:
            param_type_descriptor = _meta.TypeDescriptor(param_type, None, None)

        if default_value is not None and not isinstance(default_value, _meta.Value):
            default_value = _type_system.MetadataCodec.encode_value(default_value)

        return _Named(param_name, _meta.ModelParameter(param_type_descriptor, label, default_value))

    def define_parameters(
            self, *params: _tp.Union[_Named[_meta.ModelParameter], _tp.List[_Named[_meta.ModelParameter]]]) \
            -> _tp.Dict[str, _meta.ModelParameter]:

        ApiGuard.validate_signature(self.define_parameters, *params)

        if len(params) == 1 and isinstance(params[0], list):
            return {p.item_name: p.item for p in params[0]}
        else:
            return {p.item_name: p.item for p in params}

    def define_field(
            self, field_name: str, field_type: _meta.BasicType, label: str,
            business_key: bool = False, categorical: bool = False,
            format_code: _tp.Optional[str] = None, field_order: _tp.Optional[int] = None) \
            -> _meta.FieldSchema:

        ApiGuard.validate_signature(
            self.define_field, field_name, field_type, label,
            business_key, categorical, format_code, field_order)

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

        ApiGuard.validate_signature(self.define_schema, *fields, schema_type=schema_type)

        if schema_type == _meta.SchemaType.TABLE:

            table_schema = self._build_table_schema(*fields)
            return _meta.SchemaDefinition(_meta.SchemaType.TABLE, table=table_schema)

        raise _ex.ERuntimeValidation(f"Invalid schema type [{schema_type.name}]")

    def load_schema(
            self, package: _tp.Union[_ts.ModuleType, str], schema_file: str,
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) \
            -> _meta.SchemaDefinition:

        ApiGuard.validate_signature(self.load_schema, package, schema_file, schema_type)

        return _schemas.SchemaLoader.load_schema(package, schema_file)

    def define_input_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelInputSchema:

        ApiGuard.validate_signature(self.define_input_table, *fields)

        schema_def = self.define_schema(*fields, schema_type=_meta.SchemaType.TABLE)
        return _meta.ModelInputSchema(schema=schema_def)

    def define_output_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelOutputSchema:

        ApiGuard.validate_signature(self.define_output_table, *fields)

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
