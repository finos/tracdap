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

import typing as _tp
import types as _ts

import tracdap.rt.api.experimental as _api
import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.schemas as _schemas
import tracdap.rt._impl.core.struct as _struct
import tracdap.rt._impl.core.type_system as _type_system
import tracdap.rt._impl.core.validation as _val

# Import hook interfaces into this module namespace
from tracdap.rt.api.hook import _StaticApiHook  # noqa
from tracdap.rt.api.hook import _Named  # noqa


class StaticApiImpl(_StaticApiHook):

    _T = _tp.TypeVar("_T")

    @classmethod
    def register_impl(cls):

        if not _StaticApiHook._is_registered():
            _StaticApiHook._register(StaticApiImpl())

    def array_type(self, item_type: _meta.BasicType) -> _meta.TypeDescriptor:

        _val.validate_signature(self.array_type, item_type)

        if not _val.is_primitive_type(item_type):
            raise _ex.EModelValidation(f"Arrays can only contain primitive types, [{item_type}] is not primitive")

        return _meta.TypeDescriptor(_meta.BasicType.ARRAY, arrayType=_meta.TypeDescriptor(item_type))

    def map_type(self, entry_type: _meta.BasicType) -> _meta.TypeDescriptor:

        _val.validate_signature(self.map_type, entry_type)

        if not _val.is_primitive_type(entry_type):
            raise _ex.EModelValidation(f"Maps can only contain primitive types, [{entry_type}] is not primitive")

        return _meta.TypeDescriptor(_meta.BasicType.MAP, mapType=_meta.TypeDescriptor(entry_type))

    def define_attribute(
            self, attr_name: str, attr_value: _tp.Any,
            attr_type: _tp.Optional[_meta.BasicType] = None,
            categorical: bool = False) \
            -> _Named[_meta.Value]:

        _val.validate_signature(self.define_attribute, attr_name, attr_value, attr_type, categorical)

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

        return _Named(attr_name, trac_value)

    def define_attributes(
            self, *attrs: _tp.Union[_Named[_meta.Value], _tp.List[_Named[_meta.Value]]]) \
            -> _tp.Dict[str, _meta.Value]:

        _val.validate_signature(self.define_attributes, *attrs)

        return self._build_named_dict(*attrs)

    def define_parameter(
            self, param_name: str, param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
            label: str, default_value: _tp.Optional[_tp.Any] = None,
            *, param_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
            -> _Named[_meta.ModelParameter]:

        _val.validate_signature(
            self.define_parameter,
            param_name, param_type, label, default_value,
            param_props=param_props)

        if isinstance(param_type, _meta.TypeDescriptor):
            param_type_descriptor = param_type
        else:
            param_type_descriptor = _meta.TypeDescriptor(param_type, None, None)

        if default_value is not None and not isinstance(default_value, _meta.Value):
            try:
                default_value = _type_system.MetadataCodec.convert_value(default_value, param_type_descriptor)
            except _ex.ETrac as e:
                msg = f"Default value for parameter [{param_name}] does not match the declared type"
                raise _ex.EModelValidation(msg) from e

        return _Named(param_name, _meta.ModelParameter(
            param_type_descriptor, label, default_value,
            paramProps=param_props))

    def define_parameters(
            self, *params: _tp.Union[_Named[_meta.ModelParameter], _tp.List[_Named[_meta.ModelParameter]]]) \
            -> _tp.Dict[str, _meta.ModelParameter]:

        _val.validate_signature(self.define_parameters, *params)

        return self._build_named_dict(*params)

    def define_field(
            self, field_name: str, field_type: _meta.BasicType, label: str,
            business_key: bool = False, categorical: bool = False, not_null: _tp.Optional[bool] = None,
            format_code: _tp.Optional[str] = None, field_order: _tp.Optional[int] = None) \
            -> _meta.FieldSchema:

        _val.validate_signature(
            self.define_field, field_name, field_type, label,
            business_key, categorical, not_null,
            format_code, field_order)

        # Set the not_null flag for business keys if it is not specified
        if not_null is None:
            not_null = business_key

        return _meta.FieldSchema(
            field_name,
            field_order,
            field_type,
            label,
            businessKey=business_key,
            categorical=categorical,
            notNull=not_null,
            formatCode=format_code)

    def define_struct(self, python_type: type[_api.STRUCT_TYPE]):

        _val.validate_signature(self.define_struct, python_type)

        struct_schema = _struct.StructProcessor.define_struct(python_type)
        return _meta.SchemaDefinition(schemaType=_meta.SchemaType.STRUCT, struct=struct_schema)

    def define_schema(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]],
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE, dynamic: bool = False) \
            -> _meta.SchemaDefinition:

        _val.validate_signature(self.define_schema, *fields, schema_type=schema_type, dynamic=dynamic)

        if schema_type == _meta.SchemaType.TABLE:

            if dynamic and not fields:
                table_schema = None
            else:
                table_schema = self._build_table_schema(*fields)

            return _meta.SchemaDefinition(_meta.SchemaType.TABLE, table=table_schema)

        raise _ex.ERuntimeValidation(f"Invalid schema type [{schema_type.name}]")

    def load_schema(
            self, package: _tp.Union[_ts.ModuleType, str], schema_file: str,
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) \
            -> _meta.SchemaDefinition:

        _val.validate_signature(self.load_schema, package, schema_file, schema_type)

        return _schemas.SchemaLoader.load_schema(package, schema_file)

    def infer_schema(self, dataset: _api.DATA_API) -> _meta.SchemaDefinition:

        _val.validate_signature(self.infer_schema, dataset)

        framework = _data.DataConverter.get_framework(dataset)
        converter = _data.DataConverter.for_framework(framework)

        return converter.infer_schema(dataset)

    def define_file_type(self, extension: str, mime_type: str) -> _meta.FileType:

        _val.validate_signature(self.define_file_type, extension, mime_type)

        return _meta.FileType(extension=extension, mimeType=mime_type)

    def define_input(
            self, requirement: _tp.Union[_meta.SchemaDefinition, _meta.FileType], *,
            label: _tp.Optional[str] = None,
            optional: bool = False, dynamic: bool = False,
            input_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None):

        _val.validate_signature(
            self.define_input, requirement,
            label=label, optional=optional, dynamic=dynamic,
            input_props=input_props)

        if isinstance(requirement, _meta.SchemaDefinition):

            return _meta.ModelInputSchema(
                objectType=_meta.ObjectType.DATA, schema=requirement,
                label=label, optional=optional, dynamic=dynamic,
                inputProps=input_props)

        elif isinstance(requirement, _meta.FileType):

            return _meta.ModelInputSchema(
                objectType=_meta.ObjectType.FILE, fileType=requirement,
                label=label, optional=optional, dynamic=dynamic,
                inputProps=input_props)

        else:
            raise _ex.EUnexpected()

    def define_output(
            self, requirement: _tp.Union[_meta.SchemaDefinition, _meta.FileType], *,
            label: _tp.Optional[str] = None,
            optional: bool = False, dynamic: bool = False,
            output_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None):

        _val.validate_signature(
            self.define_output, requirement,
            label=label, optional=optional, dynamic=dynamic,
            output_props=output_props)

        if isinstance(requirement, _meta.SchemaDefinition):

            return _meta.ModelOutputSchema(
                objectType=_meta.ObjectType.DATA, schema=requirement,
                label=label, optional=optional, dynamic=dynamic,
                outputProps=output_props)

        elif isinstance(requirement, _meta.FileType):

            return _meta.ModelOutputSchema(
                objectType=_meta.ObjectType.FILE, fileType=requirement,
                label=label, optional=optional, dynamic=dynamic,
                outputProps=output_props)

        else:
            raise _ex.EUnexpected()

    @staticmethod
    def _build_named_dict(
            *attrs: _tp.Union[_Named[_T], _tp.List[_Named[_T]]]) \
            -> _tp.Dict[str, _T]:

        if len(attrs) == 1 and isinstance(attrs[0], list):
            return {a.item_name: a.item for a in attrs[0]}
        else:
            return {a.item_name: a.item for a in attrs}

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
