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

import dataclasses as _dc
import datetime as _dt
import decimal as _decimal
import enum as _enum
import json as _json
import types as _ts
import typing as _tp
import uuid as _uuid

import yaml as _yaml

try:
    import pydantic as _pyd  # noqa
except ModuleNotFoundError:
    _pyd = None

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.type_system as _meta_types


class StructProcessor:

    JSON_FORMAT = "application/json"
    JSON_ALT_FORMATS = ["json", ".json"]

    YAML_FORMAT = "application/yaml"
    YAML_ALT_FORMATS = ["yaml", ".yaml", "yml"]

    __primitive_types: dict[type, _meta.BasicType] = {
        bool: _meta.BasicType.BOOLEAN,
        int: _meta.BasicType.INTEGER,
        float: _meta.BasicType.FLOAT,
        str: _meta.BasicType.STRING,
        _decimal.Decimal: _meta.BasicType.DECIMAL,
        _dt.date: _meta.BasicType.DATE,
        _dt.datetime: _meta.BasicType.DATETIME
    }

    # Support both new and old styles for generic, union and optional types
    # Old-style annotations are still valid, even when the new style is fully supported
    __generic_types: list[type] = [
        _ts.GenericAlias,
        type(_tp.List[int]),
        type(_tp.Union[int, str]),
        type(_tp.Optional[int])
    ]

    __union_types: list[type] = [
        _tp.Union,
        _tp.Optional
    ]

    # UnionType was added to the types module in Python 3.10, we support 3.9 (Jan 2025)
    if hasattr(_ts, "UnionType"):
        __generic_types.append(_ts.UnionType)
        __union_types.append(_ts.UnionType)


    @classmethod
    def define_struct(cls, python_type: type) -> _meta.SchemaDefinition:

        named_types = dict()
        named_enums = dict()

        if _dc.is_dataclass(python_type):
            return cls._define_struct_for_dataclass(python_type, named_types, named_enums, type_stack=[])

        if _pyd and issubclass(python_type, _pyd.BaseModel):
            return cls._define_struct_for_pydantic(python_type, named_types, named_enums, type_stack=[])

        raise _ex.EUnexpected()

    @classmethod
    def load_struct(cls, src: _tp.TextIO, src_format: str) -> _tp.Any:

        try:

            if src_format == cls.YAML_FORMAT or src_format.lower() in cls.YAML_ALT_FORMATS:
                config_dict = _yaml.safe_load(src)

            elif src_format == cls.JSON_FORMAT or src_format.lower() in cls.JSON_ALT_FORMATS:
                config_dict = _json.load(src)

            else:
                msg = f"Format not recognised: " + src_format
                raise _ex.EConfigLoad(msg)

            return config_dict

        except UnicodeDecodeError as e:
            err = f"Contents of the config file is garbled and cannot be read ({str(e)})"
            raise _ex.EConfigParse(err) from e

        except _json.decoder.JSONDecodeError as e:
            err = f"Config file contains invalid JSON ({str(e)})"
            raise _ex.EConfigParse(err) from e

        except (_yaml.parser.ParserError, _yaml.reader.ReaderError) as e:
            err = f"Config file contains invalid YAML ({str(e)})"
            raise _ex.EConfigParse(err) from e

    @classmethod
    def save_struct(cls, struct: _tp.Any, dst: _tp.TextIO, dst_format: str):

        StructQuoter.quote(struct, dst, dst_format)

    @classmethod
    def parse_struct(cls, data: dict, python_type: type = None) -> object:

        parser = StructParser()
        return parser.parse(python_type, data)

    @classmethod
    def _define_struct_for_dataclass(
            cls, python_type: _dc.dataclass,
            named_types: _tp.Dict[str, _meta.SchemaDefinition],
            named_enums: _tp.Dict[str, _meta.EnumValues],
            type_stack: _tp.List[str]) \
            -> _meta.SchemaDefinition:

        try:

            type_stack.append(cls._qualified_type_name(python_type))

            type_hints = _tp.get_type_hints(python_type)
            trac_fields = list()

            for field_index, dc_field in enumerate(_dc.fields(python_type)):

                field_name = dc_field.name
                python_type = type_hints[field_name]

                trac_field = cls._define_field(
                    field_name, field_index, python_type, dc_field=dc_field,
                    named_types=named_types, named_enums=named_enums, type_stack=type_stack)

                trac_fields.append(trac_field)

            if len(type_stack) == 1:
                return _meta.SchemaDefinition(
                    schemaType=_meta.SchemaType.STRUCT_SCHEMA,
                    struct=_meta.StructSchema(fields=trac_fields),
                    namedTypes=named_types, namedEnums=named_enums)
            else:
                return _meta.SchemaDefinition(
                    schemaType=_meta.SchemaType.STRUCT_SCHEMA,
                    struct=_meta.StructSchema(fields=trac_fields))

        finally:

            type_stack.pop()

    @classmethod
    def _define_struct_for_pydantic(
            cls, python_type: "type[_pyd.BaseModel]",
            named_types: _tp.Dict[str, _meta.SchemaDefinition],
            named_enums: _tp.Dict[str, _meta.EnumValues],
            type_stack: _tp.List[str]) \
            -> _meta.SchemaDefinition:

        try:

            type_stack.append(cls._qualified_type_name(python_type))

            type_hints = _tp.get_type_hints(python_type)
            trac_fields = list()

            field_index = 0

            for field_name, pyd_field in python_type.model_fields.items():

                python_type = type_hints[field_name]

                trac_field = cls._define_field(
                    field_name, field_index, python_type, pyd_field=pyd_field,
                    named_types=named_types, named_enums=named_enums, type_stack=type_stack)

                if trac_field is not None:
                    trac_fields.append(trac_field)
                    field_index += 1

            if len(type_stack) == 1:
                return _meta.SchemaDefinition(
                    schemaType=_meta.SchemaType.STRUCT_SCHEMA,
                    struct=_meta.StructSchema(fields=trac_fields),
                    namedTypes=named_types, namedEnums=named_enums)
            else:
                return _meta.SchemaDefinition(
                    schemaType=_meta.SchemaType.STRUCT_SCHEMA,
                    struct=_meta.StructSchema(fields=trac_fields))

        finally:

            type_stack.pop()

    @classmethod
    def _define_field(
            cls, name, index, python_type: type, optional=False, *,
            named_types: _tp.Dict[str, _meta.SchemaDefinition],
            named_enums: _tp.Dict[str, _meta.EnumValues],
            type_stack: _tp.List[str],
            dc_field: _dc.Field = None, pyd_field: "_pyd.fields.FieldInfo" = None) \
            -> _meta.FieldSchema:

        if python_type in cls.__primitive_types:

            return cls._define_primitive_field(
                name, index, python_type, optional,
                dc_field=dc_field, pyd_field=pyd_field)

        elif any(map(lambda _t: isinstance(python_type, _t), cls.__generic_types)):

            return cls._define_generic_field(
                name, index, python_type,
                dc_field=dc_field, pyd_field=pyd_field,
                named_types=named_types, named_enums=named_enums,
                type_stack=type_stack)

        elif isinstance(python_type, _enum.EnumMeta):

            type_name = cls._qualified_type_name(python_type)

            if type_name not in named_enums:
                enum_values = cls._define_enum_values(python_type)
                named_enums[type_name] = enum_values

            return cls._define_enum_field(
                name, index, python_type, optional,
                dc_field=dc_field, pyd_field=pyd_field)

        elif _dc.is_dataclass(python_type):

            type_name = cls._qualified_type_name(python_type)

            if type_name in type_stack:
                raise _ex.EValidation("Recursive types are not supported")

            if type_name not in named_types:
                struct_type = cls._define_struct_for_dataclass(python_type, named_types, named_enums, type_stack)
                named_types[type_name] = struct_type

            return _meta.FieldSchema(
                fieldName=name,
                fieldOrder=index,
                fieldType=_meta.BasicType.STRUCT,
                notNull=not optional,
                namedType=type_name)

        elif issubclass(python_type, _pyd.BaseModel):

            type_name = cls._qualified_type_name(python_type)

            if type_name in type_stack:
                raise _ex.EValidation("Recursive types are not supported")

            if type_name not in named_types:
                struct_type = cls._define_struct_for_pydantic(python_type, named_types, named_enums, type_stack)
                named_types[type_name] = struct_type

            return _meta.FieldSchema(
                fieldName=name,
                fieldOrder=index,
                fieldType=_meta.BasicType.STRUCT,
                notNull=not optional,
                namedType=type_name)

        else:
            raise _ex.ETracInternal("Cannot encode field type: " + str(python_type))

    @classmethod
    def _define_primitive_field(
            cls, name: str, index: int, python_type: type, optional=False, *,
            dc_field: _dc.Field = None, pyd_field: "_pyd.fields.FieldInfo" = None) \
            -> _meta.FieldSchema:

        default_value = None

        if dc_field is not None:
            if dc_field.default not in [_dc.MISSING, None]:
                default_value = _meta_types.MetadataCodec.encode_value(dc_field.default)
            elif dc_field.default_factory not in [_dc.MISSING, None]:
                native_value = dc_field.default_factory()
                default_value = _meta_types.MetadataCodec.encode_value(native_value)

        elif pyd_field is not None:
            if pyd_field.default not in [_pyd.fields.PydanticUndefined, None]:
                default_value = _meta_types.MetadataCodec.encode_value(pyd_field.default)
            elif pyd_field.default_factory not in [_pyd.fields.PydanticUndefined, None]:
                native_value = pyd_field.default_factory()
                default_value = _meta_types.MetadataCodec.encode_value(native_value)

        return _meta.FieldSchema(
            fieldName=name,
            fieldOrder=index,
            fieldType=cls.__primitive_types[python_type],
            notNull=not optional,
            defaultValue=default_value)

    @classmethod
    def _define_enum_field(
            cls, name: str, index: int, enum_type: _enum.EnumMeta, optional=False, *,
            dc_field: _dc.Field = None, pyd_field: "_pyd.fields.FieldInfo" = None) \
            -> _meta.FieldSchema:

        default_value = None

        if dc_field is not None and  dc_field.default not in [_dc.MISSING, None]:
            default_value = _meta_types.MetadataCodec.encode_value(dc_field.default.name)

        if pyd_field is not None and pyd_field.default not in [_pyd.fields.PydanticUndefined, None]:
            default_value = _meta_types.MetadataCodec.encode_value(pyd_field.default.name)

        return _meta.FieldSchema(
            fieldName=name,
            fieldOrder=index,
            fieldType=_meta.BasicType.STRING,
            categorical=True,
            notNull=not optional,
            namedEnum=cls._qualified_type_name(enum_type),
            defaultValue=default_value)

    @classmethod
    def _define_enum_values(cls, enum_type: _enum.EnumMeta) -> _meta.EnumValues:

        values = list(map(lambda value: value.name, enum_type))
        return _meta.EnumValues(values=values)

    @classmethod
    def _define_generic_field(
            cls, name, index, python_type: type, *,
            named_types: _tp.Dict[str, _meta.SchemaDefinition],
            named_enums: _tp.Dict[str, _meta.EnumValues],
            type_stack: _tp.List[str],
            dc_field: _dc.Field = None, pyd_field: "_pyd.fields.FieldInfo" = None) \
            -> _meta.FieldSchema:

        origin = _tp.get_origin(python_type)
        args = _tp.get_args(python_type)

        # types.NoneType not available in Python 3.9, so use type(None) instead
        if origin in cls.__union_types and len(args) == 2 and type(None) in args:
            optional_type = args[0] if args[1] is type(None) else args[1]
            return cls._define_field(
                name, index, optional_type, optional=True,
                dc_field=dc_field, pyd_field=pyd_field,
                named_types=named_types, named_enums=named_enums,
                type_stack=type_stack)

        elif origin in [list, _tp.List]:

            item_type = args[0]
            item_field = cls._define_field(
                "item", 0, item_type, optional=False,
                named_types=named_types, named_enums=named_enums,
                type_stack=type_stack)

            return _meta.FieldSchema(
                fieldName=name,
                fieldOrder=index,
                fieldType=_meta.BasicType.ARRAY,
                notNull=True,
                children=[item_field])

        elif origin in [dict, _tp.Dict]:

            key_type = args[0]
            key_field = _meta.FieldSchema(
                fieldName="key",
                fieldOrder=0,
                fieldType=_meta.BasicType.STRING,
                notNull=True)

            value_type = args[1]
            value_field = cls._define_field(
                "value", 1, value_type, optional=False,
                named_types=named_types, named_enums=named_enums,
                type_stack=type_stack)

            return _meta.FieldSchema(
                fieldName=name,
                fieldOrder=index,
                fieldType=_meta.BasicType.MAP,
                notNull=True,
                children=[key_field, value_field])

        else:
            raise _ex.ETracInternal("Cannot encode field type: " + str(python_type))

    @classmethod
    def _qualified_type_name(cls, python_type: type):

        name = python_type.__name__
        module = python_type.__module__

        if module.startswith(cls.__SHIM_PREFIX):
            shim_root_index = module.index(".", len(cls.__SHIM_PREFIX)) + 1
            module = module[shim_root_index:]

        return f"{module}.{name}"

    __SHIM_PREFIX = "tracdap.shim."


class StructConformance:

    # This class is for enforcing conformance of STRUCT data against a schema
    # Raw data can be supplied as a dict (e.g. if loaded directly from JSON / YAML),
    # or as a dataclass or Pydantic model (e.g. if passed in from a model output).
    # The result is always a dict of pure Python types, any fields not defined in
    # the schema will be discarded. Any classes / types defined in client code are
    # discarded by the conformance process.

    __primitive_parse_func: _tp.Dict[type, callable] = {
        bool: bool,
        int: int,
        float: float,
        str: str,
        _decimal.Decimal: _decimal.Decimal,
        _dt.date: _dt.date.fromisoformat,
        _dt.datetime: _dt.datetime.fromisoformat
    }

    @classmethod
    def conform_to_schema(cls, schema: _meta.SchemaDefinition, raw_data: _tp.Any) -> dict:

        conformance = StructConformance()
        root_location = ""

        conformed_value = conformance._conform_struct_fields(root_location, raw_data, schema.struct.fields, schema)

        if any(conformance._errors):

            message = "One or more conformance errors in STRUCT data"

            for (location, error) in conformance._errors:
                location_info = f" (in {location})" if location else ""
                message = message + f"\n{error}{location_info}"

            raise _ex.EDataConformance(message)

        return conformed_value

    def __init__(self):
        self._log = _logging.logger_for_object(self)
        self._errors = []

    def _conform_value(self, location: str, raw_value: _tp.Any, trac_field: _meta.FieldSchema, trac_schema: _meta.SchemaDefinition):

        if raw_value is None:
            if trac_field.notNull or trac_field.businessKey:
                self._error(location, f"Field [{trac_field.fieldName}] cannot be null")
            return None

        if _meta_types.TypeMapping.is_primitive(trac_field.fieldType):
            if trac_field.categorical:
                return self._conform_categorical(location, raw_value, trac_field, trac_schema)
            else:
                return self._conform_primitive(location, raw_value, trac_field)

        if trac_field.fieldType == _meta.BasicType.ARRAY:
            return self._conform_list(location, raw_value, trac_field, trac_schema)

        if trac_field.fieldType == _meta.BasicType.MAP:
            return self._conform_map(location, raw_value, trac_field, trac_schema)

        if trac_field.fieldType == _meta.BasicType.STRUCT:
            return self._conform_struct(location, raw_value, trac_field, trac_schema)

        return self._error(location, f"Type mapping not available for field [{trac_field.fieldName}]")

    def _conform_primitive(self, location: str, raw_value: _tp.Any, trac_field):

        python_type = _meta_types.TypeMapping.trac_to_python_basic_type(trac_field.fieldType)

        try:
            if isinstance(raw_value, python_type):
                return raw_value

            elif isinstance(raw_value, str):
                parse_func = StructConformance.__primitive_parse_func[python_type]
                return parse_func(raw_value)

            elif python_type == str:
                if isinstance(raw_value, _enum.Enum):
                    return raw_value.name
                else:
                    return str(raw_value)

            else:
                raise TypeError

        except (ValueError, TypeError):
            str_value = str(raw_value)
            short_value = str_value[:20] + "..." if len(str_value) > 20 else str_value
            message = f"Invalid value [{short_value}] for field [{trac_field.fieldName}]" + \
                      f" (expected type {python_type.__name__})"
            return self._error(location, message)

    def _conform_categorical(
            self, location: str, raw_value: _tp.Any,
            trac_field: _meta.FieldSchema,
            trac_schema: _meta.SchemaDefinition):

        primitive_value = self._conform_primitive(location, raw_value, trac_field)

        if trac_field.namedEnum is not None:

            if trac_field.namedEnum not in trac_schema.namedEnums:
                return self._error(location, f"Invalid schema (named enum [{trac_field.namedType}] is not defined")

            named_enum = trac_schema.namedEnums[trac_field.namedEnum]

            if primitive_value not in named_enum.values:
                str_value = str(raw_value)
                short_value = str_value[:20] + "..." if len(str_value) > 20 else str_value
                message = f"Invalid value [{short_value}] for field [{trac_field.fieldName}]" + \
                          f" (using named enum: {trac_field.namedEnum})"
                return self._error(location, message)

        return primitive_value

    def _conform_list(
            self, location: str, raw_value: _tp.Any,
            trac_field: _meta.FieldSchema,
            trac_schema: _meta.SchemaDefinition):

        if not isinstance(raw_value, list):
            return self._error(location, f"Field [{trac_field.fieldName}] should be a list (got {type(raw_value)})")

        item_field = trac_field.children[0]

        return [
            self._conform_value(self._child_location(location, index), item, item_field, trac_schema)
            for index, item in enumerate(raw_value)
        ]

    def _conform_map(
            self, location: str, raw_value: _tp.Any,
            trac_field: _meta.FieldSchema,
            trac_schema: _meta.SchemaDefinition):

        if not isinstance(raw_value, dict):
            return self._error(location, f"Field [{trac_field.fieldName}] should be a map (got {type(raw_value)})")

        key_field = trac_field.children[0]
        value_field = trac_field.children[1]

        return {
            self._conform_value(self._child_location(location, key), key, key_field, trac_schema):
                self._conform_value(self._child_location(location, key), value, value_field, trac_schema)
            for key, value in raw_value.items()
        }

    def _conform_struct(
            self, location: str, raw_value: _tp.Any,
            trac_field: _meta.FieldSchema,
            trac_schema: _meta.SchemaDefinition):

        if not isinstance(raw_value, dict) \
                and not _dc.is_dataclass(type(raw_value)) \
                and not isinstance(raw_value, _pyd.BaseModel):

            return self._error(location, f"Field [{trac_field.fieldName}] should be a struct (got {type(raw_value)})")

        if trac_field.namedType is not None:
            if trac_field.namedType not in trac_schema.namedTypes:
                return self._error(location, f"Invalid schema (named type [{trac_field.namedType}] is not defined")
            struct_type = trac_schema.namedTypes[trac_field.namedType]
            struct_fields = struct_type.struct.fields
        else:
            struct_fields = trac_field.children

        return self._conform_struct_fields(location, raw_value, struct_fields, trac_schema)

    def _conform_struct_fields(
            self, location: str, raw_value: _tp.Any,
            struct_fields: _tp.List[_meta.FieldSchema],
            trac_schema: _meta.SchemaDefinition):

        struct = dict()

        for struct_field in struct_fields:

            struct_field_location = self._child_location(location, struct_field.fieldName)

            if isinstance(raw_value, dict):
                if struct_field.fieldName in raw_value:
                    struct_value_present = True
                    struct_value = raw_value.get(struct_field.fieldName)
                else:
                    struct_value_present = False
                    struct_value = None
            elif hasattr(raw_value, struct_field.fieldName):
                struct_value_present = True
                struct_value = getattr(raw_value, struct_field.fieldName)
            else:
                struct_value_present = False
                struct_value = None

            if not struct_value_present:
                if struct_field.notNull or struct_field.businessKey:
                    self._error(location, f"Missing STRUCT field [{struct_field.fieldName}] which cannot be null")
                else:
                    struct[struct_field.fieldName] = None
            else:
                struct[struct_field.fieldName] = self._conform_value(struct_field_location, struct_value, struct_field, trac_schema)

        return struct

    def _error(self, location: str, error: str) -> None:
        self._errors.append((location, error))
        return None

    @staticmethod
    def _child_location(parent_location: str, item: _tp.Union[str, int]):

        if parent_location is None or parent_location == "":
            return item
        elif isinstance(item, int):
            return f"{parent_location}[{item}]"
        else:
            return f"{parent_location}.{item}"


class StructParser:

    # New implementation of STRUCT parsing, copied from config_parser
    # After a period of stabilization, config_parser will switch to this implementation
    # It will need to inherit the class with overrides for handling dev-mode locations

    __primitive_types: _tp.Dict[type, callable] = {
        bool: bool,
        int: int,
        float: float,
        str: str,
        _decimal.Decimal: _decimal.Decimal,
        _dt.date: _dt.date.fromisoformat,
        _dt.datetime: _dt.datetime.fromisoformat
    }

    # Support both new and old styles for generic, union and optional types
    # Old-style annotations are still valid, even when the new style is fully supported
    __generic_types: list[type] = [
        _ts.GenericAlias,
        type(_tp.List[int]),
        type(_tp.Optional[int])
    ]

    # UnionType was added to the types module in Python 3.10, we support 3.9 (Jan 2025)
    if hasattr(_ts, "UnionType"):
        __generic_types.append(_ts.UnionType)

    __STRUCT_TYPE = _tp.TypeVar("__STRUCT_TYPE")

    def __init__(self):
        self._log = _logging.logger_for_object(self)
        self._errors = []

    def parse(self, config_class: type[__STRUCT_TYPE], raw_value: _tp.Any) -> __STRUCT_TYPE:

        # If config is empty, return a default (blank) config
        if raw_value is None or (isinstance(raw_value, dict) and len(raw_value) == 0):
            return config_class()

        config = self._parse_value("", raw_value, config_class)

        if any(self._errors):

            message = "One or more conformance errors in STRUCT data"

            for (location, error) in self._errors:
                location_info = f" (in {location})" if location else ""
                message = message + f"\n{error}{location_info}"

            raise _ex.EConfigParse(message)

        return config

    def _parse_value(self, location: str, raw_value: _tp.Any, annotation: type):

        if raw_value is None:
            return None

        if annotation in StructParser.__primitive_types:
            return self._parse_primitive(location, raw_value, annotation)

        # Allow parsing of generic primitives, this allows for e.g. param maps of mixed primitive types
        if annotation == _tp.Any:

            if type(raw_value) in StructParser.__primitive_types:
                return self._parse_primitive(location, raw_value, type(raw_value))
            else:
                return self._error(location, f"Expected a primitive value, got '{str(raw_value)}'")

        if isinstance(annotation, _enum.EnumMeta):
            return self._parse_enum(location, raw_value, annotation)

        if any(map(lambda _t: isinstance(annotation, _t), self.__generic_types)):
            return self._parse_generic_class(location, raw_value, annotation)  # noqa

        if _dc.is_dataclass(annotation):
            return self._parser_dataclass(location, raw_value, annotation)

        # Basic support for Pydantic, if it is installed
        if _pyd and issubclass(annotation, _pyd.BaseModel):
            return self._parser_pydantic_model(location, raw_value, annotation)

        return self._error(location, f"Cannot parse value of type {annotation.__name__}")

    def _parse_primitive(self, location: str, raw_value: _tp.Any, simple_type: type):

        parse_func = StructParser.__primitive_types[simple_type]

        try:
            if isinstance(raw_value, simple_type):
                return raw_value

            elif isinstance(raw_value, str):
                return parse_func(raw_value)

            elif simple_type == str:
                return str(raw_value)

            else:
                raise TypeError

        except (ValueError, TypeError):
            return self._error(location, f"Expected primitive type {simple_type.__name__}, got '{str(raw_value)}'")

    def _parse_enum(self, location: str, raw_value: _tp.Any, enum_type: _enum.EnumMeta):

        if not isinstance(raw_value, str):
            return self._error(location, f"Expected {enum_type.__name__} (string), got {str(raw_value)}")

        try:
            enum_value = self._parse_enum_value(raw_value, enum_type)

            if isinstance(enum_value.value, tuple):
                enum_value._value_ = enum_value.value[0]

            return enum_type.__new__(enum_type, enum_value)

        except KeyError:
            return self._error(location, f"Invalid value for {enum_type.__name__}: {raw_value}")

    @staticmethod
    def _parse_enum_value(raw_value: str, enum_type: _enum.EnumMeta):

        try:
            return enum_type.__members__[raw_value]

        except KeyError:

            # Try a case-insensitive match as a fallback
            for enum_member in enum_type.__members__:
                if enum_member.upper() == raw_value.upper():
                    return enum_type.__members__[enum_member]

            # Re-raise the exception if case-insensitive match fails
            raise

    def _parser_dataclass(self, location: str, raw_dict: _tp.Any, python_type: type) -> object:

        type_hints = _tp.get_type_hints(python_type)
        init_values = dict()
        missing_values = list()

        for dc_field in _dc.fields(python_type):  # noqa

            field_name = dc_field.name
            field_location = self._child_location(location, field_name)
            field_type = type_hints[field_name]
            field_raw_value = raw_dict.get(field_name)

            if field_raw_value is not None:
                field_value = self._parse_value(field_location, field_raw_value, field_type)
                init_values[field_name] = field_value

            elif dc_field.default is _dc.MISSING:
                self._error(location, f"Missing required value '{field_name}'")
                missing_values.append(field_name)

        # Do not try to construct an invalid instance
        if any(missing_values):
            return None

        return python_type(**init_values)

    def _parser_pydantic_model(self, location: str, raw_dict: _tp.Any, python_type: "type[_pyd.BaseModel]") -> object:

        type_hints = _tp.get_type_hints(python_type)
        init_values = dict()
        missing_values = list()

        for field_name, pyd_field in python_type.model_fields.items():

            field_location = self._child_location(location, field_name)
            field_type = type_hints[field_name]
            field_raw_value = raw_dict.get(field_name)

            if field_raw_value is not None:
                field_value = self._parse_value(field_location, field_raw_value, field_type)
                init_values[field_name] = field_value

            elif pyd_field.is_required():
                self._error(location, f"Missing required value '{field_name}'")
                missing_values.append(field_name)

        # Do not try to construct an invalid instance
        if any(missing_values):
            return None

        return python_type(**init_values)

    def _parse_generic_class(self, location: str, raw_value: _tp.Any,  metaclass: type):

        origin = _tp.get_origin(metaclass)
        args = _tp.get_args(metaclass)

        if origin == _tp.List or origin == list:

            list_type = args[0]

            if not isinstance(raw_value, list):
                return self._error(location, f"Expected a list, got {type(raw_value)}")

            return [
                self._parse_value(self._child_location(location, idx), item, list_type)
                for (idx, item) in enumerate(raw_value)]

        if origin == _tp.Dict or origin == dict:

            key_type = args[0]
            value_type = args[1]

            if not isinstance(raw_value, dict):
                return self._error(location, f"Expected {metaclass} (dict), got {type(raw_value)}")

            return {
                self._parse_value(self._child_location(location, key), key, key_type):
                self._parse_value(self._child_location(location, key), value, value_type)
                for key, value in raw_value.items()}

        # Handle Optional, which is a shorthand for tp.Union[type, None]
        if origin == tp.Union and len(args) == 2 and args[1] == type(None):  # noqa

            if raw_value is not None:
                return self._parse_value(location, raw_value, args[0])
            else:
                return None

        return self._error(location, f"Struct parser does not support generic type '{str(origin)}'")

    def _error(self, location: str, error: str) -> None:
        self._errors.append((location, error))
        return None

    @classmethod
    def _is_primitive(cls, obj: _tp.Union[type, _tp.Any]) -> bool:

        if isinstance(obj, type):
            return obj in cls.__primitive_types
        else:
            return type(obj) in cls.__primitive_types

    @staticmethod
    def _child_location(parent_location: str, item: _tp.Union[str, int]):

        if parent_location is None or parent_location == "":
            return item
        elif isinstance(item, int):
            return f"{parent_location}[{item}]"
        else:
            return f"{parent_location}.{item}"


class StructQuoter:

    # New implementation of STRUCT quoting, copied from config_parser
    # After a period of stabilization, config_parser will switch to this implementation

    INDENT = 3

    @classmethod
    def quote(cls, obj: _tp.Any, dst: _tp.TextIO, dst_format: str):


        if dst_format == StructProcessor.YAML_FORMAT or dst_format.lower() in StructProcessor.YAML_ALT_FORMATS:
            return cls.quote_yaml(obj, dst)

        if dst_format == StructProcessor.JSON_FORMAT or dst_format.lower() in StructProcessor.JSON_ALT_FORMATS:
            return cls.quote_json(obj, dst)

        # TODO :This is probably an error in the user-supplied parameters
        raise _ex.ETracInternal(f"Unsupported output format [{dst_format}]")

    @classmethod
    def quote_json(cls, obj: _tp.Any, dst: _tp.TextIO):

        _json.dump(obj, dst, cls=StructQuoter._JsonEncoder, indent=cls.INDENT)

    @classmethod
    def quote_yaml(cls, obj: _tp.Any, dst: _tp.TextIO):

        _yaml.dump(obj, dst,indent=cls.INDENT, Dumper=StructQuoter._YamlDumper)

    class _JsonEncoder(_json.JSONEncoder):

        def __init__(self, **kwargs):

            super().__init__(**kwargs)

            # Do not force-escape non-ascii characters, output UTF-8 instead
            self.ensure_ascii = False

        def default(self, o: _tp.Any) -> str:

            if isinstance(o, _enum.Enum):
                return o.name
            if isinstance(o, _uuid.UUID):
                return str(o)
            if isinstance(o, _dt.date):
                return o.isoformat()
            if isinstance(o, _dt.datetime):
                return o.isoformat()
            elif _dc.is_dataclass(o):
                return o.__dict__
            elif _pyd and isinstance(o, _pyd.BaseModel):
                return o.__dict__  # noqa
            else:
                return super().default(o)

    class _YamlDumper(_yaml.Dumper):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def ignore_aliases(self, data):
            return True

        def represent_data(self, data):
            if isinstance(data, _enum.Enum):
                return self.represent_str(data.name)
            if _dc.is_dataclass(data):
                return self.represent_dict(data.__dict__)
            elif _pyd and isinstance(data, _pyd.BaseModel):
                return self.represent_dict(data.__dict__)
            else:
                return super().represent_data(data)
