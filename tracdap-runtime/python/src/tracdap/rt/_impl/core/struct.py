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
    def define_struct(cls, python_type: type) -> _meta.StructSchema:

        if _dc.is_dataclass(python_type):
            return cls._define_struct_for_dataclass(python_type)

        if _pyd and issubclass(python_type, _pyd.BaseModel):
            return cls._define_struct_for_pydantic(python_type)

        raise _ex.EUnexpected()

    @classmethod
    def load_struct(cls, src: _tp.TextIO, src_format: str) -> _tp.Any:

        try:

            if src_format == "yaml" or src_format == "yml":
                config_dict = _yaml.safe_load(src)

            elif src_format == "json":
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
    def parse_struct(cls, data: dict, schema: _meta.StructSchema = None, python_type: type = None) -> object:

        parser = StructParser()
        return parser.parse(python_type, data)

    @classmethod
    def _define_struct_for_dataclass(cls, python_type: _dc.dataclass) -> _meta.StructSchema:

        type_hints = _tp.get_type_hints(python_type)
        trac_fields = dict()

        for dc_field in _dc.fields(python_type):

            field_name = dc_field.name
            python_type = type_hints[field_name]

            trac_field = cls._define_field(python_type, dc_field=dc_field)
            trac_fields[field_name] = trac_field

        return _meta.StructSchema(fields=trac_fields)

    @classmethod
    def _define_struct_for_pydantic(cls, python_type: "type[_pyd.BaseModel]") -> _meta.StructSchema:

        type_hints = _tp.get_type_hints(python_type)
        trac_fields = dict()

        for field_name, pyd_field in python_type.model_fields.items():

            python_type = type_hints[field_name]

            trac_field = cls._define_field(python_type, pyd_field=pyd_field)

            if trac_field is not None:
                trac_fields[field_name] = trac_field

        return _meta.StructSchema(fields=trac_fields)

    @classmethod
    def _define_field(
            cls, python_type: type, *,
            dc_field: _dc.Field = None,
            pyd_field: "_pyd.fields.FieldInfo" = None) \
            -> _meta.StructField:

        if python_type in cls.__primitive_types:
            return cls._define_primitive_field(python_type, dc_field=dc_field, pyd_field=pyd_field)

        elif any(map(lambda _t: isinstance(python_type, _t), cls.__generic_types)):
            return cls._define_generic_field(python_type, pyd_field=pyd_field)

        elif dc_field is not None and _dc.is_dataclass(python_type):
            pass

        elif pyd_field is not None and issubclass(python_type, _pyd.BaseModel):
            pass

        else:
            raise _ex.ETracInternal("Cannot encode field type: " + str(python_type))

    @classmethod
    def _define_primitive_field(
            cls, python_type: type, optional=False, *,
            dc_field: _dc.Field = None,
            pyd_field: "_pyd.fields.FieldInfo" = None) \
            -> _meta.StructField:

        struct_field = _meta.StructField()
        struct_field.fieldType = _meta.TypeDescriptor(basicType=cls.__primitive_types[python_type])
        struct_field.notNull = not optional

        if dc_field is not None and  dc_field.default is not _dc.MISSING:
            struct_field.defaultValue = _meta_types.MetadataCodec.encode_value(dc_field.default)

        if pyd_field is not None and pyd_field.default is not _pyd.fields.PydanticUndefined:
            struct_field.defaultValue = _meta_types.MetadataCodec.encode_value(pyd_field.default)

        return struct_field

    @classmethod
    def _define_generic_field(
            cls, python_type: type, *,
            dc_field: _dc.Field = None,
            pyd_field: "_pyd.fields.FieldInfo" = None) -> _meta.StructField:

        origin = _tp.get_origin(python_type)
        args = _tp.get_args(python_type)

        # types.NoneType not available in Python 3.9, so use type(None) instead
        if origin in cls.__union_types and len(args) == 2 and args[1] is type(None):
            optional_type = args[0]
            return cls._define_primitive_field(optional_type, optional=True, dc_field=dc_field, pyd_field=pyd_field)

        elif origin in [list, _tp.List]:
            list_type = args[0]
            pass

        elif origin in [dict, _tp.Dict]:
            key_type = args[0]
            value_type = args[1]
            pass

        else:
            raise _ex.ETracInternal("Cannot encode field type: " + str(python_type))


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

    def parse(self, config_class: type[__STRUCT_TYPE], config_dict: dict) -> __STRUCT_TYPE:

        # If config is empty, return a default (blank) config
        if config_dict is None or len(config_dict) == 0:
            return config_class()

        config = self._parse_value("", config_dict, config_class)

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

    JSON_FORMAT = "json"
    YAML_FORMAT = "yaml"
    INDENT = 3

    @classmethod
    def quote(cls, obj: _tp.Any, dst: _tp.TextIO, dst_format: str):

        if dst_format.lower() == cls.JSON_FORMAT:
            return cls.quote_json(obj, dst)

        if dst_format.lower() == cls.YAML_FORMAT:
            return cls.quote_yaml(obj, dst)

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
