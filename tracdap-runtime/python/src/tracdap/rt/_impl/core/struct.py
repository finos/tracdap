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
    import pydantic as _pydantic  # noqa
except ModuleNotFoundError:
    _pydantic = None

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.type_system as _meta_types
import tracdap.rt._impl.core.config_parser as _cfg_p


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

        if _pydantic and issubclass(python_type, _pydantic.BaseModel):
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

        # Use existing config parser impl to build dataclasses
        if _dc.is_dataclass(python_type):
            return _cfg_p.ConfigParser(python_type)._parse_value("", data, python_type)

        # For Pydantic types, build models using model_construct()
        if _pydantic and isinstance(python_type, type) and issubclass(python_type, _pydantic.BaseModel):
            return python_type.model_construct(**data)

        raise _ex.ETracInternal("Python type must be a dataclass or a pydantic model")

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
    def _define_struct_for_pydantic(cls, python_type: "type[_pydantic.BaseModel]") -> _meta.StructSchema:

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
            pyd_field: "_pydantic.fields.FieldInfo" = None) \
            -> _meta.StructField:

        if python_type in cls.__primitive_types:
            return cls._define_primitive_field(python_type, dc_field=dc_field, pyd_field=pyd_field)

        elif any(map(lambda _t: isinstance(python_type, _t), cls.__generic_types)):
            return cls._define_generic_field(python_type, pyd_field=pyd_field)

        elif dc_field is not None and _dc.is_dataclass(python_type):
            pass

        elif pyd_field is not None and issubclass(python_type, _pydantic.BaseModel):
            pass

        else:
            raise _ex.ETracInternal("Cannot encode field type: " + str(python_type))

    @classmethod
    def _define_primitive_field(
            cls, python_type: type, optional=False, *,
            dc_field: _dc.Field = None,
            pyd_field: "_pydantic.fields.FieldInfo" = None) \
            -> _meta.StructField:

        struct_field = _meta.StructField()
        struct_field.fieldType = _meta.TypeDescriptor(basicType=cls.__primitive_types[python_type])
        struct_field.notNull = not optional

        if dc_field is not None and  dc_field.default is not _dc.MISSING:
            struct_field.defaultValue = _meta_types.MetadataCodec.encode_value(dc_field.default)

        if pyd_field is not None and pyd_field.default is not _pydantic.fields.PydanticUndefined:
            struct_field.defaultValue = _meta_types.MetadataCodec.encode_value(pyd_field.default)

        return struct_field

    @classmethod
    def _define_generic_field(
            cls, python_type: type, *,
            dc_field: _dc.Field = None,
            pyd_field: "_pydantic.fields.FieldInfo" = None) -> _meta.StructField:

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


class StructQuoter:

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
            elif _dc.is_dataclass(o):
                return o.__dict__
            elif _pydantic and isinstance(o, _pydantic.BaseModel):
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
            elif _pydantic and isinstance(data, _pydantic.BaseModel):
                return self.represent_dict(data.__dict__)
            else:
                return super().represent_data(data)
