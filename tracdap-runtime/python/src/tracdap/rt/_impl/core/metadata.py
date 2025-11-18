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

import typing as tp
import decimal
import datetime as dt

import tracdap.rt.exceptions as _ex

from tracdap.rt.metadata import *


class MetadataTypes:

    """
    Map types between the TRAC metadata type system and native Python types.

    To map individual values, use :py:class:`MetadataCodec`.
    To map primary data, use :py:class:`DataMapping <tracdap.rt.impl.data.DataMapping>`.
    """

    __PRIMITIVE_TYPES: tp.List[BasicType] = [
        BasicType.BOOLEAN,
        BasicType.INTEGER,
        BasicType.FLOAT,
        BasicType.DECIMAL,
        BasicType.STRING,
        BasicType.DATE,
        BasicType.DATETIME
    ]

    __TRAC_TO_PYTHON_BASIC_TYPE: tp.Dict[BasicType, type] = {
        BasicType.BOOLEAN: bool,
        BasicType.INTEGER: int,
        BasicType.FLOAT: float,
        BasicType.DECIMAL: decimal.Decimal,
        BasicType.STRING: str,
        BasicType.DATE: dt.date,
        BasicType.DATETIME: dt.datetime
    }

    __PYTHON_TO_TRAC_BASIC_TYPE: tp.Dict[type, BasicType] = {
        bool: BasicType.BOOLEAN,
        int: BasicType.INTEGER,
        float: BasicType.FLOAT,
        decimal.Decimal: BasicType.DECIMAL,
        str: BasicType.STRING,
        dt.date: BasicType.DATE,
        dt.datetime: BasicType.DATETIME
    }

    @classmethod
    def is_primitive(cls, trac_type: tp.Union[BasicType, TypeDescriptor]) -> bool:

        if isinstance(trac_type, BasicType):
            return trac_type in cls.__PRIMITIVE_TYPES
        else:
            return trac_type.basicType in cls.__PRIMITIVE_TYPES

    @classmethod
    def trac_to_python(cls, trac_type: TypeDescriptor) -> type:

        return cls.trac_to_python_basic_type(trac_type.basicType)

    @classmethod
    def trac_to_python_basic_type(cls, trac_basic_type: BasicType) -> type:

        python_type = cls.__TRAC_TO_PYTHON_BASIC_TYPE.get(trac_basic_type)

        if python_type is None:
            raise _ex.ETracInternal(f"No Python type mapping available for TRAC type [{trac_basic_type}]")

        return python_type

    @classmethod
    def python_to_trac(cls, python_type: type) -> TypeDescriptor:

        basic_type = cls.python_to_trac_basic_type(python_type)
        return TypeDescriptor(basic_type)

    @classmethod
    def python_to_trac_basic_type(cls, python_type: type) -> BasicType:

        basic_type = cls.__PYTHON_TO_TRAC_BASIC_TYPE.get(python_type)

        if basic_type is None:
            raise _ex.ETracInternal(f"No TRAC type mapping available for Python type [{python_type}]")

        return basic_type


class MetadataCodec:

    """
    Map values between TRAC metadata :py:class:`Value <tracdap.rt.metadata.Value>` objects and native Python values.

    To map types, use :py:class:`TypeMapping`.
    To map primary data, use :py:class:`DataMapping <tracdap.rt.impl.data.DataMapping>`.
    """

    @staticmethod
    def decode_value(value: Value) -> tp.Any:

        if value is None or not isinstance(value, Value):
            raise _ex.ETracInternal()

        if value.type is None or \
           value.type.basicType is None or \
           value.type.basicType == BasicType.BASIC_TYPE_NOT_SET:

            raise _ex.ETracInternal("Missing type information")

        return MetadataCodec._decode_value_for_type(value, value.type)

    @staticmethod
    def decode_date_value(value: DateValue) -> dt.date:

        if value is None or not isinstance(value, DateValue):
            raise _ex.ETracInternal()

        return dt.date.fromisoformat(value.isoDate)

    @staticmethod
    def decode_datetime_value(value: DatetimeValue) -> dt.datetime:

        if value is None or not isinstance(value, DatetimeValue):
            raise _ex.ETracInternal()

        return dt.datetime.fromisoformat(value.isoDatetime)

    @staticmethod
    def _decode_value_for_type(value: Value, type_desc: TypeDescriptor):

        basic_type = type_desc.basicType

        if basic_type == BasicType.BOOLEAN:
            return value.booleanValue

        if basic_type == BasicType.INTEGER:
            return value.integerValue

        if basic_type == BasicType.FLOAT:
            return value.floatValue

        if basic_type == BasicType.DECIMAL:
            return decimal.Decimal(value.decimalValue.decimal)

        if basic_type == BasicType.STRING:
            return value.stringValue

        if basic_type == BasicType.DATE:
            return dt.date.fromisoformat(value.dateValue.isoDate)

        if basic_type == BasicType.DATETIME:
            return dt.datetime.fromisoformat(value.datetimeValue.isoDatetime)

        if basic_type == BasicType.ARRAY:
            items = value.arrayValue.items
            return list(MetadataCodec._decode_value_for_type(x, type_desc.arrayType) for x in items)

        if basic_type == BasicType.MAP:
            items = value.mapValue.entries.items()
            return dict((k, MetadataCodec._decode_value_for_type(v, type_desc.mapType)) for k, v in items)

        raise _ex.ETracInternal(f"Cannot decode value of type [{basic_type}]")

    @classmethod
    def encode_value(cls, value: tp.Any) -> Value:

        if value is None:
            raise _ex.ETracInternal("Cannot encode a null value")

        if isinstance(value, bool):
            type_desc = TypeDescriptor(BasicType.BOOLEAN)
            return Value(type_desc, booleanValue=value)

        if isinstance(value, int):
            type_desc = TypeDescriptor(BasicType.INTEGER)
            return Value(type_desc, integerValue=value)

        if isinstance(value, float):
            type_desc = TypeDescriptor(BasicType.FLOAT)
            return Value(type_desc, floatValue=value)

        if isinstance(value, decimal.Decimal):
            type_desc = TypeDescriptor(BasicType.DECIMAL)
            return Value(type_desc, decimalValue=DecimalValue(str(value)))

        if isinstance(value, str):
            type_desc = TypeDescriptor(BasicType.STRING)
            return Value(type_desc, stringValue=value)

        # dt.datetime inherits dt.date, so check datetime first to avoid encoding datetime as a date
        if isinstance(value, dt.datetime):
            type_desc = TypeDescriptor(BasicType.DATETIME)
            return Value(type_desc, datetimeValue=DatetimeValue(value.isoformat()))

        if isinstance(value, dt.date):
            type_desc = TypeDescriptor(BasicType.DATE)
            return Value(type_desc, dateValue=DateValue(value.isoformat()))

        if isinstance(value, list):

            if len(value) == 0:
                raise _ex.ETracInternal("Cannot encode an empty list")

            array_raw_type = type(value[0])
            array_trac_type = MetadataTypes.python_to_trac(array_raw_type)

            if any(map(lambda x: type(x) != array_raw_type, value)):
                raise _ex.ETracInternal("Cannot encode a list with values of different types")

            encoded_items = list(map(lambda x: cls.convert_value(x, array_trac_type, True), value))

            return Value(
                TypeDescriptor(BasicType.ARRAY, arrayType=array_trac_type),
                arrayValue=ArrayValue(encoded_items))

        if isinstance(value, dict):

            if len(value) == 0:
                raise _ex.ETracInternal("Cannot encode an empty dict")

            map_raw_type = type(next(iter(value.values())))
            map_trac_type = MetadataTypes.python_to_trac(map_raw_type)

            if any(map(lambda x: type(x) != array_raw_type, value.values())):
                raise _ex.ETracInternal("Cannot encode a dict with values of different types")

            encoded_entries = dict(map(lambda kv: (kv[0], cls.convert_value(kv[1], map_trac_type, True)), value.items()))

            return Value(
                TypeDescriptor(BasicType.ARRAY, mapType=map_trac_type),
                mapValue=MapValue(encoded_entries))

        raise _ex.ETracInternal(f"Cannot encode value of type [{type(value).__name__}]")

    @classmethod
    def convert_value(cls, raw_value: tp.Any, type_desc: TypeDescriptor, nested: bool = False):

        if type_desc.basicType == BasicType.BOOLEAN:
            return cls.convert_boolean_value(raw_value, nested)

        if type_desc.basicType == BasicType.INTEGER:
            return cls.convert_integer_value(raw_value)

        if type_desc.basicType == BasicType.FLOAT:
            return cls.convert_float_value(raw_value)

        if type_desc.basicType == BasicType.DECIMAL:
            return cls.convert_decimal_value(raw_value)

        if type_desc.basicType == BasicType.STRING:
            return cls.convert_string_value(raw_value)

        if type_desc.basicType == BasicType.DATE:
            return cls.convert_date_value(raw_value)

        if type_desc.basicType == BasicType.DATETIME:
            return cls.convert_datetime_value(raw_value)

        if type_desc.basicType == BasicType.ARRAY:
            return cls.convert_array_value(raw_value, type_desc.arrayType)

        if type_desc.basicType == BasicType.MAP:
            return cls.convert_map_value(raw_value, type_desc.mapType)

        raise _ex.ETracInternal(f"Conversion to value type [{type_desc.basicType.name}] is not supported yet")

    @staticmethod
    def convert_array_value(raw_value: tp.List[tp.Any], array_type: TypeDescriptor) -> Value:

        type_desc = TypeDescriptor(basicType=BasicType.ARRAY, arrayType=array_type)

        if not isinstance(raw_value, list):
            msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.ARRAY.name}"
            raise _ex.ETracInternal(msg)

        items = list(map(lambda x: MetadataCodec.convert_value(x, array_type, True), raw_value))

        return Value(type_desc, arrayValue=ArrayValue(items))

    @staticmethod
    def convert_map_value(raw_value: tp.Dict[str, tp.Any], map_type: TypeDescriptor) -> Value:

        type_desc = TypeDescriptor(basicType=BasicType.MAP, mapType=map_type)

        if not isinstance(raw_value, dict):
            msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.MAP.name}"
            raise _ex.ETracInternal(msg)

        entries = dict(map(lambda kv: (kv[0], MetadataCodec.convert_value(kv[1], map_type, True)), raw_value.items()))

        return Value(type_desc, mapValue=MapValue(entries))

    @staticmethod
    def convert_boolean_value(raw_value: tp.Any, nested: bool = False) -> Value:

        type_desc = TypeDescriptor(BasicType.BOOLEAN) if not nested else None

        if isinstance(raw_value, bool):
            return Value(type_desc, booleanValue=raw_value)

        msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.BOOLEAN.name}"
        raise _ex.ETracInternal(msg)

    @staticmethod
    def convert_integer_value(raw_value: tp.Any, nested: bool = False) -> Value:

        type_desc = TypeDescriptor(BasicType.INTEGER) if not nested else None

        # isinstance(bool_value, int) returns True! An explicit check is needed
        if isinstance(raw_value, int) and not isinstance(raw_value, bool):
            return Value(type_desc, integerValue=raw_value)

        if isinstance(raw_value, float) and raw_value.is_integer():
            return Value(type_desc, integerValue=int(raw_value))

        msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.INTEGER.name}"
        raise _ex.ETracInternal(msg)

    @staticmethod
    def convert_float_value(raw_value: tp.Any, nested: bool = False) -> Value:

        type_desc = TypeDescriptor(BasicType.FLOAT) if not nested else None

        if isinstance(raw_value, float):
            return Value(type_desc, floatValue=raw_value)

        # isinstance(bool_value, int) returns True! An explicit check is needed
        if isinstance(raw_value, int) and not isinstance(raw_value, bool):
            return Value(type_desc, floatValue=float(raw_value))

        msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.FLOAT.name}"
        raise _ex.ETracInternal(msg)

    @staticmethod
    def convert_decimal_value(raw_value: tp.Any, nested: bool = False) -> Value:

        type_desc = TypeDescriptor(BasicType.DECIMAL) if not nested else None

        if isinstance(raw_value, decimal.Decimal):
            return Value(type_desc, decimalValue=DecimalValue(str(raw_value)))

        # isinstance(bool_value, int) returns True! An explicit check is needed
        if isinstance(raw_value, int) or isinstance(raw_value, float) and not isinstance(raw_value, bool):
            return Value(type_desc, decimalValue=DecimalValue(str(raw_value)))

        msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.DECIMAL.name}"
        raise _ex.ETracInternal(msg)

    @staticmethod
    def convert_string_value(raw_value: tp.Any, nested: bool = False) -> Value:

        type_desc = TypeDescriptor(BasicType.STRING) if not nested else None

        if isinstance(raw_value, str):
            return Value(type_desc, stringValue=raw_value)

        if isinstance(raw_value, bool) or \
           isinstance(raw_value, int) or \
           isinstance(raw_value, float) or \
           isinstance(raw_value, decimal.Decimal):

            return Value(type_desc, stringValue=str(raw_value))

        msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.STRING.name}"
        raise _ex.ETracInternal(msg)

    @staticmethod
    def convert_date_value(raw_value: tp.Any, nested: bool = False) -> Value:

        type_desc = TypeDescriptor(BasicType.DATE) if not nested else None

        if isinstance(raw_value, dt.date):
            return Value(type_desc, dateValue=DateValue(isoDate=raw_value.isoformat()))

        if isinstance(raw_value, str):
            date_value = dt.date.fromisoformat(raw_value)
            return Value(type_desc, dateValue=DateValue(isoDate=date_value.isoformat()))

        msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.DATE.name}"
        raise _ex.ETracInternal(msg)

    @staticmethod
    def convert_datetime_value(raw_value: tp.Any, nested: bool = False) -> Value:

        type_desc = TypeDescriptor(BasicType.DATETIME) if not nested else None

        if isinstance(raw_value, dt.datetime):
            return Value(type_desc, datetimeValue=DatetimeValue(isoDatetime=raw_value.isoformat()))

        if isinstance(raw_value, str):
            datetime_value = dt.datetime.fromisoformat(raw_value)
            return Value(type_desc, datetimeValue=DatetimeValue(isoDatetime=datetime_value.isoformat()))

        msg = f"Value of type [{type(raw_value).__name__}] cannot be converted to {BasicType.DATETIME.name}"
        raise _ex.ETracInternal(msg)
