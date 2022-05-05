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

import typing as tp
import decimal
import datetime as dt

import pyarrow as pa

import tracdap.rt.metadata as meta
import tracdap.rt.exceptions as ex


def decode_value(value: meta.Value) -> tp.Any:

    if value is None or not isinstance(value, meta.Value):
        raise ex.ETracInternal()

    if value.type is None or \
       value.type.basicType is None or \
       value.type.basicType == meta.BasicType.BASIC_TYPE_NOT_SET:

        raise ex.ETracInternal("Missing type information")

    basic_type = value.type.basicType

    if basic_type == meta.BasicType.BOOLEAN:
        return value.booleanValue

    if basic_type == meta.BasicType.INTEGER:
        return value.integerValue

    if basic_type == meta.BasicType.FLOAT:
        return value.floatValue

    if basic_type == meta.BasicType.DECIMAL:
        return decimal.Decimal(value.decimalValue.decimal)

    if basic_type == meta.BasicType.STRING:
        return value.stringValue

    if basic_type == meta.BasicType.DATE:
        return dt.date.fromisoformat(value.dateValue.isoDate)

    if basic_type == meta.BasicType.DATETIME:
        return dt.datetime.fromisoformat(value.datetimeValue.isoDatetime)

    raise ex.ETracInternal(f"Decoding value type [{basic_type}] is not supported yet")


def encode_value(value: tp.Any) -> meta.Value:

    if value is None:
        raise ex.ETracInternal("Cannot encode a null value")

    if isinstance(value, bool):
        type_desc = meta.TypeDescriptor(meta.BasicType.BOOLEAN)
        return meta.Value(type_desc, booleanValue=value)

    if isinstance(value, int):
        type_desc = meta.TypeDescriptor(meta.BasicType.INTEGER)
        return meta.Value(type_desc, integerValue=value)

    if isinstance(value, float):
        type_desc = meta.TypeDescriptor(meta.BasicType.FLOAT)
        return meta.Value(type_desc, floatValue=value)

    if isinstance(value, decimal.Decimal):
        type_desc = meta.TypeDescriptor(meta.BasicType.BOOLEAN)
        return meta.Value(type_desc, decimalValue=meta.DecimalValue(str(value)))

    if isinstance(value, str):
        type_desc = meta.TypeDescriptor(meta.BasicType.STRING)
        return meta.Value(type_desc, stringValue=value)

    if isinstance(value, dt.date):
        type_desc = meta.TypeDescriptor(meta.BasicType.DATE)
        return meta.Value(type_desc, dateValue=meta.DateValue(value.isoformat()))

    if isinstance(value, dt.datetime):
        type_desc = meta.TypeDescriptor(meta.BasicType.DATETIME)
        return meta.Value(type_desc, datetimeValue=meta.DatetimeValue(value.isoformat()))

    raise ex.ETracInternal(f"Encoding value type [{type(value)}] is not supported yet")


def convert_value(raw_value: tp.Any, type_desc: meta.TypeDescriptor):

    if type_desc.basicType == meta.BasicType.BOOLEAN:
        return convert_boolean_value(raw_value)

    if type_desc.basicType == meta.BasicType.INTEGER:
        return convert_integer_value(raw_value)

    if type_desc.basicType == meta.BasicType.FLOAT:
        return convert_float_value(raw_value)

    if type_desc.basicType == meta.BasicType.DECIMAL:
        return convert_decimal_value(raw_value)

    if type_desc.basicType == meta.BasicType.STRING:
        return convert_string_value(raw_value)

    if type_desc.basicType == meta.BasicType.DATE:
        return convert_date_value(raw_value)

    if type_desc.basicType == meta.BasicType.DATETIME:
        return convert_datetime_value(raw_value)

    raise ex.ETracInternal(f"Conversion to value type [{type_desc.basicType.name}] is not supported yet")


def convert_boolean_value(raw_value: tp.Any) -> meta.Value:

    type_desc = meta.TypeDescriptor(meta.BasicType.BOOLEAN)

    if isinstance(raw_value, bool):
        return meta.Value(type_desc, booleanValue=raw_value)

    raise ex.ETracInternal(f"Value of type [{type(raw_value)}] cannot be converted to {meta.BasicType.BOOLEAN.name}")


def convert_integer_value(raw_value: tp.Any) -> meta.Value:

    type_desc = meta.TypeDescriptor(meta.BasicType.INTEGER)

    if isinstance(raw_value, int):
        return meta.Value(type_desc, integerValue=raw_value)

    if isinstance(raw_value, float) and raw_value.is_integer():
        return meta.Value(type_desc, integerValue=int(raw_value))

    raise ex.ETracInternal(f"Value of type [{type(raw_value)}] cannot be converted to {meta.BasicType.INTEGER.name}")


def convert_float_value(raw_value: tp.Any) -> meta.Value:

    type_desc = meta.TypeDescriptor(meta.BasicType.FLOAT)

    if isinstance(raw_value, float):
        return meta.Value(type_desc, floatValue=raw_value)

    if isinstance(raw_value, int):
        return meta.Value(type_desc, floatValue=float(raw_value))

    raise ex.ETracInternal(f"Value of type [{type(raw_value)}] cannot be converted to {meta.BasicType.FLOAT.name}")


def convert_decimal_value(raw_value: tp.Any) -> meta.Value:

    type_desc = meta.TypeDescriptor(meta.BasicType.DECIMAL)

    if isinstance(raw_value, decimal.Decimal):
        return meta.Value(type_desc, decimalValue=meta.DecimalValue(str(raw_value)))

    if isinstance(raw_value, int) or isinstance(raw_value, float):
        return meta.Value(type_desc, decimalValue=meta.DecimalValue(str(raw_value)))

    raise ex.ETracInternal(f"Value of type [{type(raw_value)}] cannot be converted to {meta.BasicType.DECIMAL.name}")


def convert_string_value(raw_value: tp.Any) -> meta.Value:

    type_desc = meta.TypeDescriptor(meta.BasicType.STRING)

    if isinstance(raw_value, str):
        return meta.Value(type_desc, stringValue=raw_value)

    if isinstance(raw_value, bool) or \
       isinstance(raw_value, int) or \
       isinstance(raw_value, float) or \
       isinstance(raw_value, decimal.Decimal):

        return meta.Value(type_desc, stringValue=str(raw_value))

    raise ex.ETracInternal(f"Value of type [{type(raw_value)}] cannot be converted to {meta.BasicType.STRING.name}")


def convert_date_value(raw_value: tp.Any) -> meta.Value:

    type_desc = meta.TypeDescriptor(meta.BasicType.DATE)

    if isinstance(raw_value, dt.date):
        return meta.Value(type_desc, dateValue=meta.DateValue(isoDate=raw_value.isoformat()))

    if isinstance(raw_value, str):
        date_value = dt.date.fromisoformat(raw_value)
        return meta.Value(type_desc, dateValue=meta.DateValue(isoDate=date_value.isoformat()))

    raise ex.ETracInternal(f"Value of type [{type(raw_value)}] cannot be converted to {meta.BasicType.DATE.name}")


def convert_datetime_value(raw_value: tp.Any) -> meta.Value:

    type_desc = meta.TypeDescriptor(meta.BasicType.DATETIME)

    if isinstance(raw_value, dt.datetime):
        return meta.Value(type_desc, datetimeValue=meta.DatetimeValue(isoDatetime=raw_value.isoformat()))

    if isinstance(raw_value, str):
        datetime_value = dt.datetime.fromisoformat(raw_value)
        return meta.Value(type_desc, datetimeValue=meta.DatetimeValue(isoDatetime=datetime_value.isoformat()))

    raise ex.ETracInternal(f"Value of type [{type(raw_value)}] cannot be converted to {meta.BasicType.DATETIME.name}")


# Matches TRAC_ARROW_TYPE_MAPPING in ArrowSchema, tracdap-lib-data

__TRAC_DECIMAL_PRECISION = 38
__TRAC_DECIMAL_SCALE = 12
__TRAC_TIMESTAMP_UNIT = "ms"
__TRAC_TIMESTAMP_ZONE = None

__TRAC_TO_ARROW_TYPE_MAPPING = {
    meta.BasicType.BOOLEAN: pa.bool_(),
    meta.BasicType.INTEGER: pa.int64(),
    meta.BasicType.FLOAT: pa.float64(),
    meta.BasicType.DECIMAL: pa.decimal128(__TRAC_DECIMAL_PRECISION, __TRAC_DECIMAL_SCALE),
    meta.BasicType.STRING: pa.utf8(),
    meta.BasicType.DATE: pa.date32(),
    meta.BasicType.DATETIME: pa.timestamp(__TRAC_TIMESTAMP_UNIT, __TRAC_TIMESTAMP_ZONE)
}


def arrow_to_python_type(arrow_type: pa.DataType) -> type:

    if pa.types.is_boolean(arrow_type):
        return bool

    if pa.types.is_integer(arrow_type):
        return int

    if pa.types.is_floating(arrow_type):
        return float

    if pa.types.is_decimal(arrow_type):
        return decimal.Decimal

    if pa.types.is_string(arrow_type):
        return str

    if pa.types.is_date(arrow_type):
        return dt.date

    if pa.types.is_timestamp(arrow_type):
        return dt.datetime

    raise ex.EData(f"No Python type conversion available for Arrow type {arrow_type}")  # TODO


def python_to_arrow_type(python_type: type) -> pa.DataType:

    if python_type == bool:
        return pa.bool_()

    if python_type == int:
        return pa.int64()

    if python_type == float:
        return pa.float64()

    if python_type == decimal.Decimal:
        return pa.decimal128(__TRAC_DECIMAL_PRECISION, __TRAC_DECIMAL_SCALE)

    if python_type == str:
        return pa.utf8()

    if python_type == dt.date:
        return pa.date32()

    if python_type == dt.datetime:
        return pa.timestamp(__TRAC_TIMESTAMP_UNIT, __TRAC_TIMESTAMP_ZONE)

    raise ex.EData(f"No Arrow type conversion available for Python type {python_type}")  # TODO


def trac_to_arrow_schema(trac_schema: meta.SchemaDefinition) -> pa.Schema:

    if trac_schema.schemaType != meta.SchemaType.TABLE:
        raise ex.ETracInternal(f"Schema type [{trac_schema.schemaType}] cannot be converted for Apache Arrow")

    arrow_fields = [
        (f.fieldName, trac_to_arrow_basic_type(f.fieldType))
        for f in trac_schema.table.fields]

    return pa.schema(arrow_fields, metadata={})


def trac_to_arrow_type(trac_type: meta.TypeDescriptor) -> pa.DataType:

    return trac_to_arrow_basic_type(trac_type.basicType)


def trac_to_arrow_basic_type(trac_basic_type: meta.BasicType) -> pa.DataType:

    arrow_type = __TRAC_TO_ARROW_TYPE_MAPPING.get(trac_basic_type)

    if arrow_type is None:
        raise ex.ETracInternal(f"Cannot convert TRAC data type [{trac_basic_type}] for use with Apache Arrow")

    return arrow_type


def trac_arrow_decimal_type() -> pa.Decimal128Type:
    return pa.decimal128(__TRAC_DECIMAL_PRECISION, __TRAC_DECIMAL_SCALE)
