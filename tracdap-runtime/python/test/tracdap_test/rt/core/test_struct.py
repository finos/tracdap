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

import dataclasses as dc
import datetime as dt
import enum
import decimal
import sys
import typing as tp
import unittest

try:
    import pydantic as pyd  # noqa
except ModuleNotFoundError:
    pyd = None

import tracdap.rt.api.experimental as _api
import tracdap.rt.exceptions as _ex
import tracdap.rt.metadata as _meta
import tracdap.rt._impl.core.struct as _struct  # noqa
import tracdap.rt._impl.core.type_system as _meta_types  # noqa

if sys.version_info >= (3, 10):
    python_310_compat = True
else:
    python_310_compat = False


_DEFAULT_DATE = dt.date.today()
_DEFAULT_DATETIME = dt.datetime.now(dt.timezone.utc)

def default_date():
    return _DEFAULT_DATE

def default_datetime():
    return _DEFAULT_DATETIME

class ExampleEnum(enum.Enum):
    VALUE1 = 1
    VALUE2 = 2
    VALUE3 = 3

@dc.dataclass(frozen=True)
class DataClassSubStruct:

    field1: str
    field2: int
    enumField: tp.Optional[ExampleEnum]

@dc.dataclass(frozen=True)
class DataclassStruct:

    boolField: bool = True
    intField: int = 1
    floatField: float = 1.0
    decimalField: decimal.Decimal = dc.field(default=decimal.Decimal(1.0))
    strField: str = "hello"
    dateField: dt.date = dc.field(default_factory=default_date)
    datetimeField: dt.datetime = dc.field(default_factory=default_datetime)

    enumField: ExampleEnum = ExampleEnum.VALUE1

    quotedField: "str" = dc.field(default="hello")
    optionalField: tp.Optional[str] = dc.field(default=None)
    optionalQuotedField: "tp.Optional[str]" = None

    listField: tp.List[int] = dc.field(default_factory=list)
    dictField: tp.Dict[str, dt.datetime] = dc.field(default_factory=dict)

    structField: tp.Optional[DataClassSubStruct] = None
    nestedStructField: tp.Dict[str, DataClassSubStruct] = dc.field(default_factory=dict)
    nestedOptionalStructField: tp.Dict[str, tp.Optional[DataClassSubStruct]] = dc.field(default_factory=dict)

@dc.dataclass(frozen=True)
class DataClassRecursive:

    field1: str
    field2: int

    recursiveField: "tp.Optional[DataClassRecursive]" = dc.field(default=None)

if python_310_compat:

    @dc.dataclass(frozen=True)
    class NewUnionStruct:

        boolField: bool = True
        intField: int = 1
        floatField: float = 1.0
        decimalField: decimal.Decimal = dc.field(default=decimal.Decimal(1.0))
        strField: str = "hello"
        dateField: dt.date = dc.field(default_factory=default_date)
        datetimeField: dt.datetime = dc.field(default_factory=default_datetime)

        enumField: ExampleEnum = ExampleEnum.VALUE1

        quotedField: "str" = dc.field(default="hello")
        optionalField: str | None = dc.field(default=None)
        optionalQuotedField: "None | str" = None

        listField: list[int] = dc.field(default_factory=list)
        dictField: dict[str, dt.datetime] = dc.field(default_factory=dict)

        structField: DataClassSubStruct | None = None
        nestedStructField: dict[str, DataClassSubStruct] = dc.field(default_factory=dict)
        nestedOptionalStructField: dict[str, DataClassSubStruct | None] = dc.field(default_factory=dict)

if pyd:

    class PydanticSubStruct(pyd.BaseModel):

        field1: str
        field2: int
        enumField: tp.Optional[ExampleEnum]

    class PydanticStruct(pyd.BaseModel):

        boolField: bool = True
        intField: int = 1
        floatField: float = 1.0
        decimalField: decimal.Decimal = pyd.Field(default=decimal.Decimal(1.0))
        strField: str = "hello"
        dateField: dt.date = pyd.Field(default_factory=default_date)
        datetimeField: dt.datetime = pyd.Field(default_factory=default_datetime)

        enumField: ExampleEnum = ExampleEnum.VALUE1

        quotedField: "str" = pyd.Field(default="hello")
        optionalField: tp.Optional[str] = pyd.Field(default=None)
        optionalQuotedField: "tp.Optional[str]" = None

        listField: tp.List[int] = pyd.Field(default_factory=list)
        dictField: tp.Dict[str, dt.datetime] = pyd.Field(default_factory=dict)

        structField: tp.Optional[PydanticSubStruct] = None
        nestedStructField: tp.Dict[str, PydanticSubStruct] = pyd.Field(default_factory=dict)
        nestedOptionalStructField: tp.Dict[str, tp.Optional[PydanticSubStruct]] = pyd.Field(default_factory=dict)

    class PydanticRecursive(pyd.BaseModel):

        field1: str
        field2: int

        recursiveField: "tp.Optional[PydanticRecursive]" = pyd.Field(default=None)

# Python 3.9 doesn't allow calling static methods at class scope to init class static vars
def define_struct_schema(sub_struct_type: type):

    return _meta.SchemaDefinition(
        schemaType=_meta.SchemaType.STRUCT_SCHEMA,
        struct=_meta.StructSchema(fields=[

            _meta.FieldSchema(
                fieldName="boolField", fieldOrder=0, fieldType=_meta.BasicType.BOOLEAN, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value(True)),

            _meta.FieldSchema(
                fieldName="intField", fieldOrder=1, fieldType=_meta.BasicType.INTEGER, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value(1)),

            _meta.FieldSchema(
                fieldName="floatField", fieldOrder=2, fieldType=_meta.BasicType.FLOAT, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value(1.0)),

            _meta.FieldSchema(
                fieldName="decimalField", fieldOrder=3, fieldType=_meta.BasicType.DECIMAL, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value(decimal.Decimal(1.0))),

            _meta.FieldSchema(
                fieldName="strField", fieldOrder=4, fieldType=_meta.BasicType.STRING, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value("hello")),

            _meta.FieldSchema(
                fieldName="dateField", fieldOrder=5, fieldType=_meta.BasicType.DATE, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value(_DEFAULT_DATE)),

            _meta.FieldSchema(
                fieldName="datetimeField", fieldOrder=6, fieldType=_meta.BasicType.DATETIME, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value(_DEFAULT_DATETIME)),

            _meta.FieldSchema(
                fieldName="enumField", fieldOrder=7, fieldType=_meta.BasicType.STRING, categorical=True, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value(ExampleEnum.VALUE1.name),
                namedEnum=f"{ExampleEnum.__module__}.{ExampleEnum.__name__}"),

            _meta.FieldSchema(
                fieldName="quotedField", fieldOrder=8, fieldType=_meta.BasicType.STRING, notNull=True,
                label="", defaultValue=_meta_types.MetadataCodec.encode_value("hello")),

            _meta.FieldSchema(
                fieldName="optionalField", fieldOrder=9, fieldType=_meta.BasicType.STRING, notNull=False,
                label="", defaultValue=None),

            _meta.FieldSchema(
                fieldName="optionalQuotedField", fieldOrder=10, fieldType=_meta.BasicType.STRING, notNull=False,
                label="", defaultValue=None),

            _meta.FieldSchema(
                fieldName="listField",
                fieldOrder=11,
                fieldType=_meta.BasicType.ARRAY,
                label="",
                notNull=True,
                children=[
                    _meta.FieldSchema(
                        fieldName="item",
                        fieldOrder=0,
                        fieldType=_meta.BasicType.INTEGER,
                        label="",
                        notNull=True)
                ]),

            _meta.FieldSchema(
                fieldName="dictField",
                fieldOrder=12,
                fieldType=_meta.BasicType.MAP,
                label="",
                notNull=True,
                children=[
                    _meta.FieldSchema(
                        fieldName="key",
                        fieldOrder=0,
                        fieldType=_meta.BasicType.STRING,
                        label="",
                        notNull=True),
                    _meta.FieldSchema(
                        fieldName="value",
                        fieldOrder=1,
                        fieldType=_meta.BasicType.DATETIME,
                        label="",
                        notNull=True),
                ]),

            _meta.FieldSchema(
                fieldName="structField",
                fieldOrder=13,
                fieldType=_meta.BasicType.STRUCT,
                label="",
                notNull=False,
                namedType=f"{sub_struct_type.__module__}.{sub_struct_type.__name__}"),

            _meta.FieldSchema(
                fieldName="nestedStructField",
                fieldOrder=14,
                fieldType=_meta.BasicType.MAP,
                label="",
                notNull=True,
                children=[
                    _meta.FieldSchema(
                        fieldName="key",
                        fieldOrder=0,
                        fieldType=_meta.BasicType.STRING,
                        label="",
                        notNull=True),
                    _meta.FieldSchema(
                        fieldName="value",
                        fieldOrder=1,
                        fieldType=_meta.BasicType.STRUCT,
                        label="",
                        notNull=True,
                        namedType=f"{sub_struct_type.__module__}.{sub_struct_type.__name__}")]),

            _meta.FieldSchema(
                fieldName="nestedOptionalStructField",
                fieldOrder=15,
                fieldType=_meta.BasicType.MAP,
                label="",
                notNull=True,
                children=[
                    _meta.FieldSchema(
                        fieldName="key",
                        fieldOrder=0,
                        fieldType=_meta.BasicType.STRING,
                        label="",
                        notNull=True),
                    _meta.FieldSchema(
                        fieldName="value",
                        fieldOrder=1,
                        fieldType=_meta.BasicType.STRUCT,
                        label="",
                        notNull=False,
                        namedType=f"{sub_struct_type.__module__}.{sub_struct_type.__name__}")])]),

        namedTypes={
            f"{sub_struct_type.__module__}.{sub_struct_type.__name__}": _meta.SchemaDefinition(
                schemaType=_meta.SchemaType.STRUCT_SCHEMA,
                struct=_meta.StructSchema(fields=[
                    _meta.FieldSchema(
                        fieldName="field1", fieldOrder=0, fieldType=_meta.BasicType.STRING, notNull=True,
                        label="", defaultValue=None),

                    _meta.FieldSchema(
                        fieldName="field2", fieldOrder=1, fieldType=_meta.BasicType.INTEGER, notNull=True,
                        label="", defaultValue=None),

                    _meta.FieldSchema(
                        fieldName="enumField", fieldOrder=2, fieldType=_meta.BasicType.STRING, categorical=True, notNull=False,
                        label="", namedEnum=f"{ExampleEnum.__module__}.{ExampleEnum.__name__}")]))},

        namedEnums={
            f"{ExampleEnum.__module__}.{ExampleEnum.__name__}": _meta.EnumValues(
                values=["VALUE1", "VALUE2", "VALUE3"]
            )
        }
    )


class StructProcessingTest(unittest.TestCase):

    _api.init_static()

    DATACLASS_STRUCT_SCHEMA = define_struct_schema(DataClassSubStruct)
    PYDANTIC_STRUCT_SCHEMA = define_struct_schema(PydanticSubStruct) if pyd is not None else None


    def test_define_dataclass_struct(self):

        struct_schema = _struct.StructProcessor.define_struct(DataclassStruct)

        self.assertIsNotNone(struct_schema)
        self._assert_schema_equal(self.DATACLASS_STRUCT_SCHEMA, struct_schema)

    @unittest.skipIf(not python_310_compat, "Python >= 3.10 is required to support the new typing syntax")
    def test_define_dataclass_310_struct(self):

        struct_schema = _struct.StructProcessor.define_struct(NewUnionStruct)

        self.assertIsNotNone(struct_schema)
        self._assert_schema_equal(self.DATACLASS_STRUCT_SCHEMA, struct_schema)

    def test_define_dataclass_recursive(self):

        self.assertRaises(_ex.EValidation, lambda: _struct.StructProcessor.define_struct(DataClassRecursive))

    @unittest.skipIf(pyd is None, "Pydantic is not installed")
    def test_define_pydantic_struct(self):

        struct_schema = _struct.StructProcessor.define_struct(PydanticStruct)

        self.assertIsNotNone(struct_schema)
        self._assert_schema_equal(self.PYDANTIC_STRUCT_SCHEMA, struct_schema)

    @unittest.skipIf(pyd is None, "Pydantic is not installed")
    def test_define_pydantic_recursive(self):

        self.assertRaises(_ex.EValidation, lambda: _struct.StructProcessor.define_struct(PydanticRecursive))

    def _assert_schema_equal(self, expected: _meta.SchemaDefinition, actual: _meta.SchemaDefinition):

        actual_fields = {f.fieldName: f for f in actual.struct.fields}
        expected_fields = {f.fieldName: f for f in expected.struct.fields}

        self.assertSetEqual(set(expected_fields.keys()), set(actual_fields.keys()))

        for key, expected_field in expected_fields.items():

            actual_field = actual_fields.get(key)

            self.assertIsNotNone(actual_field, "key = " + key)
            self.assertEqual(key, actual_field.fieldName)
            self.assertEqual(expected_field, actual_field)

        self.assertSetEqual(set(expected.namedEnums.keys()), set(actual.namedEnums.keys()))

        for key, named_enum in expected.namedEnums.items():

            actual_named_enum = actual.namedEnums.get(key)

            self.assertIsNotNone(actual_named_enum, "key = " + key)
            self.assertListEqual(named_enum.values, actual_named_enum.values)

        self.assertSetEqual(set(expected.namedTypes.keys()), set(actual.namedTypes.keys()))

        for key, named_type in expected.namedTypes.items():

            actual_named_type= actual.namedTypes.get(key)

            self.assertIsNotNone(actual_named_type, "key = " + key)
            self._assert_schema_equal(named_type, actual_named_type)
