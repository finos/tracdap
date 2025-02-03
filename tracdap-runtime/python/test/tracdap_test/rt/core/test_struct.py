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

import unittest
import dataclasses as dc
import sys
import typing as tp

try:
    import pydantic as pyd
except ModuleNotFoundError:
    pyd = None

import tracdap.rt.metadata as _meta
import tracdap.rt._impl.core.struct as _struct  # noqa

if sys.version_info >= (3, 10):
    new_union_syntax = True
else:
    new_union_syntax = False


class PurePythonStruct:

    def __init__(self, field1: str, field2: float, field3: bool = True):
        self.field1 = field1
        self.field2 = field2
        self.field3 = field3

@dc.dataclass(frozen=True)
class DataclassStruct:

    fieldX: tp.Optional[str] = None
    fieldZ: "str" = "hello"
    field0: dict[str, float] = dc.field(default_factory=dict)
    field1: str = "hello"
    field2: float = dc.field(default=1.2)
    field3: bool = True

if pyd:

    class PydanticStruct(pyd.BaseModel):
        field1: str
        field2: float
        field3: bool

if new_union_syntax:

    @dc.dataclass(frozen=True)
    class NewUnionStruct:
        field1: tp.Optional[str] = None
        field2: str | None = None
        field3: "tp.Optional[str]" = None
        field4: "str | None" = None


class StructProcessingTest(unittest.TestCase):

    def test_define_pure_python_struct(self):

        struct_schema = _struct.StructProcessor.define_struct(PurePythonStruct)
        self.assertIsNotNone(struct_schema, _meta.StructSchema)

    def test_define_dataclass_struct(self):

        struct_schema = _struct.StructProcessor.define_struct(DataclassStruct)
        self.assertIsNotNone(struct_schema, _meta.StructSchema)

    @unittest.skipIf(pyd is None, "Pydantic is not installed")
    def test_define_pydantic_struct(self):

        struct_schema = _struct.StructProcessor.define_struct(PydanticStruct)
        self.assertIsNotNone(struct_schema, _meta.StructSchema)

    @unittest.skipIf(not new_union_syntax, "Python >= 3.10 is required to support the new union syntax")
    def test_define_new_union_struct(self):

        struct_schema = _struct.StructProcessor.define_struct(NewUnionStruct)
        self.assertIsNotNone(struct_schema, _meta.StructSchema)



