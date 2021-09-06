#  Copyright 2021 Accenture Global Solutions Limited
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

import dataclasses as dc
import typing as tp

import pandas as pd

import trac.rt.metadata as _meta


@dc.dataclass(frozen=True)
class DataItem:

    pandas: tp.Optional[pd.DataFrame] = None
    pyspark: tp.Any = None

    column_filter: tp.Optional[tp.List[str]] = None


@dc.dataclass(frozen=True)
class DataPartKey:

    @classmethod
    def for_root(cls) -> 'DataPartKey':
        return DataPartKey(opaque_key='part_root')

    opaque_key: str


@dc.dataclass(frozen=True)
class DataView:

    schema: _meta.SchemaDefinition
    parts: tp.Dict[DataPartKey, tp.List[DataItem]]
