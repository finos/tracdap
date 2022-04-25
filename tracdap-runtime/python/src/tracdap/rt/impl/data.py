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

from __future__ import annotations

import dataclasses as dc
import typing as tp

import pyarrow as pa
import pandas as pd

import tracdap.rt.metadata as _meta
import tracdap.rt.impl.type_system as _types


@dc.dataclass(frozen=True)
class DataItemSpec:

    data_item: str
    data_def: _meta.DataDefinition
    storage_def: _meta.StorageDefinition
    schema_def: tp.Optional[_meta.SchemaDefinition]


@dc.dataclass(frozen=True)
class DataPartKey:

    @classmethod
    def for_root(cls) -> DataPartKey:
        return DataPartKey(opaque_key='part_root')

    opaque_key: str


@dc.dataclass(frozen=True)
class DataItem:

    schema: pa.Schema
    table: tp.Optional[pa.Table] = None
    batches: tp.Optional[tp.List[pa.RecordBatch]] = None

    pandas: tp.Optional[pd.DataFrame] = None
    pyspark: tp.Any = None


@dc.dataclass(frozen=True)
class DataView:

    trac_schema: _meta.SchemaDefinition
    arrow_schema: pa.Schema

    parts: tp.Dict[DataPartKey, tp.List[DataItem]]

    @staticmethod
    def for_trac_schema(trac_schema: _meta.SchemaDefinition):
        arrow_schema = _types.trac_to_arrow_schema(trac_schema)
        return DataView(trac_schema, arrow_schema, dict())


class DataMapping:

    @classmethod
    def item_to_pandas(cls, item: DataItem) -> pd.DataFrame:

        if item.pandas is not None:
            return item.pandas.copy()

        if item.table is not None:
            return item.table.to_pandas()

        if item.batches is not None:
            table = pa.Table.from_batches(item.batches, data_item.schema)  # noqa
            return table.to_pandas()

        raise RuntimeError()  # todo

    @classmethod
    def view_to_pandas(cls, view: DataView, part: DataPartKey) -> pd.DataFrame:

        deltas = view.parts.get(part)

        if not deltas:
            raise RuntimeError()  # todo

        if len(deltas) == 1:
            return cls.item_to_pandas(deltas[0])

        batches = {
            batch
            for delta in deltas
            for batch in (
                delta.batches
                if delta.batches
                else delta.table.to_batches())}

        table = pa.Table.from_batches(batches) # noqa
        return table.to_pandas()

    @classmethod
    def item_from_pandas(cls, df: pd.DataFrame, schema: tp.Optional[pa.Schema]) -> DataItem:

        if len(df) == 0:
            table = pa.Table.from_batches(list(), schema)  # noqa
        else:
            table = pa.Table.from_pandas(df, schema, preserve_index=False)  # noqa

        return DataItem(schema, table)

    @classmethod
    def add_item_to_view(cls, view: DataView, part: DataPartKey, item: DataItem) -> DataView:

        prior_deltas = view.parts.get(part) or list()
        deltas = [*prior_deltas, item]
        parts = {**view.parts, part: deltas}

        return DataView(view.trac_schema, view.arrow_schema, parts)
