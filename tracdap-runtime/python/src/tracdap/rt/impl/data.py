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
import pyarrow.compute as pc
import pandas as pd

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
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
    def arrow_to_pandas(cls, table: pa.Table) -> pd.DataFrame:

        return table.to_pandas()

    @classmethod
    def pandas_to_arrow(cls, df: pd.DataFrame, schema: tp.Optional[pa.Schema] = None) -> pa.Table:

        if len(df) == 0:
            df_schema = pa.Schema.from_pandas(df)  # noqa
            table = pa.Table.from_batches(list(), df_schema)  # noqa
        else:
            schema_columns = schema.names if schema is not None else None
            table = pa.Table.from_pandas(df, preserve_index=False, columns=schema_columns)  # noqa

        # If there is no explict schema, give back the table exactly as it was received from Pandas
        # There could be an option here to coerce types to the appropriate TRAC standard types
        # E.g. unsigned int 32 -> signed int 64, TRAC standard integer type

        if schema is None:
            return table
        else:
            return DataConformity.conform_table(table, schema)

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
    def item_to_pandas(cls, item: DataItem) -> pd.DataFrame:

        if item.pandas is not None:
            return item.pandas.copy()

        if item.table is not None:
            return cls.arrow_to_pandas(item.table)

        if item.batches is not None:
            table = pa.Table.from_batches(item.batches, data_item.schema)  # noqa
            return cls.arrow_to_pandas(table)

        raise RuntimeError()  # todo

    @classmethod
    def pandas_to_item(cls, df: pd.DataFrame, schema: tp.Optional[pa.Schema]) -> DataItem:

        table = cls.pandas_to_arrow(df, schema)
        return DataItem(table.schema, table)

    @classmethod
    def add_item_to_view(cls, view: DataView, part: DataPartKey, item: DataItem) -> DataView:

        prior_deltas = view.parts.get(part) or list()
        deltas = [*prior_deltas, item]
        parts = {**view.parts, part: deltas}

        return DataView(view.trac_schema, view.arrow_schema, parts)


class DataConformity:

    @classmethod
    def conform_table(cls, table: pa.Table, schema: pa.Schema) -> pa.Table:

        # Coerce types to match expected schema where possible
        for schem_index in range(len(schema.types)):

            schema_column_name = schema.names[schem_index]
            schema_column_type = schema.types[schem_index]

            table_index = table.schema.get_field_index(schema_column_name)
            table_column: pa.Array = table.column(table_index)

            # Avoid manipulating columns that already have the correct type and location
            if table_index == schem_index and table_column.type == schema_column_type:
                continue

            if table_column.type != schema_column_type:
                table_column = cls._coerce_vector(table_column, schema_column_type)

            table = table.remove_column(table_index)
            table = table.add_column(schem_index, schema_column_name, table_column)

        # Only include columns explicitly defined in the schema
        while table.num_columns > len(schema.types):
            table = table.remove_column(table.num_columns - 1)

        return table

    @classmethod
    def _coerce_vector(cls, vector: pa.Array, target_type: pa.DataType) -> pa.Array:

        if len(vector) == 0 and (vector.type is None or pa.types.is_null(vector.type)):
            return pa.array([], type=target_type, size=0)

        if pa.types.is_boolean(target_type):
            return cls._coerce_boolean(vector)

        if pa.types.is_integer(target_type):
            return cls._coerce_integer(vector, target_type)

        if pa.types.is_floating(target_type):
            return cls._coerce_float(vector, target_type)

        if pa.types.is_decimal(target_type):
            return cls._coerce_decimal(vector, target_type)

        if pa.types.is_string(target_type):
            return cls._coerce_string(vector, target_type)

        if pa.types.is_date(target_type):
            return cls._coerce_date(vector, target_type)

        if pa.types.is_timestamp(target_type):
            return cls._coerce_timestamp(vector, target_type)

        raise _ex.EDataValidation(f"Unsupported data type {target_type}")

    @staticmethod
    def _coerce_boolean(vector: pa.Array) -> pa.BooleanArray:

        if pa.types.is_boolean(vector.type):
            return vector  # noqa

        raise _ex.EDataValidation(f"Cannot coerce type {vector.type} into {pa.bool_()}")

    @staticmethod
    def _coerce_integer(vector: pa.Array, target_type: pa.DataType) -> pa.IntegerArray:

        if pa.types.is_integer(vector.type):

            source_bit_width = vector.type.bit_width
            target_bit_width = target_type.bit_width
            source_signed = pa.types.is_signed_integer(vector.type)
            target_signed = pa.types.is_signed_integer(target_type)

            if source_bit_width == target_bit_width and source_signed == target_signed:
                return vector  # noqa

            if source_bit_width < target_bit_width and source_signed == target_signed:
                return pc.cast(vector, target_type)

            # Unsigned types can be safely cast to signed types with larger byte width
            if source_bit_width < target_bit_width and target_signed:
                return pc.cast(vector, target_type)

        raise _ex.EDataValidation(f"Cannot coerce type {vector.type} into {target_type}")

    @staticmethod
    def _coerce_float(vector: pa.Array, target_type: pa.DataType) -> pa.FloatingPointArray:

        if pa.types.is_floating(vector.type):

            source_bit_width = vector.type.bit_wdith
            target_bit_width = target_type.bit_width

            if source_bit_width == target_bit_width:
                return vector  # noqa

            if source_bit_width < target_bit_width:
                return pc.cast(vector, target_type)

        if pa.types.is_integer(vector.type):
            return pc.cast(vector, target_type)

        raise _ex.EDataValidation(f"Cannot coerce type {vector.type} into {target_type}")

    @staticmethod
    def _coerce_decimal(vector: pa.Array, target_type: pa.DataType) -> pa.Array:

        # Allow coercing decimal 128 -> decimal 256, but not vice versa
        if pa.types.is_decimal(vector.type):

            source_max = vector.type.precision - vector.type.scale
            target_max = target_type.precision - target_type.scale  # noqa

            if source_max <= target_max:
                rounded = pc.round(vector, ndigits=target_type.scale)  # noqa
                return pc.cast(rounded, target_type)

        # Coerce floats and integers to decimal, always allowed
        if pa.types.is_floating(vector.type) or pa.types.is_integer(vector.type):
            return pc.cast(vector, target_type)

        raise _ex.EDataValidation(f"Cannot coerce type {vector.type} into {target_type}")

    @staticmethod
    def _coerce_string(vector: pa.Array, target_type: pa.DataType) -> pa.Array:

        if pa.types.is_string(vector.type):
            return vector

        raise _ex.EDataValidation(f"Cannot coerce type {vector.type} into {target_type}")

    @staticmethod
    def _coerce_date(vector: pa.Array, target_type: pa.DataType) -> pa.Array:

        if pa.types.is_date(vector.type):

            if target_type.bit_width >= vector.type.bit_width:
                return pc.cast(vector, target_type)

        raise _ex.EDataValidation(f"Cannot coerce type {vector.type} into {target_type}")

    @staticmethod
    def _coerce_timestamp(vector: pa.Array, target_type: pa.DataType) -> pa.Array:

        if pa.types.is_timestamp(vector.type):

            if not isinstance(target_type, pa.TimestampType):
                raise _ex.EUnexpected()

            if target_type.unit == "s":
                rounding_unit = "second"
            elif target_type.unit == "ms":
                rounding_unit = "millisecond"
            elif target_type.unit == "us":
                rounding_unit = "microsecond"
            elif target_type.unit == "ns":
                rounding_unit = "nanosecond"
            else:
                raise _ex.EUnexpected()

            # round_temporal does not change the type for the rounded vector
            # E.g. rounding "us" vector to "ms" results in "us" vector with truncated values
            # So, we need to set safe=False when calling pc.cast()

            if target_type.bit_width >= vector.type.bit_width:
                rounded_vector = pc.round_temporal(vector, unit=rounding_unit)  # noqa
                return pc.cast(vector, target_type, safe=False)

        raise _ex.EDataValidation(f"Cannot coerce type {vector.type} into {target_type}")
