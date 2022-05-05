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
import platform

import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.util as _util


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
    def arrow_to_pandas(cls, table: pa.Table) -> pd.DataFrame:

        return table.to_pandas(
            ignore_metadata=True,  # noqa
            date_as_object=False,  # noqa
            timestamp_as_object=False,  # noqa
            types_mapper=cls.__ARROW_TO_PANDAS_TYPES.get)

    # TODO: Move to type_system
    # However only partial mapping is possible, object dtypes will not map this way
    __ARROW_TO_PANDAS_TYPES = {
        pa.bool_(): pd.BooleanDtype(),
        pa.int64(): pd.Int64Dtype(),
        # TODO: What should be the default behavior for floats?
        # Float64DType is available in Pandas 1.2 and later, it offers more consistent handling of NaN / null
        pa.float64(): pd.Float64Dtype() if "Float64Dtype" in pd.__dict__ else None,
        pa.utf8(): pd.StringDtype()
    }

    @classmethod
    def pandas_to_view(cls, df: pd.DataFrame, prior_view: DataView, part: DataPartKey):

        item = cls.pandas_to_item(df, prior_view.arrow_schema)
        return cls.add_item_to_view(prior_view, part, item)

    @classmethod
    def pandas_to_item(cls, df: pd.DataFrame, schema: tp.Optional[pa.Schema]) -> DataItem:

        table = cls.pandas_to_arrow(df, schema)
        return DataItem(table.schema, table)

    @classmethod
    def pandas_to_arrow(cls, df: pd.DataFrame, schema: tp.Optional[pa.Schema] = None) -> pa.Table:

        # TODO: Handle conformance of schema fields before converting, or convert all fields then conform

        if len(df) == 0:
            df_schema = pa.Schema.from_pandas(df, preserve_index=False)  # noqa
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
            return DataConformance.conform_to_schema(table, schema, df.dtypes)

    @classmethod
    def add_item_to_view(cls, view: DataView, part: DataPartKey, item: DataItem) -> DataView:

        prior_deltas = view.parts.get(part) or list()
        deltas = [*prior_deltas, item]
        parts = {**view.parts, part: deltas}

        return DataView(view.trac_schema, view.arrow_schema, parts)


class _Logging:
    pass


class DataConformance:

    __log = _util.logger_for_namespace(_Logging.__module__ + ".DataConformance")

    __pandas_datetime_type = pd.to_datetime([]).dtype

    __E_FIELD_MISSING = \
        "Field [{field_name}] is missing from the data"

    __E_WRONG_DATA_TYPE = \
        "Field [{field_name}] contains the wrong data type " + \
        "(expected {field_type}, got {vector_type})"
    __E_DATA_LOSS_WILL_OCCUR = \
        "Field [{field_name}] cannot be converted from {vector_type} to {field_type}, data will be lost"
    __E_DATA_LOSS_DID_OCCUR = \
        "Field [{field_name}] cannot be converted from {vector_type} to {field_type}, " + \
        "data will be lost ({error_details})"

    __E_TIMEZONE_DOES_NOT_MATCH = \
        "Field [{field_name}] cannot be converted from {vector_type} to {field_type}, " + \
        "source and target have different time zones"

    @classmethod
    def conform_to_schema(
            cls, table: pa.Table, schema: pa.Schema,
            pandas_types=None, warn_extra_columns=True) \
            -> pa.Table:

        # If Pandas types are supplied they must match the table, i.e. table has been converted from Pandas
        if pandas_types is not None and len(pandas_types) != len(table.schema.types):
            raise _ex.EUnexpected()

        cls._check_duplicate_fields(schema, True)
        cls._check_duplicate_fields(table.schema, False)

        table_indices = {f.lower(): i for (i, f) in enumerate(table.schema.names)}
        conformed_data = []
        conformance_errors = []

        # Coerce types to match expected schema where possible
        for schema_index in range(len(schema.names)):

            try:
                schema_field = schema.field(schema_index)
                table_index = table_indices.get(schema_field.name.lower())

                if table_index is None:
                    message = cls.__E_FIELD_MISSING.format(field_name=schema_field.name)
                    cls.__log.error(message)
                    raise _ex.EDataConformance(message)

                table_column: pa.Array = table.column(table_index)

                pandas_type = pandas_types[table_index] \
                    if pandas_types is not None \
                    else None

                if table_column.type == schema_field.type:
                    conformed_column = table_column
                else:
                    conformed_column = cls._coerce_vector(table_column, schema_field, pandas_type)

                if not schema_field.nullable and table_column.null_count > 0:
                    message = f"Null values present in non-null field [{schema_field.name}]"
                    cls.__log.error(message)
                    raise _ex.EDataConformance(message)

                conformed_data.append(conformed_column)

            except _ex.EDataConformance as e:
                conformance_errors.append(e)

        # Columns not defined in the schema will not be included in the conformed output
        if warn_extra_columns and table.num_columns > len(schema.types):

            schema_columns = set(map(str.lower, schema.names))
            extra_columns = [
                f"[{col}]"
                for col in table.schema.names
                if col.lower() not in schema_columns]

            message = f"Columns not defined in the schema will be dropped: {', '.join(extra_columns)}"
            cls.__log.warning(message)

        if any(conformance_errors):
            if len(conformance_errors) == 1:
                raise conformance_errors[0]
            else:
                cls.__log.error("There were multiple data conformance errors")
                raise _ex.EDataConformance("There were multiple data conformance errors", conformance_errors)

        return pa.Table.from_arrays(conformed_data, schema=schema)  # noqa

    @classmethod
    def _check_duplicate_fields(cls, schema: pa.Schema, schema_or_table: bool):

        check = {}

        for field in schema.names:
            field_lower = field.lower()
            if field_lower not in check:
                check[field_lower] = []
            check[field_lower].append(field)

        duplicate_fields = dict(filter(lambda f_fs: len(f_fs[1]) > 1, check.items()))

        if any(duplicate_fields):

            duplicate_info = []

            for field_lower, fields in duplicate_fields.items():
                if all(map(lambda f: f == fields[0], fields)):
                    duplicate_info.append(f"[{fields[0]}]")
                else:
                    duplicate_info.append(f"[{field_lower}] ({', '.join(fields)} differ only by case)")

            source = "Schema" if schema_or_table else "Data"

            error_message = f"{source} contains duplicate fields: " + ", ".join(duplicate_info)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message)

    @classmethod
    def _coerce_vector(cls, vector: pa.Array, field: pa.Field, pandas_type=None) -> pa.Array:

        if pa.types.is_null(vector.type):

            if field.nullable:
                return pa.array([], type=field.type, size=len(vector))
            else:
                raise _ex.EDataConformance(f"All null values in non-null field [{field.name}]")

        if pa.types.is_boolean(field.type):
            return cls._coerce_boolean(vector, field)

        if pa.types.is_integer(field.type):
            return cls._coerce_integer(vector, field)

        if pa.types.is_floating(field.type):
            return cls._coerce_float(vector, field)

        if pa.types.is_decimal(field.type):
            return cls._coerce_decimal(vector, field)

        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            return cls._coerce_string(vector, field)

        if pa.types.is_date(field.type):
            return cls._coerce_date(vector, field, pandas_type)

        if pa.types.is_timestamp(field.type):
            return cls._coerce_timestamp(vector, field)

        raise _ex.EDataConformance(f"Unsupported data type {field.type}")  # TODO

    @classmethod
    def _coerce_boolean(cls, vector: pa.Array, field: pa.Field) -> pa.BooleanArray:

        if pa.types.is_boolean(vector.type):
            return vector  # noqa

        error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
        cls.__log.error(error_message)
        raise _ex.EDataConformance(error_message)

    @classmethod
    def _coerce_integer(cls, vector: pa.Array, field: pa.Field) -> pa.IntegerArray:

        try:

            if pa.types.is_integer(vector.type):
                return pc.cast(vector, field.type)

            else:
                error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
                cls.__log.error(error_message)
                raise _ex.EDataConformance(error_message)

        except pa.ArrowInvalid as e:

            error_message = cls._format_error(cls.__E_DATA_LOSS_DID_OCCUR, vector, field, e)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message) from e

    @classmethod
    def _coerce_float(cls, vector: pa.Array, field: pa.Field) -> pa.FloatingPointArray:

        try:

            # Coercing between float types
            if pa.types.is_floating(vector.type):

                # Casting floats to a wider type is allowed
                # Casting to a less wide type does not raise exceptions when values do not fit
                # So we need an explict check on which casts are allowed

                source_bit_width = vector.type.bit_width
                target_bit_width = field.type.bit_width

                if source_bit_width == target_bit_width:
                    return vector  # noqa

                # cast() is available for float32 -> float64, but not for float16 -> float32/float64
                elif source_bit_width == 32 and target_bit_width == 64:
                    return pc.cast(vector, field.type)

                elif source_bit_width > target_bit_width:
                    error_message = cls._format_error(cls.__E_DATA_LOSS_WILL_OCCUR, vector, field)
                    cls.__log.error(error_message)
                    raise _ex.EDataConformance(error_message)

            # All integer types can be coerced to float32 or float64
            if pa.types.is_integer(vector.type) and not pa.types.is_float16(field.type):
                return pc.cast(vector, field.type)

            if pa.types.is_integer(vector.type) and vector.type.bit_width <= 16:
                return pc.cast(vector, field.type)

            error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message)

        except pa.ArrowInvalid as e:

            error_message = cls._format_error(cls.__E_DATA_LOSS_DID_OCCUR, vector, field, e)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message) from e

    @classmethod
    def _coerce_decimal(cls, vector: pa.Array, field: pa.Field) -> pa.Array:

        # Loss of precision is allowed, but loss of data is not
        # Arrow will raise an error if cast() results in loss of data

        try:

            # For decimal values, arrow will raise an error on loss of precision
            # Round explicitly to the required scale so there is no loss of precision in cast()
            if pa.types.is_decimal(vector.type):
                rounded = pc.round(vector, ndigits=field.type.scale)  # noqa
                return pc.cast(rounded, field.type)

            # Floats and integers can always be coerced to decimal, so long as there is no data loss
            elif pa.types.is_floating(vector.type) or pa.types.is_integer(vector.type):
                return pc.cast(vector, field.type)

            else:
                error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
                cls.__log.error(error_message)
                raise _ex.EDataConformance(error_message)

        except pa.ArrowInvalid as e:

            error_message = cls._format_error(cls.__E_DATA_LOSS_DID_OCCUR, vector, field, e)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message) from e

    @classmethod
    def _coerce_string(cls, vector: pa.Array, field: pa.Field) -> pa.Array:

        if pa.types.is_string(field.type):
            if pa.types.is_string(vector.type):
                return vector

        if pa.types.is_large_string(field.type):
            if pa.types.is_large_string(vector.type):
                return vector
            # Allow up-casting string -> large_string
            if pa.types.is_string(vector.type):
                return pc.cast(vector, field.type)

        error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
        cls.__log.error(error_message)

        raise _ex.EDataConformance(error_message)

    @classmethod
    def _coerce_date(cls, vector: pa.Array, field: pa.Field, pandas_type=None) -> pa.Array:

        # Allow casting date32 -> date64, both range and precision are greater so there is no data loss
        if pa.types.is_date(vector.type):
            if field.type.bit_width >= vector.type.bit_width:
                return pc.cast(vector, field.type)

        # Special handling for Pandas/NumPy date values
        # These are encoded as np.datetime64[ns] in Pandas -> pa.timestamp64[ns] in Arrow
        # Only allow this conversion if the vector is coming from Pandas with datetime type
        if pandas_type == cls.__pandas_datetime_type:
            if pa.types.is_timestamp(vector.type) and vector.type.unit == "ns":
                return pc.cast(vector, field.type)

        error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
        cls.__log.error(error_message)

        raise _ex.EDataConformance(error_message)

    @classmethod
    def _coerce_timestamp(cls, vector: pa.Array, field: pa.Field) -> pa.Array:

        try:

            if pa.types.is_timestamp(vector.type):

                if not isinstance(field.type, pa.TimestampType):
                    raise _ex.EUnexpected()

                if vector.type.tz != field.type.tz:
                    error_message = cls._format_error(cls.__E_TIMEZONE_DOES_NOT_MATCH, vector, field)
                    cls.__log.error(error_message)
                    raise _ex.EDataConformance(error_message)

                # The cast() function applied to timestamp on Windows does not correctly detect overflows / under-flows
                # To get consistent behavior, this custom implementation casts to int64, the underlying type
                # Then performs the required scaling on the int64 vector, which does throw for overflows
                # Bug exists in Arrow 7.0.0 as of May 2022

                # This also avoids the need for timezone lookup on Windows
                # Although zone conversion is not supported, a tz database is still required
                # When casting timestamps with source and target type in the same zone

                if platform.system().lower().startswith("win"):
                    return cls._coerce_timestamp_windows(vector, field)

                if field.type.unit == "s":
                    rounding_unit = "second"
                elif field.type.unit == "ms":
                    rounding_unit = "millisecond"
                elif field.type.unit == "us":
                    rounding_unit = "microsecond"
                elif field.type.unit == "ns":
                    rounding_unit = "nanosecond"
                else:
                    raise _ex.EUnexpected()

                # Loss of precision is allowed, loss of data is not
                # Rounding will prevent errors in cast() due to loss of precision
                # cast() will fail if the source value is outside the range of the target type

                rounded_vector = pc.round_temporal(vector, unit=rounding_unit)  # noqa
                return pc.cast(rounded_vector, field.type)

            else:
                error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
                cls.__log.error(error_message)
                raise _ex.EDataConformance(error_message)

        except pa.ArrowInvalid as e:

            error_message = cls._format_error(cls.__E_DATA_LOSS_DID_OCCUR, vector, field, e)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message) from e

    @classmethod
    def _coerce_timestamp_windows(cls, vector: pa.Array, field: pa.Field):

        scaling_map = {"s": 1, "ms": 1000, "us": 1000000, "ns": 1000000000}
        src_scale = scaling_map.get(vector.type.unit)
        tgt_scale = scaling_map.get(field.type.unit)

        if src_scale is None or tgt_scale is None:
            raise _ex.EUnexpected()  # Invalid timestamp type

        int64_vector: pa.IntegerArray = pc.cast(vector, pa.int64())

        if src_scale > tgt_scale:

            scaling = src_scale / tgt_scale
            scaling_vector = pa.array([scaling for _ in range(len(vector))], pa.int64())
            scaled_vector = pc.divide_checked(int64_vector, scaling_vector)  # noqa

        else:

            scaling = tgt_scale / src_scale
            scaling_vector = pa.array([scaling for _ in range(len(vector))], pa.int64())
            scaled_vector = pc.multiply_checked(int64_vector, scaling_vector)  # noqa

        return pc.cast(scaled_vector, field.type)

    @classmethod
    def _format_error(cls, error_template: str, vector: pa.Array, field: pa.Field, e: Exception = None):

        return error_template.format(
            field_name=field.name,
            field_type=field.type,
            vector_type=vector.type,
            error_details=str(e))
