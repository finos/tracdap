#  Copyright 2023 Accenture Global Solutions Limited
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

import datetime as dt
import decimal
import pathlib
import codecs
import csv
import typing as tp

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.csv as pa_csv

import tracdap.rt.ext.plugins as plugins
import tracdap.rt.exceptions as ex

# Import storage interfaces
from tracdap.rt.ext.storage import IDataFormat

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers

# TODO: Remove dependency on internal code
import tracdap.rt._impl.data as _data


class CsvStorageFormat(IDataFormat):

    FORMAT_CODE = "CSV"
    FILE_EXTENSION = "csv"

    __LENIENT_CSV_PARSER = "lenient_csv_parser"
    __LENIENT_MISSING_COLUMNS = "lenient_missing_columns"
    __DATE_FORMAT = "date_format"
    __DATETIME_FORMAT = "datetime_format"

    __TRUE_VALUES = ['true', 't', 'yes' 'y', '1']
    __FALSE_VALUES = ['false', 'f', 'no' 'n', '0']

    def __init__(self, format_options: tp.Dict[str, tp.Any] = None):

        self._log = _helpers.logger_for_object(self)

        self._format_options = format_options
        self._use_lenient_parser = False
        self._date_format = None
        self._datetime_format = None

        if format_options:

            if self.__LENIENT_CSV_PARSER in format_options:
                self._use_lenient_parser = self._validate_lenient_flag(format_options[self.__LENIENT_CSV_PARSER])

            if self.__DATE_FORMAT in format_options:
                self._date_format = self._validate_date_format(format_options[self.__DATE_FORMAT])

            if self.__DATETIME_FORMAT in format_options:
                self._datetime_format = self._validate_datetime_format(format_options[self.__DATETIME_FORMAT])

    @classmethod
    def _validate_lenient_flag(cls, lenient_flag: tp.Any):

        if lenient_flag is None:
            return False

        if isinstance(lenient_flag, bool):
            return lenient_flag

        if isinstance(lenient_flag, str):
            if lenient_flag.lower() == "false":
                return False
            if lenient_flag.lower() == "true":
                return True

        raise ex.EConfigParse(f"Invalid lenient flag for CSV storage: [{lenient_flag}]")

    @classmethod
    def _validate_date_format(cls, date_format: str):

        try:
            current_date = dt.datetime.now().date()
            current_date.strftime(date_format)
            return date_format
        except EncodingWarning:
            raise ex.EConfigParse(f"Invalid date format for CSV storage: [{date_format}]")

    @classmethod
    def _validate_datetime_format(cls, datetime_format: str):

        try:
            current_datetime = dt.datetime.now()
            current_datetime.strftime(datetime_format)
            return datetime_format
        except EncodingWarning:
            raise ex.EConfigParse(f"Invalid datetime format for CSV storage: [{datetime_format}]")

    def format_code(self) -> str:
        return self.FORMAT_CODE

    def file_extension(self) -> str:
        return self.FILE_EXTENSION

    def read_table(self, source: tp.BinaryIO, schema: tp.Optional[pa.Schema]) -> pa.Table:

        # For CSV data, if there is no schema then type inference will do unpredictable things!

        if schema is None or len(schema.names) == 0 or len(schema.types) == 0:
            raise ex.EDataConformance("An explicit schema is required to load CSV data")

        if self._use_lenient_parser:
            return self._read_table_lenient(source, schema)
        else:
            return self._read_table_arrow(source, schema)

    def write_table(self, target: tp.Union[str, pathlib.Path, tp.BinaryIO], table: pa.Table):

        write_options = pa_csv.WriteOptions()
        write_options.include_header = True

        # Arrow cannot yet apply the required output formatting for all data types
        # For types that require extra formatting, explicitly format them in code and output the string values

        formatted_table = self._format_outputs(table)

        pa_csv.write_csv(formatted_table, target, write_options)

    def _read_table_arrow(self, source: tp.BinaryIO, schema: pa.Schema) -> pa.Table:

        try:

            read_options = pa_csv.ReadOptions()
            read_options.encoding = 'utf-8'
            read_options.use_threads = False

            parse_options = pa_csv.ParseOptions()
            parse_options.newlines_in_values = True

            convert_options = pa_csv.ConvertOptions()
            convert_options.include_columns = schema.names
            convert_options.column_types = {n: t for (n, t) in zip(schema.names, schema.types)}
            convert_options.strings_can_be_null = True
            convert_options.quoted_strings_can_be_null = False

            return pa_csv.read_csv(source, read_options, parse_options, convert_options)

        except pa.ArrowInvalid as e:
            err = f"CSV file decoding failed, content is garbled"
            self._log.exception(err)
            raise ex.EDataCorruption(err) from e

        except pa.ArrowKeyError as e:
            err = f"CSV file decoding failed, one or more columns is missing"
            self._log.error(err)
            self._log.exception(str(e))
            raise ex.EDataCorruption(err) from e

    @classmethod
    def _format_outputs(cls, table: pa.Table) -> pa.Table:

        for column_index in range(table.num_columns):

            column: pa.Array = table.column(column_index)
            format_applied = False

            if pa.types.is_floating(column.type):

                # Arrow outputs NaN as null
                # If a float column contains NaN, use our own formatter to distinguish between them

                has_nan = pac.any(pac.and_not(  # noqa
                    column.is_null(nan_is_null=True),
                    column.is_null(nan_is_null=False)))

                if has_nan.as_py():
                    column = cls._format_float(column)
                    format_applied = True

            if pa.types.is_decimal(column.type):
                column = cls._format_decimal(column)
                format_applied = True

            if pa.types.is_timestamp(column.type):
                column = cls._format_timestamp(column)
                format_applied = True

            if format_applied:
                field = pa.field(table.schema.names[column_index], pa.utf8())
                table = table \
                    .remove_column(column_index) \
                    .add_column(column_index, field, column)

        return table

    @classmethod
    def _format_float(cls, column: pa.Array) -> pa.Array:

        def format_float(f: pa.FloatScalar):

            if not f.is_valid:
                return None

            return str(f.as_py())

        return pa.array(map(format_float, column), pa.utf8())

    @classmethod
    def _format_decimal(cls, column: pa.Array) -> pa.Array:

        # PyArrow does not support output of decimals as text, the cast (format) function is not implemented
        # So, explicitly format any decimal fields as strings before trying to save them

        column_type = column.type

        # Ensure the full information from the column is recorded in text form
        # Use the scale of the column as the maximum d.p., then strip away any unneeded trailing chars

        def format_decimal(d: pa.Scalar):

            if not d.is_valid:
                return None

            decimal_format = f".{column_type.scale}f"
            decimal_str = format(d.as_py(), decimal_format)

            return decimal_str.rstrip('0').rstrip('.')

        return pa.array(map(format_decimal, column), pa.utf8())

    @classmethod
    def _format_timestamp(cls, column: pa.Array) -> pa.Array:

        # PyArrow outputs timestamps with a space ' ' separator between date and time
        # ISO format requires a 'T' between date and time

        column_type: pa.TimestampType = column.type

        if column_type.unit == "s":
            timespec = 'seconds'
        elif column_type.unit == "ms":
            timespec = "milliseconds"
        else:
            timespec = "microseconds"

        def format_timestamp(t: pa.Scalar):

            if not t.is_valid:
                return None

            return t.as_py().isoformat(timespec=timespec)

        return pa.array(map(format_timestamp, column), pa.utf8())

    def _read_table_lenient(self, source: tp.BinaryIO, schema: pa.Schema) -> pa.Table:

        try:

            stream_reader = codecs.getreader('utf-8')
            text_source = stream_reader(source)

            csv_params = {
                "skipinitialspace": True,  # noqa
                "doublequote": True  # noqa
            }

            csv_reader = csv.reader(text_source, **csv_params)

            header = next(csv_reader)
            header_lower = list(map(str.lower, header))

            schema_columns = dict((col.lower(), index) for index, col in enumerate(schema.names))

            lenient_columns = _helpers.get_plugin_property(self._format_options, self.__LENIENT_MISSING_COLUMNS)
            missing_columns = list(filter(lambda col_: col_.lower() not in header_lower, schema.names))

            # The lenient_missing_columns flag allows missing columns so long as they are nullable
            # Primarily intended for reading schema files if not all the columns are needed
            if any(missing_columns):

                if not lenient_columns:
                    msg = f"CSV data is missing one or more columns: [{', '.join(missing_columns)}]"
                    self._log.error(msg)
                    raise ex.EDataConformance(msg)

                missing_not_null = list(filter(lambda col: not schema.field(col).nullable, missing_columns))

                if any(missing_not_null):
                    msg = f"CSV data is missing one or more not-null columns: [{', '.join(missing_not_null)}]"
                    self._log.error(msg)
                    raise ex.EDataConformance(msg)

            # Set up column mappings using field indices (avoid string lookups in the loop)

            col_mapping = [schema_columns.get(col) for col in header_lower]
            python_types = list(map(_data.DataMapping.arrow_to_python_type, schema.types))
            nullable_flags = list(map(lambda c: schema.field(c).nullable, range(len(schema.names))))

            data = [[] for _ in range(len(schema.names))]
            csv_row = 1  # Allowing for header
            csv_col = 0

            for row in csv_reader:

                # Extra values in the row is an error, they don't belong to any column
                if len(row) > len(header):
                    err = f"CSV decoding failed, unexpected extra columns on row [{csv_row}]"
                    self._log.exception(err)
                    raise ex.EDataCorruption(err)

                # Missing values at the end of the row are filled with blanks/nulls
                # This is only valid if the missing fields are nullable or of string type
                if len(row) < len(header):
                    null_values = ["" for _ in range(len(header) - len(row))]
                    row = row + null_values

                for raw_value in row:

                    col_name = header[csv_col]
                    output_col = col_mapping[csv_col]

                    if output_col is not None:
                        python_type = python_types[output_col]
                        nullable = nullable_flags[output_col]
                        python_value = self._convert_python_value(raw_value, python_type, nullable, csv_row, col_name)
                        data[output_col].append(python_value)

                    csv_col += 1

                csv_col = 0
                csv_row += 1

            data_dict = dict(zip(schema.names, data))

            # In lenient column mode, fill in any missing nullable columns with nulls
            if lenient_columns:
                for output_col in missing_columns:
                    data_dict[output_col] = list(None for _ in range(csv_row - 1))

            table = pa.Table.from_pydict(data_dict, schema)  # noqa

            return table

        except StopIteration as e:
            err = f"CSV decoding failed, no readable content"
            self._log.exception(err)
            raise ex.EDataCorruption(err) from e

        except UnicodeDecodeError as e:
            err = f"CSV decoding failed, content is garbled"
            self._log.exception(err)
            raise ex.EDataCorruption(err) from e

    def _convert_python_value(
            self, raw_value: tp.Any, python_type: type, nullable: bool,
            row: int, col: str) -> tp.Any:

        try:

            # Python's csv.reader does not know the difference between empty string and null

            if raw_value is None or (isinstance(raw_value, str) and raw_value == ""):
                if python_type == str:
                    return raw_value if nullable else ""
                if nullable:
                    return None
                else:
                    msg = f"CSV data contains null value for a not-null field (col = {col}, row = {row})"
                    self._log.error(msg)
                    raise ex.EDataConformance(msg)

            if isinstance(raw_value, python_type):
                return raw_value

            if python_type == bool:
                if isinstance(raw_value, str):
                    if raw_value.lower() in self.__TRUE_VALUES:
                        return True
                    if raw_value.lower() in self.__FALSE_VALUES:
                        return False
                if isinstance(raw_value, int) or isinstance(raw_value, float):
                    if raw_value == 1:
                        return True
                    if raw_value == 0:
                        return False

            if python_type == int:
                if isinstance(raw_value, float) and raw_value.is_integer():
                    return int(raw_value)
                if isinstance(raw_value, str):
                    return int(raw_value)

            if python_type == float:
                if isinstance(raw_value, int):
                    return float(raw_value)
                if isinstance(raw_value, str):
                    return float(raw_value)

            if python_type == decimal.Decimal:
                if isinstance(raw_value, int):
                    return decimal.Decimal.from_float(float(raw_value))
                if isinstance(raw_value, float):
                    return decimal.Decimal.from_float(raw_value)
                if isinstance(raw_value, str):
                    return decimal.Decimal(raw_value)

            if python_type == str:
                return str(raw_value)

            if python_type == dt.date:
                if isinstance(raw_value, str):
                    if self._date_format is not None:
                        return dt.datetime.strptime(raw_value, self._date_format).date()
                    else:
                        return dt.date.fromisoformat(raw_value)

            if python_type == dt.datetime:
                if isinstance(raw_value, str):
                    if self._datetime_format is not None:
                        return dt.datetime.strptime(raw_value, self._datetime_format)
                    else:
                        return dt.datetime.fromisoformat(raw_value)

        except Exception as e:

            msg = f"CSV data does not match the schema and cannot be converted" \
                + f" (col = {col}, row = {row}, expected type = [{python_type.__name__}], value = [{str(raw_value)}])" \
                + f": {str(e)}"

            self._log.exception(msg)
            raise ex.EDataConformance(msg) from e

        # Default case: unrecognized python_type

        msg = f"CSV data does not match the schema and cannot be converted" \
              + f" (col = {col}, row = {row}, expected type = [{python_type.__name__}], value = [{str(raw_value)}])"

        self._log.error(msg)
        raise ex.EDataConformance(msg)


plugins.PluginManager.register_plugin(
    IDataFormat, CsvStorageFormat,
    ["CSV", ".csv", "text/csv"])
