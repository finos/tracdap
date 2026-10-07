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
import hashlib
import io
import pathlib
import typing as tp

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.feather as pa_ft
import pyarrow.parquet as pa_pq

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.storage as _ext_storage
import tracdap.rt._impl.core.config_parser as _cfg_p
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.type_system as _types


@dc.dataclass(frozen=True)
class CaptureFormat:

    format_code: str
    mime_type: str


class CaptureFormats:

    # Kept in step with CAPTURE_FORMATS in the platform validation library
    __CAPTURE_FORMATS = {
        "CSV": CaptureFormat("CSV", "text/csv"),
        "PARQUET": CaptureFormat("PARQUET", "application/vnd.apache.parquet"),
        "ARROW_FILE": CaptureFormat("ARROW_FILE", "application/vnd.apache.arrow.file")
    }

    @classmethod
    def file_extension(cls, storage_path: str) -> tp.Optional[str]:

        suffix = pathlib.PurePosixPath(storage_path).suffix
        return suffix[1:] if len(suffix) > 1 else None

    @classmethod
    def for_extension(cls, extension: str) -> tp.Optional[CaptureFormat]:

        try:
            codec = _storage.FormatManager.get_data_format(f".{extension.lower()}", format_options={})
            return cls.__CAPTURE_FORMATS.get(codec.format_code())
        except _ex.EStorageConfig:
            return None

    @classmethod
    def file_type(cls, extension: str, capture_format: CaptureFormat) -> _meta.FileType:
        return _meta.FileType(extension.lower(), capture_format.mime_type)


def _attr(attr_name: str, value: tp.Any) -> _meta.TagUpdate:

    return _meta.TagUpdate(
        _meta.TagOperation.CREATE_OR_REPLACE_ATTR,
        attr_name, _types.MetadataCodec.encode_value(value))


class CaptureReader:

    """Read a captured file from external storage, keeping its bytes unchanged and recording where they came from"""

    def __init__(self, storage: _storage.StorageManager):
        self.__storage = storage
        self.__log = _logging.logger_for_object(self)

    def read_file(self, capture_name: str, location: _meta.ExternalLocation, size_limit: int) -> _data.DataItem:

        storage_key = location.storageKey
        storage_path = location.storagePath

        if not self.__storage.has_file_storage(storage_key, external=True):
            raise _ex.EStorageConfig(
                f"Capture [{capture_name}] failed: [{storage_key}] is not a configured external storage location")

        file_storage = self.__storage.get_file_storage(storage_key, external=True)

        try:
            file_stat = file_storage.stat(storage_path)
        except _ex.EStorage as e:
            raise _ex.EStorageRequest(
                f"Capture [{capture_name}] failed: File not found [{storage_path}] in [{storage_key}]") from e

        if file_stat.file_type != _ext_storage.FileType.FILE:
            raise _ex.EStorageRequest(
                f"Capture [{capture_name}] failed: [{storage_path}] in [{storage_key}] is not a file")

        if file_stat.size > size_limit:
            raise _ex.EStorageRequest(
                f"Capture [{capture_name}] failed: File size [{file_stat.size}] bytes exceeds the capture size limit" +
                f" [{size_limit}] bytes, set by [captureMaxSize] in the platform's executor config" +
                f" (the [{_cfg_p.ConfigKeys.RUNTIME_LIMIT_CAPTURE_SIZE}] property when running locally)")

        content = file_storage.read_bytes(storage_path)
        content_hash = hashlib.sha256(content).hexdigest()

        self.__log.info(
            f"Captured [{capture_name}] from [{storage_key}] at [{storage_path}]," +
            f" size = [{len(content)}], sha256 = [{content_hash}]")

        attrs = [
            _attr("trac_import_location_key", storage_key),
            _attr("trac_import_location_path", storage_path),
            _attr("trac_import_file_name", pathlib.PurePosixPath(storage_path).name),
            _attr("trac_import_file_size", len(content)),
            _attr("trac_import_content_hash", content_hash)]

        if file_stat.mtime is not None:
            attrs.append(_attr("trac_import_file_modified", file_stat.mtime))

        return _data.DataItem.for_file_content(content).with_attrs(attrs)



class CaptureDecoder:

    """Decode the bytes of a captured file into a table conformed to TRAC's standard types"""

    __SCHEMA_SOURCE_NAMES = {
        _meta.CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED: "DECLARED",
        _meta.CaptureSchemaSource.CAPTURE_SCHEMA_FILE: "FILE"
    }

    def __init__(self, storage: _storage.StorageManager):
        self.__storage = storage
        self.__log = _logging.logger_for_object(self)

    def decode_table(
            self, capture_name: str, file_item: _data.DataItem,
            storage_key: str, format_code: str,
            schema_source: _meta.CaptureSchemaSource,
            declared_schema: tp.Optional[_meta.SchemaDefinition],
            file_id: _meta.TagHeader) \
            -> _data.DataItem:

        try:

            content: bytes = file_item.content
            codec = self._codec(storage_key, format_code)
            source_columns = self._source_columns(content, format_code)

            if schema_source == _meta.CaptureSchemaSource.CAPTURE_SCHEMA_DECLARED:
                table, trac_schema = self._decode_declared(capture_name, content, codec, format_code, source_columns, declared_schema)
            elif schema_source == _meta.CaptureSchemaSource.CAPTURE_SCHEMA_FILE:
                table, trac_schema = self._decode_file_schema(capture_name, content, codec)
            else:
                raise _ex.EUnexpected()

            arrow_schema = _data.DataMapping.trac_to_arrow_schema(trac_schema)
            conformed_table = _data.DataConformance.conform_to_schema(table, arrow_schema, warn_extra_columns=False)

        except _ex.EData as e:
            raise type(e)(f"Capture [{capture_name}] failed: {str(e)}") from e

        attrs = [
            *file_item.attrs,
            _attr("trac_import_schema_source", self.__SCHEMA_SOURCE_NAMES[schema_source]),
            _attr("trac_import_source_columns", len(source_columns)),
            _attr("trac_capture_file", file_id.objectId)]

        return _data.DataItem \
            .for_table(conformed_table, conformed_table.schema, trac_schema) \
            .with_attrs(attrs)

    def _codec(self, storage_key: str, format_code: str) -> _ext_storage.IDataFormat:

        if self.__storage.has_data_storage(storage_key, external=True):
            data_storage = self.__storage.get_data_storage(storage_key, external=True)
            if isinstance(data_storage, _storage.CommonDataStorage):
                return data_storage.get_data_format(format_code)

        return _storage.FormatManager.get_data_format(format_code, format_options={})

    @staticmethod
    def _source_columns(content: bytes, format_code: str) -> tp.List[str]:

        try:
            if format_code == "CSV":
                return pa_csv.open_csv(io.BytesIO(content)).schema.names
            elif format_code == "PARQUET":
                return pa_pq.read_schema(io.BytesIO(content)).names
            elif format_code == "ARROW_FILE":
                return pa.ipc.open_file(io.BytesIO(content)).schema.names
            else:
                raise _ex.EUnexpected()

        except pa.ArrowInvalid as e:
            raise _ex.EDataCorruption(f"Unable to read the file's columns, content is garbled") from e

    def _decode_declared(
            self, capture_name: str, content: bytes,
            codec: _ext_storage.IDataFormat, format_code: str,
            source_columns: tp.List[str],
            declared_schema: _meta.SchemaDefinition) \
            -> tp.Tuple[pa.Table, _meta.SchemaDefinition]:

        declared_arrow_schema = _data.DataMapping.trac_to_arrow_schema(declared_schema)
        source_names = self._match_columns(declared_arrow_schema.names, source_columns)

        # Request the matched columns under the file's spelling, with the declared types
        # CSV date-times are read as strings, so values with and without offsets can be handled per column
        csv_datetimes = list()
        read_fields = list()

        for declared_field, source_name in zip(declared_arrow_schema, source_names):
            if format_code == "CSV" and pa.types.is_timestamp(declared_field.type):
                csv_datetimes.append(source_name)
                read_fields.append(pa.field(source_name, pa.utf8()))
            else:
                read_fields.append(pa.field(source_name, declared_field.type))

        table = codec.read_table(io.BytesIO(content), pa.schema(read_fields))

        for column_name in csv_datetimes:
            table = self._convert_csv_datetime(capture_name, table, column_name)

        table = self._convert_zoned_timestamps(capture_name, table)

        left_out = [column for column in source_columns if column not in source_names]
        if any(left_out):
            self.__log.info(f"Capture [{capture_name}] left out columns not in the declared schema: {left_out}")

        return table, declared_schema

    def _decode_file_schema(
            self, capture_name: str, content: bytes,
            codec: _ext_storage.IDataFormat) \
            -> tp.Tuple[pa.Table, _meta.SchemaDefinition]:

        table = codec.read_table(io.BytesIO(content), None)

        # A column with no non-null values has no type of its own
        for index, field in enumerate(table.schema):
            if pa.types.is_null(field.type):
                table = table.set_column(index, pa.field(field.name, pa.utf8()), pa.nulls(table.num_rows, pa.utf8()))

        table = self._convert_zoned_timestamps(capture_name, table)

        for field in table.schema:
            try:
                _data.DataMapping.arrow_to_trac_type(field.type)
            except _ex.ETracInternal as e:
                raise _ex.EDataConformance(f"Column [{field.name}] has a type TRAC can't represent [{field.type}]") from e

        return table, _data.DataMapping.arrow_to_trac_schema(table.schema)

    @staticmethod
    def _match_columns(declared_names: tp.List[str], source_columns: tp.List[str]) -> tp.List[str]:

        source_names = list()

        for declared_name in declared_names:

            matches = [column for column in source_columns if column.lower() == declared_name.lower()]

            if len(matches) == 0:
                raise _ex.EDataConformance(f"Field [{declared_name}] in the declared schema is missing from the file")

            if len(matches) > 1:
                raise _ex.EDataConformance(
                    f"Field [{declared_name}] in the declared schema matches more than one column in the file: {matches}")

            source_names.append(matches[0])

        return source_names

    def _convert_csv_datetime(self, capture_name: str, table: pa.Table, column_name: str) -> pa.Table:

        index = table.schema.get_field_index(column_name)
        column = table.column(index)
        unit = _data.DataMapping.DEFAULT_TIMESTAMP_UNIT

        try:
            converted = pc.cast(column, pa.timestamp(unit, tz="UTC")).cast(pa.timestamp(unit))
            if column.null_count < len(column):
                self.__log.info(f"Capture [{capture_name}] converted column [{column_name}] to UTC from offsets")
        except pa.ArrowInvalid:
            try:
                converted = pc.cast(column, pa.timestamp(unit))
            except pa.ArrowInvalid as e:
                raise _ex.EDataConformance(
                    f"Column [{column_name}] mixes date-times with and without a zone offset," +
                    f" or has values that are not ISO 8601 date-times") from e

        return table.set_column(index, pa.field(column_name, converted.type), converted)

    def _convert_zoned_timestamps(self, capture_name: str, table: pa.Table) -> pa.Table:

        for index, field in enumerate(table.schema):

            if pa.types.is_timestamp(field.type) and field.type.tz is not None:

                naive_type = pa.timestamp(field.type.unit)
                converted = table.column(index).cast(naive_type)
                table = table.set_column(index, pa.field(field.name, naive_type, field.nullable), converted)

                self.__log.info(
                    f"Capture [{capture_name}] converted column [{field.name}] to UTC from zone [{field.type.tz}]")

        return table
