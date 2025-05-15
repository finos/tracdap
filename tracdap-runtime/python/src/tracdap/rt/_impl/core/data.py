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

import abc
import copy
import dataclasses as dc
import pathlib
import typing as tp
import datetime as dt
import decimal
import platform
import random

import pyarrow as pa
import pyarrow.compute as pc

try:
    import pandas  # noqa
except ModuleNotFoundError:
    pandas = None

try:
    import polars  # noqa
except ModuleNotFoundError:
    polars = None

import tracdap.rt.api.experimental as _api
import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.logging as _log
import tracdap.rt._impl.core.util as _util


@dc.dataclass(frozen=True)
class DataPartKey:

    @classmethod
    def for_root(cls) -> "DataPartKey":
        return DataPartKey(opaque_key='part-root')

    opaque_key: str


@dc.dataclass(frozen=True)
class DataSpec:

    object_type: _meta.ObjectType
    schema_type: _meta.SchemaType
    data_item: str

    definition: tp.Union[_meta.DataDefinition, _meta.FileDefinition]
    storage: _meta.StorageDefinition
    schema: tp.Optional[_meta.SchemaDefinition] = None

    primary_id: _meta.TagHeader = None
    storage_id: _meta.TagHeader = None
    schema_id: tp.Optional[_meta.TagHeader] = None
    context_key: tp.Optional[str] = None

    @staticmethod
    def create_data_spec(
            data_item: str,
            definition: _meta.DataDefinition,
            storage: _meta.StorageDefinition,
            schema: tp.Optional[_meta.SchemaDefinition] = None) -> "DataSpec":

        if schema:
            schema_type = schema.schemaType
        elif definition.schema:
            schema_type = definition.schema.schemaType
        else:
            schema_type = _meta.SchemaType.SCHEMA_TYPE_NOT_SET

        return DataSpec(
            _meta.ObjectType.DATA, schema_type, data_item,
            definition, storage, schema)

    @staticmethod
    def create_file_spec(
            data_item: str,
            definition: _meta.FileDefinition,
            storage: _meta.StorageDefinition) -> "DataSpec":

        return DataSpec(
            _meta.ObjectType.FILE, _meta.SchemaType.SCHEMA_TYPE_NOT_SET, data_item,
            definition, storage)

    @staticmethod
    def create_empty_spec(object_type: _meta.ObjectType, schema_type: _meta.SchemaType):
        return DataSpec(object_type, schema_type, "", None, None, None)  # noqa

    def with_ids(
            self, primary_id: _meta.TagHeader,
            storage_id: _meta.TagHeader,
            schema_id: tp.Optional[_meta.TagHeader] = None,
            context_key: tp.Optional[str] = None):

        return DataSpec(
            self.object_type, self.schema_type, self.data_item,
            self.definition, self.storage, self.schema,
            primary_id, storage_id, schema_id, context_key)

    def is_empty(self):
        return self.data_item is None or len(self.data_item) == 0


class StorageLayout:

    __LAYOUTS: "tp.Dict[_meta.StorageLayout, StorageLayout]" = dict()

    @classmethod
    def select(cls, layout_key: _meta.StorageLayout) -> "StorageLayout":

        # Legacy compatibility - layout key not set in storage definition
        if not layout_key or layout_key.value == 0:
            layout_key = _meta.StorageLayout.OBJECT_ID_LAYOUT

        layout = cls.__LAYOUTS.get(layout_key)

        if layout is not None:
            return layout

        if layout_key == _meta.StorageLayout.OBJECT_ID_LAYOUT:
            layout = ObjectIdLayout()
        elif layout_key == _meta.StorageLayout.DEVELOPER_LAYOUT:
            layout = DevelopmentLayout()
        else:
            raise _ex.ETracInternal(f"Unknown storage layout [{layout_key.name}]")

        cls.__LAYOUTS[layout_key] = layout

        return layout

    @abc.abstractmethod
    def layout_key(self) -> _meta.StorageLayout:
        pass

    @abc.abstractmethod
    def new_data_spec(
            self, data_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: tp.Optional[str], trac_schema: _meta.SchemaDefinition,
            storage_config: _cfg.StorageConfig) -> DataSpec:
        pass

    @abc.abstractmethod
    def new_data_version(
            self, data_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: tp.Optional[str], trac_schema: _meta.SchemaDefinition,
            prior_spec: DataSpec) -> DataSpec:
        pass

    @abc.abstractmethod
    def new_file_spec(
            self, file_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: str, file_type: _meta.FileType,
            storage_config: _cfg.StorageConfig) -> DataSpec:
        pass

    @abc.abstractmethod
    def new_file_version(
            self, file_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: str, file_type: _meta.FileType,
            prior_spec: DataSpec) -> DataSpec:
        pass


class BaseLayout(StorageLayout):

    __DATA_ITEM_TEMPLATE = "data/{}/{}/{}/snap-{:d}/delta-{:d}"
    __FILE_ITEM_TEMPLATE = "file/{}/version-{}"

    @abc.abstractmethod
    def _data_storage_path(
            self, data_id: _meta.TagHeader, context_key: str, trac_schema: _meta.SchemaDefinition,
            part_key: _meta.PartKey, snap_index: int, delta_index: int, storage_format: str,
            prior_copy: tp.Optional[_meta.StorageCopy]):
        pass

    @abc.abstractmethod
    def _file_storage_path(
            self, file_id: _meta.TagHeader, file_def: _meta.FileDefinition,
            prior_copy: tp.Optional[_meta.StorageCopy]):
        pass

    def new_data_spec(
            self, data_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: tp.Optional[str], trac_schema: _meta.SchemaDefinition,
            storage_config: _cfg.StorageConfig) -> DataSpec:

        part_key = _meta.PartKey("part-root", _meta.PartType.PART_ROOT)
        snap_index = 0

        data_item = self.__DATA_ITEM_TEMPLATE.format(
            trac_schema.schemaType.name.lower(), data_id.objectId,
            part_key.opaqueKey, snap_index, 0)

        # Blank data definition with no parts
        new_data_def = _meta.DataDefinition(
            schema=trac_schema,
            storageId=_util.selector_for_latest(storage_id))

        data_def = self._add_new_snap(new_data_def, data_item, part_key, snap_index)

        # Take default location from the storage config
        storage_key = storage_config.defaultBucket
        storage_format = "JSON" if trac_schema.schemaType == _meta.SchemaType.STRUCT else storage_config.defaultFormat
        storage_path = self._data_storage_path(data_id, context_key, trac_schema, part_key, snap_index, 0, storage_format, prior_copy=None)

        storage_copy = _meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=_meta.CopyStatus.COPY_AVAILABLE,
            copyTimestamp=data_id.objectTimestamp)

        new_storage_def = _meta.StorageDefinition()

        storage_def = self._add_storage_copy(new_storage_def, data_item, storage_copy)

        # Dynamic data def will always use an embedded schema (this is no ID for an external schema)

        return DataSpec \
            .create_data_spec(data_item, data_def, storage_def, schema=None) \
            .with_ids(data_id, storage_id)

    def new_data_version(
            self, data_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: tp.Optional[str], trac_schema: _meta.SchemaDefinition,
            prior_spec: DataSpec) -> DataSpec:

        part_key = _meta.PartKey("part-root", _meta.PartType.PART_ROOT)
        snap_index = prior_spec.primary_id.objectVersion  # snap index is zero-based

        data_item = self.__DATA_ITEM_TEMPLATE.format(
            trac_schema.schemaType.name.lower(), data_id.objectId,
            part_key.opaqueKey, snap_index, 0)

        data_def = self._add_new_snap(prior_spec.definition, data_item, part_key, snap_index)

        prior_item = next(iter(prior_spec.storage.dataItems.keys()), None)
        prior_copy = self._find_storage_copy(prior_item, prior_spec.storage)

        if prior_copy is None:
            raise _ex.ETracInternal(f"Missing prior metadata for [{_util.object_key(data_id)}]")

        storage_key = prior_copy.storageKey
        storage_format = prior_copy.storageFormat
        storage_path = self._data_storage_path(data_id, context_key, trac_schema, part_key, snap_index, 0, storage_format, prior_copy)

        storage_copy = _meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=_meta.CopyStatus.COPY_AVAILABLE,
            copyTimestamp=data_id.objectTimestamp)

        storage_def = self._add_storage_copy(prior_spec.storage, data_item, storage_copy)

        return DataSpec \
            .create_data_spec(data_item, data_def, storage_def, schema=None) \
            .with_ids(data_id, storage_id)

    def new_file_spec(
            self, file_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: str, file_type: _meta.FileType,
            storage_config: _cfg.StorageConfig) -> DataSpec:

        data_item = self.__FILE_ITEM_TEMPLATE.format(file_id.objectId, file_id.objectVersion)

        file_def = _meta.FileDefinition(
            name=f"{context_key}.{file_type.extension}",
            extension=file_type.extension,
            mimeType=file_type.mimeType,
            dataItem=data_item,
            storageId=_util.selector_for_latest(storage_id),
            size=0)

        storage_key = storage_config.defaultBucket
        storage_format = file_def.mimeType
        storage_path = self._file_storage_path(file_id, file_def, prior_copy=None)

        storage_copy = _meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=_meta.CopyStatus.COPY_AVAILABLE,
            copyTimestamp=file_id.objectTimestamp)

        new_storage_def = _meta.StorageDefinition()
        new_storage_def.layout = self.layout_key()

        storage_def = self._add_storage_copy(new_storage_def, data_item, storage_copy)

        return DataSpec \
            .create_file_spec(data_item, file_def, storage_def) \
            .with_ids(file_id, storage_id)

    def new_file_version(
            self, file_id: _meta.TagHeader, storage_id: _meta.TagHeader,
            context_key: str, file_type: _meta.FileType, prior_spec: DataSpec) -> DataSpec:

        data_item = self.__FILE_ITEM_TEMPLATE.format(file_id.objectId, file_id.objectVersion)

        file_def = _meta.FileDefinition(
            name=f"{context_key}.{file_type.extension}",
            extension=file_type.extension,
            mimeType=file_type.mimeType,
            dataItem=data_item,
            storageId=_util.selector_for_latest(storage_id),
            size=0)

        prior_copy = self._find_storage_copy(prior_spec.definition.dataItem, prior_spec.storage)

        if prior_copy is None:
            raise _ex.ETracInternal(f"Missing prior metadata for [{_util.object_key(file_id)}]")

        storage_key = prior_copy.storageKey
        storage_format = file_def.mimeType
        storage_path = self._file_storage_path(file_id, file_def, prior_copy=None)

        storage_copy = _meta.StorageCopy(
            storageKey=storage_key,
            storagePath=storage_path,
            storageFormat=storage_format,
            copyStatus=_meta.CopyStatus.COPY_AVAILABLE,
            copyTimestamp=file_id.objectTimestamp)

        storage_def = self._add_storage_copy(prior_spec.storage, data_item, storage_copy)

        return DataSpec \
            .create_file_spec(data_item, file_def, storage_def) \
            .with_ids(file_id, storage_id)

    @classmethod
    def _add_new_snap(
            cls, data_def: _meta.DataDefinition,data_item: str,
            part_key: _meta.PartKey, snap_index: int):

        delta = _meta.DataDefinition.Delta(
            deltaIndex=0,
            dataItem=data_item)

        snap = _meta.DataDefinition.Snap(
            snapIndex=snap_index,
            deltas=[delta])

        part = _meta.DataDefinition.Part(
            partKey=part_key,
            snap=snap)

        data_def = copy.copy(data_def)
        data_def.parts = copy.copy(data_def.parts)
        data_def.parts[part_key.opaqueKey] = part

        return data_def

    @classmethod
    def _add_storage_copy(cls, storage_def: _meta.StorageDefinition, data_item: str, storage_copy: _meta.StorageCopy):

        new_incarnation = _meta.StorageIncarnation(
            copies=[storage_copy],
            incarnationIndex=0,
            incarnationTimestamp=storage_copy.copyTimestamp,
            incarnationStatus=_meta.IncarnationStatus.INCARNATION_AVAILABLE)

        new_item = _meta.StorageItem(incarnations=[new_incarnation])

        storage_def = copy.copy(storage_def)
        storage_def.dataItems = copy.copy(storage_def.dataItems)
        storage_def.dataItems[data_item] = new_item

        return storage_def

    @classmethod
    def _find_storage_copy(cls, data_item: str, storage_def: _meta.StorageDefinition) -> tp.Optional[_meta.StorageCopy]:

        if data_item is None:
            return None

        storage_item = storage_def.dataItems.get(data_item)

        if storage_item is None:
            return None

        # Latest available incarnation
        incarnation = next(filter(
            lambda i: i.incarnationStatus == _meta.IncarnationStatus.INCARNATION_AVAILABLE,
            reversed(storage_item.incarnations)), None)

        if incarnation is None:
            return None

        # Use any available copy (currently there is no location preference)
        return next(filter(
            lambda c: c.copyStatus == _meta.CopyStatus.COPY_AVAILABLE,
            incarnation.copies), None)


class ObjectIdLayout(BaseLayout):

    __DATA_STORAGE_TEMPLATE = "data/{}/{}/{}/snap-{:d}/delta-{:d}-x{:0>6x}"
    __FILE_STORAGE_TEMPLATE = "file/{}/version-{:d}-x{:0>6x}/{}.{}"

    def __init__(self):
        self.__random = random.Random()
        self.__random.seed()

    def layout_key(self) -> _meta.StorageLayout:
        return _meta.StorageLayout.OBJECT_ID_LAYOUT

    def _data_storage_path(
            self, data_id, context_key, trac_schema,
            part_key, snap_index, delta_index,
            storage_format, prior_copy):

        schema_type = trac_schema.schemaType.name.lower()
        version_suffix = self.__random.randint(0, 1 << 24)

        return self.__DATA_STORAGE_TEMPLATE.format(
            schema_type, data_id.objectId,
            part_key.opaqueKey, snap_index, delta_index,
            version_suffix)

    def _file_storage_path(self, file_id, file_def, prior_copy):

        version_suffix = self.__random.randint(0, 1 << 24)

        return self.__FILE_STORAGE_TEMPLATE.format(
            file_id.objectId, file_id.objectVersion, version_suffix,
            file_def.name, file_def.extension.lower())


class DevelopmentLayout(BaseLayout):

    __DEFAULT_OUTPUT_DIR = "Dev Outputs"

    __DATA_STORAGE_PATH = "{}/{}{}.{}"
    __FILE_STORAGE_PATH = "{}/{}{}.{}"

    def layout_key(self) -> _meta.StorageLayout:
        return _meta.StorageLayout.DEVELOPER_LAYOUT

    def _data_storage_path(
            self, data_id, context_key, trac_schema,
            part_key, snap_index, delta_index,
            storage_format, prior_copy):

        storage_dir = self._dev_storage_dir(prior_copy)
        suffix = f"-{data_id.objectVersion}" if data_id.objectVersion > 1 else ""

        if prior_copy is not None:
            prior_path = pathlib.Path(prior_copy.storagePath)
            file_name = prior_path.stem
            if data_id.objectVersion > 2 and "-" in file_name:
                file_name = file_name[:file_name.rfind("-")]
        else:
            file_name = context_key

        return self.__DATA_STORAGE_PATH.format(storage_dir, file_name, suffix, storage_format.lower())

    def _file_storage_path(self, file_id, file_def, prior_copy):

        storage_dir = self._dev_storage_dir(prior_copy)
        suffix = f"-{file_id.objectVersion}" if file_id.objectVersion > 1 else ""

        return self.__FILE_STORAGE_PATH.format(storage_dir, file_def.name, suffix, file_def.extension.lower())

    def _dev_storage_dir(self, prior_copy: _meta.StorageCopy):

        if prior_copy is None:
            return self.__DEFAULT_OUTPUT_DIR

        prior_path = pathlib.Path(prior_copy.storagePath)

        if len(prior_path.parts) > 1:
            return prior_path.parent
        else:
            return self.__DEFAULT_OUTPUT_DIR


def build_data_spec(
        data_id: _meta.TagHeader, storage_id: _meta.TagHeader,
        context_key: tp.Optional[str], trac_schema: _meta.SchemaDefinition,
        storage_config: _cfg.StorageConfig,
        prior_spec: tp.Optional[DataSpec] = None) \
        -> DataSpec:

    if prior_spec is None:
        layout_key = storage_config.defaultLayout
        layout = StorageLayout.select(layout_key)
        return layout.new_data_spec(data_id, storage_id, context_key, trac_schema, storage_config)

    else:
        layout_key = prior_spec.storage.layout
        layout = StorageLayout.select(layout_key)
        return layout.new_data_version(data_id, storage_id, context_key, trac_schema, prior_spec)


def build_file_spec(
        file_id: _meta.TagHeader, storage_id: _meta.TagHeader,
        context_key: tp.Optional[str],  file_type: _meta.FileType,
        storage_config: _cfg.StorageConfig,
        prior_spec: tp.Optional[DataSpec] = None) \
        -> DataSpec:

    if prior_spec is None:
        layout_key = storage_config.defaultLayout
        layout = StorageLayout.select(layout_key)
        return layout.new_file_spec(file_id, storage_id, context_key, file_type, storage_config)

    else:
        layout_key = prior_spec.storage.layout
        layout = StorageLayout.select(layout_key)
        return layout.new_file_version(file_id, storage_id, context_key, file_type, prior_spec)


@dc.dataclass(frozen=True)
class DataItem:

    object_type: _meta.ObjectType
    schema_type: _meta.SchemaType

    content: tp.Any = None
    content_type: tp.Type = None
    content_func: tp.Callable[[], tp.Any] = None

    trac_schema: _meta.SchemaDefinition = None
    native_schema: tp.Any = None

    # TODO: Remove legacy API and use content / native_schema instead
    schema: pa.Schema = None
    table: tp.Optional[pa.Table] = None

    def is_empty(self) -> bool:
        return self.content is None

    @staticmethod
    def create_empty(
            object_type: _meta.ObjectType = _meta.ObjectType.DATA,
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) -> "DataItem":

        if object_type == _meta.ObjectType.DATA and schema_type == _meta.SchemaType.TABLE:
            return DataItem(_meta.ObjectType.DATA, _meta.SchemaType.TABLE, schema=pa.schema([]))
        else:
            return DataItem(object_type, schema_type)

    @staticmethod
    def for_table(table: pa.Table, schema: pa.Schema, trac_schema: _meta.SchemaDefinition) -> "DataItem":

        return DataItem(
            _meta.ObjectType.DATA, _meta.SchemaType.TABLE,
            content=table, content_type=pa.Table,
            trac_schema=trac_schema, native_schema=schema,
            table=table, schema=schema)

    @staticmethod
    def for_struct(content: tp.Any):

        return DataItem(
            _meta.ObjectType.DATA, _meta.SchemaType.STRUCT,
            content=content, content_type=type(content))

    @staticmethod
    def for_file_content(content: bytes):

        return DataItem(
            _meta.ObjectType.FILE, _meta.SchemaType.SCHEMA_TYPE_NOT_SET,
            content=content, content_type=bytes)


@dc.dataclass(frozen=True)
class DataView:

    object_type: _meta.ObjectType

    trac_schema: _meta.SchemaDefinition = None
    arrow_schema: pa.Schema = None

    parts: tp.Dict[DataPartKey, tp.List[DataItem]] = None
    file_item: tp.Optional[DataItem] = None

    @staticmethod
    def create_empty(object_type: _meta.ObjectType = _meta.ObjectType.DATA) -> "DataView":
        if object_type == _meta.ObjectType.DATA:
            return DataView(object_type, _meta.SchemaDefinition(), pa.schema([]), dict())
        else:
            return DataView(object_type)

    @staticmethod
    def for_trac_schema(trac_schema: _meta.SchemaDefinition):
        if trac_schema.schemaType == _meta.SchemaType.TABLE:
            arrow_schema = DataMapping.trac_to_arrow_schema(trac_schema)
            return DataView(_meta.ObjectType.DATA, trac_schema, arrow_schema, dict())
        else:
            return DataView(_meta.ObjectType.DATA, trac_schema, parts = dict())

    @staticmethod
    def for_arrow_schema(arrow_schema: pa.Schema):
        trac_schema = DataMapping.arrow_to_trac_schema(arrow_schema)
        return DataView(_meta.ObjectType.DATA, trac_schema, arrow_schema, dict())

    @staticmethod
    def for_file_item(file_item: DataItem):
        return DataView(file_item.object_type, file_item=file_item)

    def with_trac_schema(self, trac_schema: _meta.SchemaDefinition):
        arrow_schema = DataMapping.trac_to_arrow_schema(trac_schema)
        return DataView(_meta.ObjectType.DATA, trac_schema, arrow_schema, self.parts)

    def with_part(self, part_key: DataPartKey, part: DataItem):
        new_parts = copy.copy(self.parts)
        new_parts[part_key] = [part]
        return DataView(self.object_type, self.trac_schema, self.arrow_schema, new_parts)

    def with_file_item(self, file_item: DataItem):
        return DataView(self.object_type, file_item=file_item)

    def is_empty(self) -> bool:
        if self.object_type == _meta.ObjectType.FILE:
            return self.file_item is None
        else:
            return self.parts is None or not any(self.parts.values())


class _DataInternal:
    pass


class DataMapping:

    """
    Map primary data between different supported data frameworks, preserving equivalent data types.

    DataMapping is for primary data, to map metadata types and values use
    :py:class:`TypeMapping <tracdap.rt.impl.type_system.TypeMapping>` and
    :py:class:`TypeMapping <tracdap.rt.impl.type_system.MetadataCodec>`.
    """

    __log = _log.logger_for_namespace(_DataInternal.__module__ + ".DataMapping")

    # Matches TRAC_ARROW_TYPE_MAPPING in ArrowSchema, tracdap-lib-data

    DEFAULT_DECIMAL_PRECISION = 38
    DEFAULT_DECIMAL_SCALE = 12
    DEFAULT_TIMESTAMP_UNIT = "ms"
    DEFAULT_TIMESTAMP_ZONE = None

    __TRAC_TO_ARROW_BASIC_TYPE_MAPPING = {
        _meta.BasicType.BOOLEAN: pa.bool_(),
        _meta.BasicType.INTEGER: pa.int64(),
        _meta.BasicType.FLOAT: pa.float64(),
        _meta.BasicType.DECIMAL: pa.decimal128(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE),
        _meta.BasicType.STRING: pa.utf8(),
        _meta.BasicType.DATE: pa.date32(),
        _meta.BasicType.DATETIME: pa.timestamp(DEFAULT_TIMESTAMP_UNIT, DEFAULT_TIMESTAMP_ZONE)
    }

    __ARROW_TO_TRAC_BASIC_TYPE_MAPPING = {
        pa.bool_(): _meta.BasicType.BOOLEAN,
        pa.int8(): _meta.BasicType.INTEGER,
        pa.int16(): _meta.BasicType.INTEGER,
        pa.int32(): _meta.BasicType.INTEGER,
        pa.int64():_meta.BasicType.INTEGER,
        pa.uint8(): _meta.BasicType.INTEGER,
        pa.uint16(): _meta.BasicType.INTEGER,
        pa.uint32(): _meta.BasicType.INTEGER,
        pa.uint64(): _meta.BasicType.INTEGER,
        pa.float16(): _meta.BasicType.FLOAT,
        pa.float32(): _meta.BasicType.FLOAT,
        pa.float64(): _meta.BasicType.FLOAT,
        pa.string(): _meta.BasicType.STRING,
        pa.utf8(): _meta.BasicType.STRING,
        pa.date32(): _meta.BasicType.DATE,
        pa.date64(): _meta.BasicType.DATE
    }

    # For now, categorical handling is disabled by default and enabled by this setting
    # The default will change to "true" for the 0.9 release
    CATEGORICAL_CONFIG_KEY = "trac.runtime.categorical"
    __categorical_enabled = False

    @classmethod
    def enable_categorical(cls, enabled: bool):
        cls.__categorical_enabled = enabled

    @classmethod
    def arrow_to_python_type(cls, arrow_type: pa.DataType) -> type:

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

        # The python type for a dictionary-encoded field is its value type
        if pa.types.is_dictionary(arrow_type):
            if isinstance(arrow_type, pa.DictionaryType):
                return cls.arrow_to_python_type(arrow_type.value_type)

        raise _ex.ETracInternal(f"No Python type mapping available for Arrow type [{arrow_type}]")

    @classmethod
    def python_to_arrow_type(cls, python_type: type) -> pa.DataType:

        if python_type == bool:
            return pa.bool_()

        if python_type == int:
            return pa.int64()

        if python_type == float:
            return pa.float64()

        if python_type == decimal.Decimal:
            return pa.decimal128(cls.DEFAULT_DECIMAL_PRECISION, cls.DEFAULT_DECIMAL_SCALE)

        if python_type == str:
            return pa.utf8()

        if python_type == dt.date:
            return pa.date32()

        if python_type == dt.datetime:
            return pa.timestamp(cls.DEFAULT_TIMESTAMP_UNIT, cls.DEFAULT_TIMESTAMP_ZONE)

        raise _ex.ETracInternal(f"No Arrow type mapping available for Python type [{python_type}]")

    @classmethod
    def trac_to_arrow_type(cls, trac_type: _meta.TypeDescriptor) -> pa.DataType:

        return cls.trac_to_arrow_basic_type(trac_type.basicType)

    @classmethod
    def trac_to_arrow_basic_type(cls, trac_basic_type: _meta.BasicType) -> pa.DataType:

        arrow_type = cls.__TRAC_TO_ARROW_BASIC_TYPE_MAPPING.get(trac_basic_type)

        if arrow_type is None:
            raise _ex.ETracInternal(f"No Arrow type mapping available for TRAC type [{trac_basic_type}]")

        return arrow_type

    @classmethod
    def trac_to_arrow_schema(cls, trac_schema: _meta.SchemaDefinition) -> pa.Schema:

        if trac_schema.schemaType != _meta.SchemaType.TABLE:
            raise _ex.ETracInternal(f"Schema type [{trac_schema.schemaType}] cannot be converted for Apache Arrow")

        arrow_fields = list(map(cls.trac_to_arrow_field, trac_schema.table.fields))

        return pa.schema(arrow_fields, metadata={})

    @classmethod
    def trac_to_arrow_field(cls, trac_field: _meta.FieldSchema):

        arrow_type = cls.trac_to_arrow_basic_type(trac_field.fieldType)

        # Categorical data uses an unordered dictionary with int32 index, ordered encoding not (currently) supported
        # For legacy compatability, only use dictionary encoding if the categorical feature is enabled
        if trac_field.categorical and cls.__categorical_enabled:
            arrow_type = pa.dictionary(pa.int32(), arrow_type, False)

        nullable = not (trac_field.notNull or trac_field.businessKey)

        return pa.field(trac_field.fieldName, arrow_type, nullable)

    @classmethod
    def trac_arrow_decimal_type(cls) -> pa.Decimal128Type:

        return pa.decimal128(
            cls.DEFAULT_DECIMAL_PRECISION,
            cls.DEFAULT_DECIMAL_SCALE,)

    @classmethod
    def arrow_to_trac_schema(cls, arrow_schema: pa.Schema) -> _meta.SchemaDefinition:

        trac_fields = list(
            cls.arrow_to_trac_field(i, arrow_schema.field(i))
            for (i, f) in enumerate(arrow_schema.names))

        return _meta.SchemaDefinition(
            schemaType=_meta.SchemaType.TABLE,
            partType=_meta.PartType.PART_ROOT,
            table=_meta.TableSchema(trac_fields))

    @classmethod
    def arrow_to_trac_field(cls, field_index: int, field: pa.Field) -> _meta.FieldSchema:

        field_type = cls.arrow_to_trac_type(field.type)
        label = field.metadata["label"] if field.metadata and "label" in field.metadata else field.name

        # When converting Arrow -> TRAC, always set the categorical flag for dictionary encoded fields
        # This affects dynamic imports and is informational only (physical layout is controlled by Arrow schemas)

        return _meta.FieldSchema(
            field.name, field_index, field_type,
            label=label,
            businessKey=False,
            notNull=not field.nullable,
            categorical=pa.types.is_dictionary(field.type))

    @classmethod
    def arrow_to_trac_type(cls, arrow_type: pa.DataType) -> _meta.BasicType:

        mapped_basic_type = cls.__ARROW_TO_TRAC_BASIC_TYPE_MAPPING.get(arrow_type)  # noqa

        if mapped_basic_type is not None:
            return mapped_basic_type

        if pa.types.is_decimal(arrow_type):
            return _meta.BasicType.DECIMAL

        if pa.types.is_timestamp(arrow_type):
            return _meta.BasicType.DATETIME

        # The basic type for a dictionary-encoded field is its value type
        if pa.types.is_dictionary(arrow_type):
            if isinstance(arrow_type, pa.DictionaryType):
                return cls.arrow_to_trac_type(arrow_type.value_type)

        raise _ex.ETracInternal(f"No data type mapping available for Arrow type [{arrow_type}]")

    @classmethod
    def add_item_to_view(cls, view: DataView, part: DataPartKey, item: DataItem) -> DataView:

        prior_deltas = view.parts.get(part) or list()
        deltas = [*prior_deltas, item]
        parts = {**view.parts, part: deltas}

        return DataView(view.object_type, view.trac_schema, view.arrow_schema, parts=parts)

    @classmethod
    def view_to_arrow(cls, view: DataView, part: DataPartKey) -> pa.Table:

        deltas = view.parts.get(part)

        # Sanity checks

        if not view.arrow_schema:
            raise _ex.ETracInternal(f"Data view schema not set")

        if not deltas:
            raise _ex.ETracInternal(f"Data view for part [{part.opaque_key}] does not contain any items")

        # For a single delta, use the existing Arrow content
        if len(deltas) == 1:
            return cls.item_to_arrow(deltas[0])

        # For multiple deltas, construct a new table by assembling the record batches
        # Atm no consideration is given to overwriting records based on business key
        batches = iter(
            batch
            for delta in deltas
            for batch in cls.item_to_arrow(delta).to_batches())

        return pa.Table.from_batches(batches) # noqa

    @classmethod
    def item_to_arrow(cls, item: DataItem) -> pa.Table:

        if item.content_type != pa.Table:
            detail = f"expected Arrow table, got [{item.content_type}]"
            raise _ex.ETracInternal(f"Data item does not contain tabular data ({detail})")

        return item.content

    @classmethod
    def arrow_to_pandas(
            cls, table: pa.Table,
            schema: tp.Optional[pa.Schema] = None,
            temporal_objects_flag: bool = False) -> "pandas.DataFrame":

        # This is a legacy internal method and should be removed
        # DataMapping is no longer responsible for individual data APIs

        # Maintained temporarily for compatibility with existing deployments

        converter = PandasArrowConverter(_api.PANDAS, use_temporal_objects=temporal_objects_flag)
        return converter.from_internal(table, schema)

    @classmethod
    def pandas_to_arrow(
            cls, df: "pandas.DataFrame",
            schema: tp.Optional[pa.Schema] = None) -> pa.Table:

        # This is a legacy internal method and should be removed
        # DataMapping is no longer responsible for individual data APIs

        # Maintained temporarily for compatibility with existing deployments

        converter = PandasArrowConverter(_api.PANDAS)
        return converter.to_internal(df, schema)



T_DATA_API = tp.TypeVar("T_DATA_API")
T_INTERNAL_DATA = tp.TypeVar("T_INTERNAL_DATA")
T_INTERNAL_SCHEMA = tp.TypeVar("T_INTERNAL_SCHEMA")


class DataConverter(tp.Generic[T_DATA_API, T_INTERNAL_DATA, T_INTERNAL_SCHEMA]):

    # Available per-framework args, to enable framework-specific type-checking in public APIs
    # These should (for a purist point of view) be in the individual converter classes
    # For now there are only a few converters, they are all defined here so this is OK
    __FRAMEWORK_ARGS = {
        _api.PANDAS: {"use_temporal_objects": tp.Optional[bool]},
        _api.POLARS: {}
    }

    @classmethod
    def get_framework(cls, dataset: _api.DATA_API) -> _api.DataFramework[_api.DATA_API]:

        if pandas is not None and isinstance(dataset, pandas.DataFrame):
            return _api.PANDAS

        if polars is not None and isinstance(dataset, polars.DataFrame):
            return _api.POLARS

        data_api_type = f"{type(dataset).__module__}.{type(dataset).__name__}"
        raise _ex.EPluginNotAvailable(f"No data framework available for type [{data_api_type}]")

    @classmethod
    def get_framework_args(cls, framework: _api.DataFramework[_api.DATA_API]) -> tp.Dict[str, type]:

        return cls.__FRAMEWORK_ARGS.get(framework) or {}

    @classmethod
    def for_framework(cls, framework: _api.DataFramework[_api.DATA_API], **framework_args) -> "DataConverter[_api.DATA_API, pa.Table, pa.Schema]":

        if framework == _api.PANDAS:
            if pandas is not None:
                return PandasArrowConverter(framework, **framework_args)
            else:
                raise _ex.EPluginNotAvailable(f"Optional package [{framework}] is not installed")

        if framework == _api.POLARS:
            if polars is not None:
                return PolarsArrowConverter(framework)
            else:
                raise _ex.EPluginNotAvailable(f"Optional package [{framework}] is not installed")

        raise _ex.EPluginNotAvailable(f"Data framework [{framework}] is not recognized")

    @classmethod
    def for_dataset(cls, dataset: _api.DATA_API) -> "DataConverter[_api.DATA_API, pa.Table, pa.Schema]":

        return cls.for_framework(cls.get_framework(dataset))

    @classmethod
    def noop(cls) -> "DataConverter[T_INTERNAL_DATA, T_INTERNAL_DATA, T_INTERNAL_SCHEMA]":
        return NoopConverter()

    def __init__(self, framework: _api.DataFramework[T_DATA_API]):
        self.framework = framework

    @abc.abstractmethod
    def from_internal(self, dataset: T_INTERNAL_DATA, schema: tp.Optional[T_INTERNAL_SCHEMA] = None) -> T_DATA_API:
        pass

    @abc.abstractmethod
    def to_internal(self, dataset: T_DATA_API, schema: tp.Optional[T_INTERNAL_SCHEMA] = None) -> T_INTERNAL_DATA:
        pass

    @abc.abstractmethod
    def infer_schema(self, dataset: T_DATA_API) -> _meta.SchemaDefinition:
        pass


class NoopConverter(DataConverter[T_INTERNAL_DATA, T_INTERNAL_DATA, T_INTERNAL_SCHEMA]):

    def __init__(self):
        super().__init__(_api.DataFramework("internal", None))  # noqa

    def from_internal(self, dataset: T_INTERNAL_DATA, schema: tp.Optional[T_INTERNAL_SCHEMA] = None) -> T_DATA_API:
        return dataset

    def to_internal(self, dataset: T_DATA_API, schema: tp.Optional[T_INTERNAL_SCHEMA] = None) -> T_INTERNAL_DATA:
        return dataset

    def infer_schema(self, dataset: T_DATA_API) -> _meta.SchemaDefinition:
        raise _ex.EUnexpected()  # A real converter should be selected before use


# Data frameworks are optional, do not blow up the module just because one framework is unavailable!
if pandas is not None:

    class PandasArrowConverter(DataConverter[pandas.DataFrame, pa.Table, pa.Schema]):

        # Check the Pandas dtypes for handling floats are available before setting up the type mapping
        __PANDAS_VERSION_ELEMENTS = pandas.__version__.split(".")
        __PANDAS_MAJOR_VERSION = int(__PANDAS_VERSION_ELEMENTS[0])
        __PANDAS_MINOR_VERSION = int(__PANDAS_VERSION_ELEMENTS[1])

        if __PANDAS_MAJOR_VERSION == 2:

            __PANDAS_DATE_TYPE = pandas.to_datetime([dt.date(2000, 1, 1)]).as_unit(DataMapping.DEFAULT_TIMESTAMP_UNIT).dtype
            __PANDAS_DATETIME_TYPE = pandas.to_datetime([dt.datetime(2000, 1, 1, 0, 0, 0)]).as_unit(DataMapping.DEFAULT_TIMESTAMP_UNIT).dtype

            @classmethod
            def __pandas_datetime_type(cls, tz, unit):
                if tz is None and unit is None:
                    return cls.__PANDAS_DATETIME_TYPE
                _unit = unit if unit is not None else DataMapping.DEFAULT_TIMESTAMP_UNIT
                if tz is None:
                    return pandas.to_datetime([dt.datetime(2000, 1, 1, 0, 0, 0)]).as_unit(_unit).dtype
                else:
                    return pandas.DatetimeTZDtype(tz=tz, unit=_unit)

        # Minimum supported version for Pandas is 1.2, when pandas.Float64Dtype was introduced
        elif __PANDAS_MAJOR_VERSION == 1 and __PANDAS_MINOR_VERSION >= 2:

            __PANDAS_DATE_TYPE = pandas.to_datetime([dt.date(2000, 1, 1)]).dtype
            __PANDAS_DATETIME_TYPE = pandas.to_datetime([dt.datetime(2000, 1, 1, 0, 0, 0)]).dtype

            @classmethod
            def __pandas_datetime_type(cls, tz, unit):  # noqa
                if tz is None:
                    return cls.__PANDAS_DATETIME_TYPE
                else:
                    return pandas.DatetimeTZDtype(tz=tz)

        else:
            raise _ex.EStartup(f"Pandas version not supported: [{pandas.__version__}]")

        # Only partial mapping is possible, decimal and temporal dtypes cannot be mapped this way
        __ARROW_TO_PANDAS_TYPE_MAPPING = {
            pa.bool_(): pandas.BooleanDtype(),
            pa.int8(): pandas.Int8Dtype(),
            pa.int16(): pandas.Int16Dtype(),
            pa.int32(): pandas.Int32Dtype(),
            pa.int64(): pandas.Int64Dtype(),
            pa.uint8(): pandas.UInt8Dtype(),
            pa.uint16(): pandas.UInt16Dtype(),
            pa.uint32(): pandas.UInt32Dtype(),
            pa.uint64(): pandas.UInt64Dtype(),
            pa.float16(): pandas.Float32Dtype(),
            pa.float32(): pandas.Float32Dtype(),
            pa.float64(): pandas.Float64Dtype(),
            pa.string(): pandas.StringDtype(),
            pa.utf8(): pandas.StringDtype()
        }

        __DEFAULT_TEMPORAL_OBJECTS = False

        # Expose date type for testing
        @classmethod
        def pandas_date_type(cls):
            return cls.__PANDAS_DATE_TYPE

        # Expose datetime type for testing
        @classmethod
        def pandas_datetime_type(cls, tz=None, unit=None):
            return cls.__pandas_datetime_type(tz, unit)

        def __init__(self, framework: _api.DataFramework[T_DATA_API], use_temporal_objects: tp.Optional[bool] = None):
            super().__init__(framework)
            if use_temporal_objects is None:
                self.__temporal_objects_flag = self.__DEFAULT_TEMPORAL_OBJECTS
            else:
                self.__temporal_objects_flag = use_temporal_objects

        def from_internal(self, table: pa.Table, schema: tp.Optional[pa.Schema] = None) -> pandas.DataFrame:

            if schema is not None:
                table = DataConformance.conform_to_schema(table, schema, warn_extra_columns=False)
            else:
                DataConformance.check_duplicate_fields(table.schema.names, False)

                # Use Arrow's built-in function to convert to Pandas
            return table.to_pandas(

                # Mapping for arrow -> pandas types for core types
                types_mapper=self.__ARROW_TO_PANDAS_TYPE_MAPPING.get,

                # Use Python objects for dates and times if temporal_objects_flag is set
                date_as_object=self.__temporal_objects_flag,  # noqa
                timestamp_as_object=self.__temporal_objects_flag,  # noqa

                # Do not bring any Arrow metadata into Pandas dataframe
                ignore_metadata=True,  # noqa

                # Do not consolidate memory across columns when preparing the Pandas vectors
                # This is a significant performance win for very wide datasets
                split_blocks=True)  # noqa

        def to_internal(self, df: pandas.DataFrame, schema: tp.Optional[pa.Schema] = None) -> pa.Table:

            # Converting pandas -> arrow needs care to ensure type coercion is applied correctly
            # Calling Table.from_pandas with the supplied schema will very often reject data
            # Instead, we convert the dataframe as-is and then apply type conversion in a second step
            # This allows us to apply specific coercion rules for each data type

            # As an optimisation, the column filter means columns will not be converted if they are not needed
            # E.g. if a model outputs lots of undeclared columns, there is no need to convert them

            column_filter = DataConformance.column_filter(df.columns, schema)  # noqa

            if len(df) > 0:

                table = pa.Table.from_pandas(df, columns=column_filter, preserve_index=False)  # noqa

            # Special case handling for converting an empty dataframe
            # These must flow through the pipe with valid schemas, like any other dataset
            # Type coercion and column filtering happen in conform_to_schema, if a schema has been supplied

            else:

                empty_df = df.filter(column_filter) if column_filter else df
                empty_schema = pa.Schema.from_pandas(empty_df, preserve_index=False)  # noqa

                table = pa.Table.from_batches(list(), empty_schema)  # noqa

            # If there is no explict schema, give back the table exactly as it was received from Pandas
            # There could be an option here to infer and coerce for TRAC standard types
            # E.g. unsigned int 32 -> signed int 64, TRAC standard integer type

            if schema is None:
                DataConformance.check_duplicate_fields(table.schema.names, False)
                return table

            # If a schema has been supplied, apply data conformance
            # If column filtering has been applied, we also need to filter the pandas dtypes used for hinting

            else:
                df_types = df.dtypes.filter(column_filter) if column_filter else df.dtypes
                return DataConformance.conform_to_schema(table, schema, df_types)

        def infer_schema(self, dataset: pandas.DataFrame) -> _meta.SchemaDefinition:

            arrow_schema = pa.Schema.from_pandas(dataset, preserve_index=False)  # noqa
            return DataMapping.arrow_to_trac_schema(arrow_schema)


# Data frameworks are optional, do not blow up the module just because one framework is unavailable!
if polars is not None:

    class PolarsArrowConverter(DataConverter[polars.DataFrame, pa.Table, pa.Schema]):

        def __init__(self, framework: _api.DataFramework[T_DATA_API]):
            super().__init__(framework)

        def from_internal(self, table: pa.Table, schema: tp.Optional[pa.Schema] = None) -> polars.DataFrame:

            if schema is not None:
                table = DataConformance.conform_to_schema(table, schema, warn_extra_columns=False)
            else:
                DataConformance.check_duplicate_fields(table.schema.names, False)

            return polars.from_arrow(table)

        def to_internal(self, df: polars.DataFrame, schema: tp.Optional[pa.Schema] = None,) -> pa.Table:

            column_filter = DataConformance.column_filter(df.columns, schema)

            filtered_df = df.select(polars.col(*column_filter)) if column_filter else df
            table = filtered_df.to_arrow()

            if schema is None:
                DataConformance.check_duplicate_fields(table.schema.names, False)
                return table
            else:
                return DataConformance.conform_to_schema(table, schema, None)

        def infer_schema(self, dataset: T_DATA_API) -> _meta.SchemaDefinition:

            arrow_schema = dataset.top_k(1).to_arrow().schema
            return DataMapping.arrow_to_trac_schema(arrow_schema)


class DataConformance:

    """
    Check and/or apply conformance between datasets and schemas.
    """

    __log = _log.logger_for_namespace(_DataInternal.__module__ + ".DataConformance")

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

    __E_WRONG_CATEGORICAL_TYPE = \
        "Field [{field_name}] categorical types do not match" + \
        "(expected {field_type}, got {vector_type})"

    @classmethod
    def column_filter(cls, columns: tp.List[str], schema: tp.Optional[pa.Schema]) -> tp.Optional[tp.List[str]]:

        cls.check_duplicate_fields(columns, False)

        if not schema:
            return None

        cls.check_duplicate_fields(schema.names, True)

        schema_columns = set(c.lower() for c in schema.names)
        extra_columns = list(filter(lambda c: c.lower() not in schema_columns, columns))

        if not any(extra_columns):
            return None

        column_filter = list(filter(lambda c: c.lower() in schema_columns, columns))

        message = f"Columns not defined in the schema will be dropped: {', '.join(extra_columns)}"
        cls.__log.warning(message)

        return column_filter

    @classmethod
    def conform_to_schema(
            cls, table: pa.Table, schema: pa.Schema,
            pandas_types=None, warn_extra_columns=True) \
            -> pa.Table:

        """
        Align an Arrow table to an Arrow schema.

        Columns will be matched using case-insensitive matching and columns not in the schema will be dropped.
        The resulting table will have the field order and case defined in the schema.

        Where column types do not match exactly, type coercion will be applied if possible.
        In some cases type coercion may result in overflows,
        for example casting int64 -> int32 will fail if any values are greater than the maximum int32 value.

        If the incoming data has been converted from Pandas, there are some conversions that can be applied
        if the original Pandas dtype is known. These dtypes can be supplied via the pandas_dtypes parameter
        and should line up with the data in the table (i.e. dtypes are for the source data, not the target schema).

        The method will return a dataset whose schema exactly matches the requested schema.
        If it is not possible to make the data conform to the schema for any reason, EDataConformance will be raised.

        :param table: The data to be conformed
        :param schema: The schema to conform to
        :param pandas_types: Pandas dtypes for the table, if the table has been converted from Pandas
        :param warn_extra_columns: Whether to log warnings it the table contains columns not in the schema
        :return: The conformed data, whose schema will exactly match the supplied schema parameter
        :raises: _ex.EDataConformance if conformance is not possible for any reason
        """

        # If Pandas types are supplied they must match the table, i.e. table has been converted from Pandas
        if pandas_types is not None and len(pandas_types) != len(table.schema.types):
            raise _ex.EUnexpected()

        cls.check_duplicate_fields(schema.names, True)
        cls.check_duplicate_fields(table.schema.names, False)

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

                pandas_type = pandas_types.iloc[table_index] \
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

            schema_columns = set(map(lambda c: c.lower(), schema.names))
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
    def check_duplicate_fields(cls, fields: tp.List[str], schema_or_table: bool):

        check = {}

        for field in fields:
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

        # Handle null vector
        if pa.types.is_null(vector.type):
            if field.nullable:
                return pa.nulls(size=len(vector), type=field.type)
            else:
                raise _ex.EDataConformance(f"All null values in non-null field [{field.name}]")

        # If the vector is dict-encoded but the expected result is not, decode the dictionary
        if pa.types.is_dictionary(vector.type) and not pa.types.is_dictionary(field.type):
            if isinstance(vector, pa.DictionaryArray):
                dict_vector: pa.DictionaryArray = vector
                vector = dict_vector.dictionary_decode()

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

        if pa.types.is_dictionary(field.type):
            return cls._coerce_dictionary(vector, field)

        error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
        cls.__log.error(error_message)
        raise _ex.EDataConformance(error_message)

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

        try:

            if pa.types.is_string(field.type):
                if pa.types.is_string(vector.type):
                    return vector
                # Try to down-cast large string -> string, will raise ArrowInvalid if data does not fit
                if pa.types.is_large_string(vector.type):
                    return pc.cast(vector, field.type, safe=True)

            if pa.types.is_large_string(field.type):
                if pa.types.is_large_string(vector.type):
                    return vector
                # Allow up-casting string -> large_string
                if pa.types.is_string(vector.type):
                    return pc.cast(vector, field.type)

            error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message)

        except pa.ArrowInvalid as e:

            error_message = cls._format_error(cls.__E_DATA_LOSS_DID_OCCUR, vector, field, e)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message) from e


    @classmethod
    def _coerce_date(cls, vector: pa.Array, field: pa.Field, pandas_type=None) -> pa.Array:

        # The bit-width restriction could be removed here
        # For date types there is never loss of precision and pa.cast will raise an error on overflow
        # Impact to client code is unlikely, still this change should happen with a TRAC minor version update
        if pa.types.is_date(vector.type):
            if field.type.bit_width >= vector.type.bit_width:
                return pc.cast(vector, field.type)

        # Special handling for date values coming from Pandas/NumPy
        # Only allow these conversions if the vector is supplied with Pandas type info
        # For Pandas 1.x, dates are always encoded as np.datetime64[ns]
        # For Pandas 2.x dates are still np.datetime64 but can be in s, ms, us or ns
        # This conversion will not apply to dates held in Pandas using the Python date object types
        if pandas_type is not None:
            if pa.types.is_timestamp(vector.type) and pandas.api.types.is_datetime64_any_dtype(pandas_type):
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
    def _coerce_dictionary(cls, vector: pa.Array, field: pa.Field):

        try:

            if not isinstance(field.type, pa.DictionaryType):
                raise _ex.EUnexpected()

            field_type: pa.DictionaryType = field.type

            # Supplied vector is a dictionary (but the dictionary type is not an exact match)
            if pa.types.is_dictionary(vector.type):

                if not isinstance(vector.type, pa.DictionaryType):
                    raise _ex.EUnexpected()

                vector_type: pa.DictionaryType = vector.type

                # Do not allow coercion to a smaller index type or from unordered to ordered
                if (vector_type.index_type.bit_width > field_type.index_type.bit_width) or \
                   (field_type.ordered and not vector_type.ordered):

                    error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
                    cls.__log.error(error_message)
                    raise _ex.EDataConformance(error_message)

                # Value types are the same - basic cast should succeed
                if vector_type.value_type == field_type.value_type:
                    return pc.cast(vector, field.type)

                # Value types differ - try to coerce the underlying dictionary
                elif isinstance(vector, pa.DictionaryArray):
                    try:
                        values_field = pa.field(field.name, field_type.value_type, field.nullable)
                        values_vector = cls._coerce_vector(vector.dictionary, values_field)
                        dict_vector = pa.DictionaryArray.from_arrays(vector.indices, values_vector, ordered=field_type.ordered)  # noqa
                        return pc.cast(dict_vector, field.type)
                    # Handle errors converting the value type
                    except _ex.EDataConformance as e:
                        error_message = cls._format_error(cls.__E_WRONG_CATEGORICAL_TYPE, vector, field)
                        cls.__log.error(error_message)
                        raise _ex.EDataConformance(error_message) from e

                # Special handling for chunked dictionaries
                elif isinstance(vector, pa.ChunkedArray):
                    chunks = [cls._coerce_dictionary(chunk, field) for chunk in vector.chunks]
                    return pa.chunked_array(chunks)

                # Vector type not recognized, coercion is not possible
                else:
                    error_message = cls._format_error(cls.__E_WRONG_DATA_TYPE, vector, field)
                    cls.__log.error(error_message)
                    raise _ex.EDataConformance(error_message)

            # Supplied vector matches the dictionary value type - perform dictionary encoding
            elif vector.type == field_type.value_type and not field_type.ordered:
                return vector.dictionary_encode().cast(field.type)

            # Fallback option - try to coerce the value type first, then perform dictionary encoding
            else:
                try:
                    values_field = pa.field(field.name, field_type.value_type, field.nullable)
                    values_vector = cls._coerce_vector(vector, values_field)
                    return values_vector.dictionary_encode().cast(field.type)
                # Handle errors converting the value type
                except _ex.EDataConformance as e:
                    error_message = cls._format_error(cls.__E_WRONG_CATEGORICAL_TYPE, vector, field)
                    cls.__log.error(error_message)
                    raise _ex.EDataConformance(error_message) from e

        except pa.ArrowInvalid as e:

            error_message = cls._format_error(cls.__E_DATA_LOSS_DID_OCCUR, vector, field, e)
            cls.__log.error(error_message)
            raise _ex.EDataConformance(error_message) from e

    @classmethod
    def _format_error(cls, error_template: str, vector: pa.Array, field: pa.Field, e: Exception = None):

        return error_template.format(
            field_name=field.name,
            field_type=field.type,
            vector_type=vector.type,
            error_details=str(e))
