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

import contextlib
import copy
import io
import logging
import pathlib
import typing as tp
import re
import traceback

import tracdap.rt.api as _api
import tracdap.rt.api.experimental as _eapi
import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.data as _data
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.struct as _struct
import tracdap.rt._impl.core.type_system as _types
import tracdap.rt._impl.core.util as _util
import tracdap.rt._impl.core.validation as _val


class TracContextImpl(_api.TracContext):

    """
    TracContextImpl is the main implementation of the API class TracContext (from tracdap.rt.api).
    It provides get/put operations on the inputs, outputs and parameters of a model according to the model definition,
    as well as exposing other information needed by the model at runtime and offering a few utility functions.

    An instance of TracContextImpl is constructed by the runtime engine for each model node in the execution graph.
    Parameters and schemas will be pre-populated from the job definition. Normally, input data will be created by parent
    nodes in the execution graph and passed in when the context is constructed, so they are available when required. In
    the simplest case, a get call picks a dataset from the map and a put call puts a dataset into the map. Under this
    mechanism, no outputs are passed downstream until the model finishes executing.

    Optimizations for lazy loading and eager saving require the context to call back into the runtime engine. For lazy
    load, the graph node to prepare an input is injected when the data is requested and the model thread blocks until
    it is available; for eager save outputs are sent to child actors as soon as they are produced. In both cases this
    complexity is hidden from the model, which only sees one thread with synchronous get/put calls.

    :param model_def: Definition object for the model that will run in this context
    :param model_class: Type for the model that will run in this context
    :param local_ctx: Dictionary of all parameters, inputs and outputs that will be available to the model
            Parameters are supplied as python native types.
            Output views will contain schemas but no data.
    """

    def __init__(self,
                 model_def: _meta.ModelDefinition,
                 model_class: _api.TracModel.__class__,
                 local_ctx: tp.Dict[str, tp.Any],
                 dynamic_outputs: tp.List[str] = None,
                 checkout_directory: pathlib.Path = None,
                 log_provider: _logging.LogProvider = None):

        # If no log provider is supplied, use the default (system logs only)
        if log_provider is None:
            log_provider = _logging.LogProvider()

        self.__ctx_log = log_provider.logger_for_object(self)
        self.__model_log = log_provider.logger_for_class(model_class)

        self.__model_def = model_def
        self.__model_class = model_class
        self.__local_ctx = local_ctx if local_ctx is not None else {}
        self.__dynamic_outputs = dynamic_outputs if dynamic_outputs is not None else []

        self.__val = TracContextValidator(
            self.__ctx_log,
            self.__model_def,
            self.__local_ctx,
            self.__dynamic_outputs,
            checkout_directory)

    def get_parameter(self, parameter_name: str) -> tp.Any:

        _val.validate_signature(self.get_parameter, parameter_name)

        self.__val.check_item_valid_identifier(parameter_name, TracContextValidator.PARAMETER)
        self.__val.check_item_defined_in_model(parameter_name, TracContextValidator.PARAMETER)
        self.__val.check_item_available_in_context(parameter_name, TracContextValidator.PARAMETER)

        value: _meta.Value = self.__local_ctx.get(parameter_name)

        self.__val.check_context_object_type(parameter_name, value, _meta.Value)

        return _types.MetadataCodec.decode_value(value)

    def has_dataset(self, dataset_name: str) -> bool:

        _val.validate_signature(self.has_dataset, dataset_name)

        self.__val.check_item_valid_identifier(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_defined_in_model(dataset_name, TracContextValidator.DATASET)

        data_view: _data.DataView = self.__local_ctx.get(dataset_name)

        if data_view is None:
            return False

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_context_data_view_type(dataset_name, data_view, _meta.ObjectType.DATA)

        return not data_view.is_empty()

    def get_schema(self, dataset_name: str) -> _meta.SchemaDefinition:

        _val.validate_signature(self.get_schema, dataset_name)

        self.__val.check_item_valid_identifier(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_defined_in_model(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_available_in_context(dataset_name, TracContextValidator.DATASET)

        static_schema = self.__get_static_schema(self.__model_def, dataset_name)
        data_view: _data.DataView = self.__local_ctx.get(dataset_name)

        # Check the data view has a well-defined schema even if a static schema exists in the model
        # This ensures errors are always reported and is consistent with get_pandas_table()

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_context_data_view_type(dataset_name, data_view, _meta.ObjectType.DATA)
        self.__val.check_dataset_schema_defined(dataset_name, data_view)

        # If a static schema exists, that takes priority
        # Return deep copies, do not allow model code to change schemas provided by the engine

        if static_schema is not None:
            return copy.deepcopy(static_schema)
        else:
            return copy.deepcopy(data_view.trac_schema)

    def get_table(self, dataset_name: str, framework: _eapi.DataFramework[_eapi.DATA_API], **framework_args) -> _eapi.DATA_API:

        _val.validate_signature(self.get_table, dataset_name, framework)
        _val.require_package(framework.protocol_name, framework.api_type)

        self.__val.check_item_valid_identifier(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_defined_in_model(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_available_in_context(dataset_name, TracContextValidator.DATASET)
        self.__val.check_data_framework_args(framework, framework_args)

        static_schema = self.__get_static_schema(self.__model_def, dataset_name)
        data_view = self.__local_ctx.get(dataset_name)
        part_key = _data.DataPartKey.for_root()

        converter = _data.DataConverter.for_framework(framework, **framework_args)

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_context_data_view_type(dataset_name, data_view, _meta.ObjectType.DATA)
        self.__val.check_dataset_schema_defined(dataset_name, data_view)
        self.__val.check_dataset_part_present(dataset_name, data_view, part_key)

        # If the model defines a static input schema, use that for schema conformance
        # Otherwise, take what is in the incoming dataset (schema is dynamic)

        if static_schema is not None:
            schema = _data.DataMapping.trac_to_arrow_schema(static_schema)
        else:
            schema = data_view.arrow_schema

        table = _data.DataMapping.view_to_arrow(data_view, part_key)

        # Data conformance is applied automatically inside the converter, if schema != None
        return converter.from_internal(table, schema)

    def get_pandas_table(self, dataset_name: str, use_temporal_objects: tp.Optional[bool] = None)  -> "_data.pandas.DataFrame":

        return self.get_table(dataset_name, _eapi.PANDAS, use_temporal_objects=use_temporal_objects)

    def get_polars_table(self, dataset_name: str) -> "_data.polars.DataFrame":

        return self.get_table(dataset_name, _eapi.POLARS)

    def get_struct(self, struct_name: str, python_class: type[_eapi.STRUCT_TYPE] = None) -> _eapi.STRUCT_TYPE:

        _val.validate_signature(self.get_struct, struct_name, python_class)

        self.__val.check_item_valid_identifier(struct_name, TracContextValidator.DATASET)
        self.__val.check_item_defined_in_model(struct_name, TracContextValidator.DATASET)
        self.__val.check_item_available_in_context(struct_name, TracContextValidator.DATASET)

        data_view: _data.DataView = self.__local_ctx.get(struct_name)
        part_key = _data.DataPartKey.for_root()

        self.__val.check_context_object_type(struct_name, data_view, _data.DataView)
        self.__val.check_context_data_view_type(struct_name, data_view, _meta.ObjectType.DATA)
        self.__val.check_dataset_schema_defined(struct_name, data_view)

        struct_data: dict = data_view.parts[part_key][0].content
        return _struct.StructProcessor.parse_struct(struct_data, None, python_class)

    def get_file(self, file_name: str) -> bytes:

        _val.validate_signature(self.get_file, file_name)

        self.__val.check_item_valid_identifier(file_name, TracContextValidator.FILE)
        self.__val.check_item_defined_in_model(file_name, TracContextValidator.FILE)
        self.__val.check_item_available_in_context(file_name, TracContextValidator.FILE)
        
        file_view: _data.DataView = self.__local_ctx.get(file_name)

        self.__val.check_context_object_type(file_name, file_view, _data.DataView)
        self.__val.check_context_data_view_type(file_name, file_view, _meta.ObjectType.FILE)
        self.__val.check_file_content_present(file_name, file_view)
        
        return file_view.file_item.content

    def get_file_stream(self, file_name: str) -> tp.ContextManager[tp.BinaryIO]:

        buffer = self.get_file(file_name)
        return contextlib.closing(io.BytesIO(buffer))

    def put_schema(self, dataset_name: str, schema: _meta.SchemaDefinition):

        _val.validate_signature(self.get_schema, dataset_name, schema)

        # Copy the schema - schema cannot be changed in model code after put_schema
        # If field ordering is not assigned by the model, assign it here (model code will not see the numbers)
        schema_copy = self.__assign_field_order(copy.deepcopy(schema))

        self.__val.check_item_valid_identifier(dataset_name, TracContextValidator.DATASET)
        self.__val.check_dataset_is_dynamic_output(dataset_name)
        self.__val.check_provided_schema_is_valid(dataset_name, schema_copy)

        static_schema = self.__get_static_schema(self.__model_def, dataset_name)
        data_view = self.__local_ctx.get(dataset_name)

        if data_view is None:
            if static_schema is not None:
                data_view = _data.DataView.for_trac_schema(static_schema)
            else:
                data_view = _data.DataView.create_empty()

        # If there is a prior view it must contain nothing and will be replaced
        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_context_data_view_type(dataset_name, data_view, _meta.ObjectType.DATA)
        self.__val.check_dataset_schema_not_defined(dataset_name, data_view)
        self.__val.check_dataset_is_empty(dataset_name, data_view)

        updated_view = data_view.with_trac_schema(schema_copy)

        self.__local_ctx[dataset_name] = updated_view

    def put_table(
            self, dataset_name: str, dataset: _eapi.DATA_API,
            framework: tp.Optional[_eapi.DataFramework[_eapi.DATA_API]] = None,
            **framework_args):

        _val.validate_signature(self.put_table, dataset_name, dataset, framework)

        if framework is None:
            framework = _data.DataConverter.get_framework(dataset)

        _val.require_package(framework.protocol_name, framework.api_type)

        self.__val.check_item_valid_identifier(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_is_model_output(dataset_name, TracContextValidator.DATASET)
        self.__val.check_provided_dataset_type(dataset, framework.api_type)
        self.__val.check_data_framework_args(framework, framework_args)

        static_schema = self.__get_static_schema(self.__model_def, dataset_name)
        data_view = self.__local_ctx.get(dataset_name)
        part_key = _data.DataPartKey.for_root()

        converter = _data.DataConverter.for_framework(framework)

        if data_view is None:
            if static_schema is not None:
                data_view = _data.DataView.for_trac_schema(static_schema)
            else:
                data_view = _data.DataView.create_empty()

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_context_data_view_type(dataset_name, data_view, _meta.ObjectType.DATA)
        self.__val.check_dataset_schema_defined(dataset_name, data_view)
        self.__val.check_dataset_part_not_present(dataset_name, data_view, part_key)

        # Prefer static schemas for data conformance

        if static_schema is not None:
            trac_schema = static_schema
            native_schema = _data.DataMapping.trac_to_arrow_schema(static_schema)
        else:
            trac_schema = _data.DataMapping.arrow_to_trac_schema(data_view.arrow_schema)
            native_schema = data_view.arrow_schema

        # Data conformance is applied automatically inside the converter, if schema != None
        table = converter.to_internal(dataset, native_schema)
        item = _data.DataItem.for_table(table, native_schema, trac_schema)

        updated_view = _data.DataMapping.add_item_to_view(data_view, part_key, item)

        self.__local_ctx[dataset_name] = updated_view

    def put_pandas_table(self, dataset_name: str, dataset: "_data.pandas.DataFrame"):

        self.put_table(dataset_name, dataset, _eapi.PANDAS)

    def put_polars_table(self, dataset_name: str, dataset: "_data.polars.DataFrame"):

        self.put_table(dataset_name, dataset, _eapi.POLARS)

    def put_struct(self, struct_name: str, struct: _eapi.STRUCT_TYPE):

        _val.validate_signature(self.put_struct, struct_name, struct)

        self.__val.check_item_valid_identifier(struct_name, TracContextValidator.DATASET)
        self.__val.check_item_is_model_output(struct_name, TracContextValidator.DATASET)

        static_schema = self.__get_static_schema(self.__model_def, struct_name)
        data_view = self.__local_ctx.get(struct_name)
        part_key = _data.DataPartKey.for_root()

        if data_view is None:
            if static_schema is not None:
                data_view = _data.DataView.for_trac_schema(static_schema)
            else:
                data_view = _data.DataView.create_empty()

        self.__val.check_context_object_type(struct_name, data_view, _data.DataView)
        self.__val.check_context_data_view_type(struct_name, data_view, _meta.ObjectType.DATA)
        self.__val.check_dataset_schema_defined(struct_name, data_view)
        self.__val.check_dataset_part_not_present(struct_name, data_view, part_key)

        data_item = _data.DataItem.for_struct(struct)
        updated_view = _data.DataMapping.add_item_to_view(data_view, part_key, data_item)

        self.__local_ctx[struct_name] = updated_view

    def put_file(self, file_name: str, file_content: tp.Union[bytes, bytearray]):

        _val.validate_signature(self.put_file, file_name, file_content)

        self.__val.check_item_valid_identifier(file_name, TracContextValidator.FILE)
        self.__val.check_item_is_model_output(file_name, TracContextValidator.FILE)
        
        file_view: _data.DataView = self.__local_ctx.get(file_name)

        if file_view is None:
            file_view = _data.DataView.create_empty(_meta.ObjectType.FILE)

        self.__val.check_context_object_type(file_name, file_view, _data.DataView)
        self.__val.check_context_data_view_type(file_name, file_view, _meta.ObjectType.FILE)
        self.__val.check_file_content_not_present(file_name, file_view)

        if isinstance(file_content, bytearray):
            file_content = bytes(bytearray)

        file_item = _data.DataItem.for_file_content(file_content)
        self.__local_ctx[file_name] = file_view.with_file_item(file_item)

    def put_file_stream(self, file_name: str) -> tp.ContextManager[tp.BinaryIO]:

        _val.validate_signature(self.put_file_stream, file_name)

        self.__val.check_item_valid_identifier(file_name, TracContextValidator.FILE)
        self.__val.check_item_is_model_output(file_name, TracContextValidator.FILE)

        @contextlib.contextmanager
        def memory_stream(stream: io.BytesIO):
            try:
                yield stream
                buffer = stream.getbuffer().tobytes()
                self.put_file(file_name, buffer)
            finally:
                stream.close()

        return memory_stream(io.BytesIO())

    def log(self) -> logging.Logger:

        _val.validate_signature(self.log)

        return self.__model_log

    @staticmethod
    def __get_static_schema(model_def: _meta.ModelDefinition, dataset_name: str):

        input_schema = model_def.inputs.get(dataset_name)

        if input_schema is not None and not input_schema.dynamic:
            return input_schema.schema

        output_schema = model_def.outputs.get(dataset_name)

        if output_schema is not None and not output_schema.dynamic:
            return output_schema.schema

        return None

    @staticmethod
    def __assign_field_order(schema_def: _meta.SchemaDefinition):

        if schema_def is None or schema_def.table is None or schema_def.table.fields is None:
            return schema_def

        if all(map(lambda f: f.fieldOrder is None, schema_def.table.fields)):
            for index, field in enumerate(schema_def.table.fields):
                field.fieldOrder = index

        return schema_def


class TracDataContextImpl(TracContextImpl, _eapi.TracDataContext):

    def __init__(
            self, model_def: _meta.ModelDefinition, model_class: _api.TracModel.__class__,
            local_ctx: tp.Dict[str, tp.Any], dynamic_outputs: tp.List[str],
            storage_map: tp.Dict[str, tp.Union[_eapi.TracFileStorage, _eapi.TracDataStorage]],
            checkout_directory: pathlib.Path = None, log_provider: _logging.LogProvider = None):

        super().__init__(model_def, model_class, local_ctx, dynamic_outputs, checkout_directory, log_provider)

        self.__model_def = model_def
        self.__local_ctx = local_ctx
        self.__dynamic_outputs = dynamic_outputs
        self.__storage_map = storage_map
        self.__checkout_directory = checkout_directory

        self.__val: TracContextValidator = self._TracContextImpl__val  # noqa

    def get_file_storage(self, storage_key: str) -> _eapi.TracFileStorage:

        _val.validate_signature(self.get_file_storage, storage_key)

        self.__val.check_storage_valid_identifier(storage_key)
        self.__val.check_storage_available(self.__storage_map, storage_key)
        self.__val.check_storage_type(self.__storage_map, storage_key, _eapi.TracFileStorage)

        return self.__storage_map[storage_key]

    def get_data_storage(
            self, storage_key: str,
            framework: _eapi.DataFramework[_eapi.DATA_API],
            **framework_args) -> _eapi.TracDataStorage[_eapi.DATA_API]:

        _val.validate_signature(self.get_file_storage, storage_key)

        self.__val.check_storage_valid_identifier(storage_key)
        self.__val.check_storage_available(self.__storage_map, storage_key)
        self.__val.check_storage_type(self.__storage_map, storage_key, _eapi.TracDataStorage)
        self.__val.check_data_framework_args(framework, framework_args)

        storage = self.__storage_map[storage_key]
        converter = _data.DataConverter.for_framework(framework, **framework_args)

        # Create a shallow copy of the storage impl with a converter for the requested data framework
        # At some point we will need a storage factory class, bc the internal data API can also be different
        storage = copy.copy(storage)
        storage._TracDataStorageImpl__converter = converter

        return storage

    def add_data_import(self, dataset_name: str):

        _val.validate_signature(self.add_data_import, dataset_name)

        self.__val.check_item_valid_identifier(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_not_defined_in_model(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_not_available_in_context(dataset_name, TracContextValidator.DATASET)

        self.__local_ctx[dataset_name] = _data.DataView.create_empty()
        self.__dynamic_outputs.append(dataset_name)

    def set_source_metadata(self, dataset_name: str, storage_key: str, source_info: tp.Union[_eapi.FileStat, str]):

        _val.validate_signature(self.set_source_metadata, dataset_name, storage_key, source_info)

        self.__val.check_item_valid_identifier(dataset_name, TracContextValidator.DATASET)
        self.__val.check_item_available_in_context(dataset_name, TracContextValidator.DATASET)
        self.__val.check_storage_valid_identifier(storage_key)
        self.__val.check_storage_available(self.__storage_map, storage_key)

        storage = self.__storage_map[storage_key]

        if isinstance(storage, _eapi.TracFileStorage):
            if not isinstance(source_info, _eapi.FileStat):
                self.__val.report_public_error(_ex.ERuntimeValidation(f"Expected storage_info to be a FileStat, [{storage_key}] refers to file storage"))

        if isinstance(storage, _eapi.TracDataStorage):
            if not isinstance(source_info, str):
                self.__val.report_public_error(_ex.ERuntimeValidation(f"Expected storage_info to be a table name, [{storage_key}] refers to dadta storage"))

        pass  # Not implemented yet, only required when imports are sent back to the platform

    def set_attribute(self, dataset_name: str, attribute_name: str, value: tp.Any):

        _val.validate_signature(self.set_attribute, dataset_name, attribute_name, value)

        pass  # Not implemented yet, only required when imports are sent back to the platform

    def set_schema(self, dataset_name: str, schema: _meta.SchemaDefinition):

        _val.validate_signature(self.set_schema, dataset_name, schema)

        # Forward to existing method (these should be swapped round)
        self.put_schema(dataset_name, schema)


class TracFileStorageImpl(_eapi.TracFileStorage):

    def __init__(
            self, storage_key: str, storage_impl: _storage.IFileStorage,
            write_access: bool, checkout_directory, log_provider: _logging.LogProvider):

        self.__storage_key = storage_key

        self.__exists = lambda sp: storage_impl.exists(sp)
        self.__size = lambda sp: storage_impl.size(sp)
        self.__stat = lambda sp: storage_impl.stat(sp)
        self.__ls = lambda sp, rec: storage_impl.ls(sp, rec)
        self.__read_byte_stream = lambda sp: storage_impl.read_byte_stream(sp)

        if write_access:
            self.__mkdir = lambda sp, rec: storage_impl.mkdir(sp, rec)
            self.__rm = lambda sp: storage_impl.rm(sp)
            self.__rmdir = lambda sp: storage_impl.rmdir(sp)
            self.__write_byte_stream = lambda sp: storage_impl.write_byte_stream(sp)
        else:
            self.__mkdir = None
            self.__rm = None
            self.__rmdir = None
            self.__write_byte_stream = None

        # If no log provider is supplied, use the default (system logs only)
        if log_provider is None:
            log_provider = _logging.LogProvider()

        self.__log = log_provider.logger_for_object(self)
        self.__val = TracStorageValidator(self.__log, checkout_directory, self.__storage_key)

    def get_storage_key(self) -> str:

        _val.validate_signature(self.get_storage_key)

        return self.__storage_key

    def exists(self, storage_path: str) -> bool:

        _val.validate_signature(self.exists, storage_path)

        self.__val.check_operation_available(self.exists, self.__exists)
        self.__val.check_storage_path_is_valid(storage_path)

        return self.__exists(storage_path)

    def size(self, storage_path: str) -> int:

        _val.validate_signature(self.size, storage_path)

        self.__val.check_operation_available(self.size, self.__size)
        self.__val.check_storage_path_is_valid(storage_path)

        return self.__size(storage_path)

    def stat(self, storage_path: str) -> _eapi.FileStat:

        _val.validate_signature(self.stat, storage_path)

        self.__val.check_operation_available(self.stat, self.__stat)
        self.__val.check_storage_path_is_valid(storage_path)

        stat = self.__stat(storage_path)
        return _eapi.FileStat(**stat.__dict__)

    def ls(self, storage_path: str, recursive: bool = False) -> tp.List[_eapi.FileStat]:

        _val.validate_signature(self.ls, storage_path, recursive)

        self.__val.check_operation_available(self.ls, self.__ls)
        self.__val.check_storage_path_is_valid(storage_path)

        listing = self.__ls(storage_path, recursive)
        return list(_eapi.FileStat(**stat.__dict__) for stat in listing)

    def mkdir(self, storage_path: str, recursive: bool = False):

        _val.validate_signature(self.mkdir, storage_path, recursive)

        self.__val.check_operation_available(self.mkdir, self.__mkdir)
        self.__val.check_storage_path_is_valid(storage_path)
        self.__val.check_storage_path_is_not_root(storage_path)

        self.__mkdir(storage_path, recursive)

    def rm(self, storage_path: str):

        _val.validate_signature(self.rm, storage_path)

        self.__val.check_operation_available(self.rm, self.__rm)
        self.__val.check_storage_path_is_valid(storage_path)
        self.__val.check_storage_path_is_not_root(storage_path)

        self.__rm(storage_path)

    def rmdir(self, storage_path: str):

        _val.validate_signature(self.rmdir, storage_path)

        self.__val.check_operation_available(self.rmdir, self.__rmdir)
        self.__val.check_storage_path_is_valid(storage_path)
        self.__val.check_storage_path_is_not_root(storage_path)

        self.__rmdir(storage_path)

    def read_byte_stream(self, storage_path: str) -> tp.ContextManager[tp.BinaryIO]:

        _val.validate_signature(self.read_byte_stream, storage_path)

        self.__val.check_operation_available(self.read_byte_stream, self.__read_byte_stream)
        self.__val.check_storage_path_is_valid(storage_path)

        return self.__read_byte_stream(storage_path)

    def read_bytes(self, storage_path: str) -> bytes:

        _val.validate_signature(self.read_bytes, storage_path)

        self.__val.check_operation_available(self.read_bytes, self.__read_byte_stream)
        self.__val.check_storage_path_is_valid(storage_path)

        return super().read_bytes(storage_path)

    def write_byte_stream(self, storage_path: str) -> tp.ContextManager[tp.BinaryIO]:

        _val.validate_signature(self.write_byte_stream, storage_path)

        self.__val.check_operation_available(self.write_byte_stream, self.__write_byte_stream)
        self.__val.check_storage_path_is_valid(storage_path)
        self.__val.check_storage_path_is_not_root(storage_path)

        return self.__write_byte_stream(storage_path)

    def write_bytes(self, storage_path: str, data: bytes):

        _val.validate_signature(self.write_bytes, storage_path)

        self.__val.check_operation_available(self.write_bytes, self.__write_byte_stream)
        self.__val.check_storage_path_is_valid(storage_path)
        self.__val.check_storage_path_is_not_root(storage_path)

        super().write_bytes(storage_path, data)


class TracDataStorageImpl(_eapi.TracDataStorage[_eapi.DATA_API]):

    def __init__(
            self, storage_key: str, storage_impl: _storage.IDataStorageBase[_data.T_INTERNAL_DATA, _data.T_INTERNAL_SCHEMA],
            data_converter: _data.DataConverter[_eapi.DATA_API, _data.T_INTERNAL_DATA, _data.T_INTERNAL_SCHEMA],
            write_access: bool, checkout_directory, log_provider: _logging.LogProvider):

        self.__storage_key = storage_key
        self.__converter = data_converter

        self.__has_table = lambda tn: storage_impl.has_table(tn)
        self.__list_tables = lambda: storage_impl.list_tables()
        self.__read_table = lambda tn: storage_impl.read_table(tn)
        self.__native_read_query = lambda q, ps: storage_impl.native_read_query(q, **ps)

        if write_access:
            self.__create_table = lambda tn, s: storage_impl.create_table(tn, s)
            self.__write_table = lambda tn, ds: storage_impl.write_table(tn, ds)
        else:
            self.__create_table = None
            self.__write_table = None

        # If no log provider is supplied, use the default (system logs only)
        if log_provider is None:
            log_provider = _logging.LogProvider()

        self.__log = log_provider.logger_for_object(self)
        self.__val = TracStorageValidator(self.__log, checkout_directory, self.__storage_key)

    def has_table(self, table_name: str) -> bool:

        _val.validate_signature(self.has_table, table_name)

        self.__val.check_operation_available(self.has_table, self.__has_table)
        self.__val.check_table_name_is_valid(table_name)
        self.__val.check_storage_path_is_valid(table_name)

        try:
            return self.__has_table(table_name)
        except _ex.EStorageRequest as e:
            self.__val.report_public_error(e)

    def list_tables(self) -> tp.List[str]:

        _val.validate_signature(self.list_tables)

        self.__val.check_operation_available(self.list_tables, self.__list_tables)

        try:
            return self.__list_tables()
        except _ex.EStorageRequest as e:
            self.__val.report_public_error(e)

    def create_table(self, table_name: str, schema: _api.SchemaDefinition):

        _val.validate_signature(self.create_table, table_name, schema)

        self.__val.check_operation_available(self.create_table, self.__create_table)
        self.__val.check_table_name_is_valid(table_name)
        self.__val.check_storage_path_is_valid(table_name)

        arrow_schema = _data.DataMapping.trac_to_arrow_schema(schema)

        try:
            self.__create_table(table_name, arrow_schema)
        except _ex.EStorageRequest as e:
            self.__val.report_public_error(e)

    def read_table(self, table_name: str) -> _eapi.DATA_API:

        _val.validate_signature(self.read_table, table_name)

        self.__val.check_operation_available(self.read_table, self.__read_table)
        self.__val.check_table_name_is_valid(table_name)
        self.__val.check_table_name_not_reserved(table_name)

        try:
            raw_data = self.__read_table(table_name)
            return self.__converter.from_internal(raw_data)

        except _ex.EStorageRequest as e:
            self.__val.report_public_error(e)

    def native_read_query(self, query: str, **parameters) -> _eapi.DATA_API:

        _val.validate_signature(self.native_read_query, query, **parameters)

        self.__val.check_operation_available(self.native_read_query, self.__native_read_query)

        # TODO: validate query and parameters
        # Some validation is performed by the impl

        try:
            raw_data = self.__native_read_query(query, **parameters)
            return self.__converter.from_internal(raw_data)

        except _ex.EStorageRequest as e:
            self.__val.report_public_error(e)

    def write_table(self, table_name: str, dataset: _eapi.DATA_API):

        _val.validate_signature(self.write_table, table_name, dataset)

        self.__val.check_operation_available(self.read_table, self.__read_table)
        self.__val.check_table_name_is_valid(table_name)
        self.__val.check_table_name_not_reserved(table_name)
        self.__val.check_provided_dataset_type(dataset, self.__converter.framework.api_type)

        try:
            raw_data = self.__converter.to_internal(dataset)
            self.__write_table(table_name, raw_data)

        except _ex.EStorageRequest as e:
            self.__val.report_public_error(e)


class TracContextErrorReporter:

    _VALID_IDENTIFIER = re.compile("^[a-zA-Z_]\\w*$",)
    _RESERVED_IDENTIFIER = re.compile("^(trac_|_)\\w*")

    def __init__(self, log: logging.Logger, checkout_directory: pathlib.Path):

        self.__log = log
        self.__checkout_directory = checkout_directory

    def report_public_error(self, exception: Exception):

        self._report_error(str(exception), exception)

    def _report_error(self, message, cause: Exception = None):

        full_stack = traceback.extract_stack()
        model_stack = _util.filter_model_stack_trace(full_stack, self.__checkout_directory)
        model_stack_str = ''.join(traceback.format_list(list(reversed(model_stack))))
        details = _util.error_details_from_trace(model_stack)
        message = f"{message} {details}"

        self.__log.error(message)
        self.__log.error(f"Model stack trace:\n{model_stack_str}")

        if cause:
            raise _ex.ERuntimeValidation(message) from cause
        else:
            raise _ex.ERuntimeValidation(message)

    @staticmethod
    def _type_name(type_: type):

        module = type_.__module__

        if module is None or module == str.__class__.__module__ or module == tp.__name__:
            return _val.type_name(type_, False)
        else:
            return _val.type_name(type_, True)


class TracContextValidator(TracContextErrorReporter):

    PARAMETER = "Parameter"
    DATASET = "Dataset"
    FILE = "File"

    def __init__(
            self, log: logging.Logger,
            model_def: _meta.ModelDefinition,
            local_ctx: tp.Dict[str, tp.Any],
            dynamic_outputs: tp.List[str],
            checkout_directory: pathlib.Path):

        super().__init__(log, checkout_directory)

        self.__model_def = model_def
        self.__local_ctx = local_ctx
        self.__dynamic_outputs = dynamic_outputs

    def check_item_valid_identifier(self, item_name: str, item_type: str):

        if item_name is None:
            self._report_error(f"{item_type} name is null")

        if not self._VALID_IDENTIFIER.match(item_name):
            self._report_error(f"{item_type} name {item_name} is not a valid identifier")

    def check_item_defined_in_model(self, item_name: str, item_type: str):

        if item_type == self.PARAMETER:
            if item_name not in self.__model_def.parameters:
                self._report_error(f"{item_type} {item_name} is not defined in the model")
        else:
            if item_name not in self.__model_def.inputs and item_name not in self.__model_def.outputs:
                self._report_error(f"{item_type} {item_name} is not defined in the model")

    def check_item_not_defined_in_model(self, item_name: str, item_type: str):

        if item_name  in self.__model_def.inputs or item_name in self.__model_def.outputs:
            self._report_error(f"{item_type} {item_name} is already defined in the model")

        if item_name  in self.__model_def.parameters:
            self._report_error(f"{item_name} name {item_name} is already in use as a model parameter")

    def check_item_is_model_output(self, item_name: str, item_type: str):

        if item_name not in self.__model_def.outputs and item_name not in self.__dynamic_outputs:
            self._report_error(f"{item_type} {item_name} is not defined as a model output")

    def check_item_available_in_context(self, item_name: str, item_type: str):

        if item_name not in self.__local_ctx:
            self._report_error(f"{item_type} {item_name} is not available in the current context")

    def check_item_not_available_in_context(self, item_name: str, item_type: str):

        if item_name in self.__local_ctx:
            self._report_error(f"{item_type} {item_name} already exists in the current context")

    def check_dataset_is_dynamic_output(self, dataset_name: str):

        model_output: _meta.ModelOutputSchema = self.__model_def.outputs.get(dataset_name)
        dynamic_output = dataset_name in self.__dynamic_outputs

        if model_output is None and not dynamic_output:
            self._report_error(f"Dataset {dataset_name} is not defined as a model output")

        if model_output and not model_output.dynamic:
            self._report_error(f"Model output {dataset_name} is not a dynamic output")

    def check_dataset_schema_defined(self, dataset_name: str, data_view: _data.DataView):

        schema = data_view.trac_schema if data_view is not None else None

        if schema is None:
            self._report_error(f"Schema not defined for dataset {dataset_name} in the current context")

        if schema.schemaType == _meta.SchemaType.TABLE and (schema.table is None or not schema.table.fields):
            self._report_error(f"Schema not defined for dataset {dataset_name} in the current context")

        if schema.schemaType == _meta.SchemaType.STRUCT and (schema.struct is None or not schema.struct.fields):
            self._report_error(f"Schema not defined for dataset {dataset_name} in the current context")

    def check_dataset_schema_not_defined(self, dataset_name: str, data_view: _data.DataView):

        schema = data_view.trac_schema if data_view is not None else None

        if schema is not None and (schema.table or schema.schemaType != _meta.SchemaType.SCHEMA_TYPE_NOT_SET):
            self._report_error(f"Schema already defined for dataset {dataset_name} in the current context")

    def check_dataset_part_present(self, dataset_name: str, data_view: _data.DataView, part_key: _data.DataPartKey):

        part = data_view.parts.get(part_key) if data_view.parts is not None else None

        if part is None or len(part) == 0:
            self._report_error(f"No data present for {dataset_name} ({part_key}) in the current context")

    def check_dataset_part_not_present(self, dataset_name: str, data_view: _data.DataView, part_key: _data.DataPartKey):

        part = data_view.parts.get(part_key) if data_view.parts is not None else None

        if part is not None and len(part) > 0:
            self._report_error(f"Data already present for {dataset_name} ({part_key}) in the current context")

    def check_dataset_is_empty(self, dataset_name: str, data_view: _data.DataView):

        if not data_view.is_empty():
            self._report_error(f"Dataset {dataset_name} is not empty")

    def check_provided_schema_is_valid(self, dataset_name: str, schema: _meta.SchemaDefinition):

        if schema is None:
            self._report_error(f"The schema provided for [{dataset_name}] is null")

        if not isinstance(schema, _meta.SchemaDefinition):
            schema_type_name = self._type_name(type(schema))
            self._report_error(f"The object provided for [{dataset_name}] is not a schema (got {schema_type_name})")

        try:
            _val.StaticValidator.quick_validate_schema(schema)
        except _ex.EModelValidation as e:
            self._report_error(f"The schema provided for [{dataset_name}] failed validation: {str(e)}", e)

    def check_provided_dataset_type(self, dataset: tp.Any, expected_type: type):

        if dataset is None:
            self._report_error(f"Provided dataset is null")

        if not isinstance(dataset, expected_type):

            expected_type_name = self._type_name(expected_type)
            actual_type_name = self._type_name(type(dataset))

            self._report_error(
                f"Provided dataset is the wrong type" +
                f" (expected {expected_type_name}, got {actual_type_name})")

    def check_context_object_type(self, item_name: str, item: tp.Any, expected_type: type):

        if not isinstance(item, expected_type):

            expected_type_name = self._type_name(expected_type)
            actual_type_name = self._type_name(type(item))

            self._report_error(
                f"The object referenced by [{item_name}] in the current context has the wrong type" +
                f" (expected {expected_type_name}, got {actual_type_name})")

    def check_context_data_view_type(self, item_name: str, data_vew: _data.DataView, expected_type: _meta.ObjectType):

        if data_vew.object_type != expected_type:

            self._report_error(
                f"The object referenced by [{item_name}] in the current context has the wrong type" +
                f" (expected {expected_type.name}, got {data_vew.object_type.name})")

    def check_data_framework_args(self, framework: _eapi.DataFramework, framework_args: tp.Dict[str, tp.Any]):

        expected_args = _data.DataConverter.get_framework_args(framework)
        unexpected_args = list(filter(lambda arg: arg not in expected_args, framework_args.keys()))

        if any(unexpected_args):
            unknown_args = ", ".join(unexpected_args)
            self._report_error(f"Using [{framework}], some arguments were not recognized: [{unknown_args}]")

        for arg_name, arg_type in expected_args.items():

            arg_value = framework_args.get(arg_name)

            if _val.check_type(arg_type, arg_value):
                continue

            if arg_value is None:
                self._report_error(f"Using [{framework}], required argument [{arg_name}] is missing")

            else:
                expected_type_name = self._type_name(arg_type)
                actual_type_name = self._type_name(type(arg_value))

                self._report_error(
                    f"Using [{framework}], argument [{arg_name}] has the wrong type" +
                    f" (expected {expected_type_name}, got {actual_type_name})")

    def check_file_content_present(self, file_name: str, file_view: _data.DataView):

        if file_view.file_item is None or file_view.file_item.content is None:
            self._report_error(f"File content is missing or empty for [{file_name}] in the current context")

    def check_file_content_not_present(self, file_name: str, file_view: _data.DataView):

        if file_view.file_item is not None and file_view.file_item.content is not None:
            self._report_error(f"File content is already present for [{file_name}] in the current context")

    def check_storage_valid_identifier(self, storage_key):

        if storage_key is None:
            self._report_error(f"Storage key is null")

        if not self._VALID_IDENTIFIER.match(storage_key):
            self._report_error(f"Storage key {storage_key} is not a valid identifier")

    def check_storage_available(self, storage_map: tp.Dict, storage_key: str):

        storage_instance = storage_map.get(storage_key)

        if storage_instance is None:
            self._report_error(f"Storage not available for storage key [{storage_key}]")

    def check_storage_type(
            self, storage_map: tp.Dict, storage_key: str,
            storage_type: tp.Union[_eapi.TracFileStorage.__class__, _eapi.TracDataStorage.__class__]):

        storage_instance = storage_map.get(storage_key)

        if not isinstance(storage_instance, storage_type):
            if storage_type == _eapi.TracFileStorage:
                self._report_error(f"Storage key [{storage_key}] refers to data storage, not file storage")
            else:
                self._report_error(f"Storage key [{storage_key}] refers to file storage, not data storage")


class TracStorageValidator(TracContextErrorReporter):

    def __init__(self, log, checkout_directory, storage_key):
        super().__init__(log, checkout_directory)
        self.__storage_key = storage_key

    def check_operation_available(self, public_func: tp.Callable, impl_func: tp.Callable):

        if impl_func is None:
            self._report_error(f"Operation [{public_func.__name__}] is not available for storage [{self.__storage_key}]")

    def check_storage_path_is_valid(self, storage_path: str):

        if _val.StorageValidator.storage_path_is_empty(storage_path):
            self._report_error(f"Storage path is None or empty")

        if _val.StorageValidator.storage_path_invalid(storage_path):
            self._report_error(f"Storage path [{storage_path}] contains invalid characters")

        if _val.StorageValidator.storage_path_not_relative(storage_path):
            self._report_error(f"Storage path [{storage_path}] is not a relative path")

        if _val.StorageValidator.storage_path_outside_root(storage_path):
            self._report_error(f"Storage path [{storage_path}] is outside the storage root")

    def check_storage_path_is_not_root(self, storage_path: str):

        if _val.StorageValidator.storage_path_is_empty(storage_path):
            self._report_error(f"Storage path [{storage_path}] is not allowed")

    def check_table_name_is_valid(self, table_name: str):

        if table_name is None:
            self._report_error(f"Table name is null")

        if not self._VALID_IDENTIFIER.match(table_name):
            self._report_error(f"Table name {table_name} is not a valid identifier")

    def check_table_name_not_reserved(self, table_name: str):

        if self._RESERVED_IDENTIFIER.match(table_name):
            self._report_error(f"Table name {table_name} is a reserved identifier")

    def check_provided_dataset_type(self, dataset: tp.Any, expected_type: type):

        if dataset is None:
            self._report_error(f"Provided dataset is null")

        if not isinstance(dataset, expected_type):

            expected_type_name = self._type_name(expected_type)
            actual_type_name = self._type_name(type(dataset))

            self._report_error(
                f"Provided dataset is the wrong type" +
                f" (expected {expected_type_name}, got {actual_type_name})")
