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

import copy
import logging
import pathlib
import typing as tp
import re
import traceback

import pandas as pd

import tracdap.rt.api as _api
import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.type_system as _types  # noqa
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.util as _util  # noqa
import tracdap.rt._impl.validation as _val  # noqa


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

    __DEFAULT_TEMPORAL_OBJECTS = False

    def __init__(self,
                 model_def: _meta.ModelDefinition,
                 model_class: _api.TracModel.__class__,
                 local_ctx: tp.Dict[str, tp.Any],
                 checkout_directory: pathlib.Path = None):

        self.__ctx_log = _util.logger_for_object(self)
        self.__model_log = _util.logger_for_class(model_class)

        self.__model_def = model_def
        self.__model_class = model_class
        self.__local_ctx = local_ctx or {}

        self.__val = TracContextValidator(
            self.__ctx_log,
            self.__model_def,
            self.__local_ctx,
            checkout_directory)

    def get_parameter(self, parameter_name: str) -> tp.Any:

        _val.validate_signature(self.get_parameter, parameter_name)

        self.__val.check_param_valid_identifier(parameter_name)
        self.__val.check_param_defined_in_model(parameter_name)
        self.__val.check_param_available_in_context(parameter_name)

        value: _meta.Value = self.__local_ctx.get(parameter_name)

        self.__val.check_context_object_type(parameter_name, value, _meta.Value)

        return _types.MetadataCodec.decode_value(value)

    def has_dataset(self, dataset_name: str) -> bool:

        _val.validate_signature(self.has_dataset, dataset_name)

        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_dataset_defined_in_model(dataset_name)

        data_view: _data.DataView = self.__local_ctx.get(dataset_name)

        if data_view is None:
            return False

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)

        return not data_view.is_empty()

    def get_schema(self, dataset_name: str) -> _meta.SchemaDefinition:

        _val.validate_signature(self.get_schema, dataset_name)

        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_dataset_defined_in_model(dataset_name)
        self.__val.check_dataset_available_in_context(dataset_name)

        static_schema = self.__get_static_schema(self.__model_def, dataset_name)
        data_view: _data.DataView = self.__local_ctx.get(dataset_name)

        # Check the data view has a well-defined schema even if a static schema exists in the model
        # This ensures errors are always reported and is consistent with get_pandas_table()

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_dataset_schema_defined(dataset_name, data_view)

        # If a static schema exists, that takes priority
        # Return deep copies, do not allow model code to change schemas provided by the engine

        if static_schema is not None:
            return copy.deepcopy(static_schema)
        else:
            return copy.deepcopy(data_view.trac_schema)

    def get_pandas_table(self, dataset_name: str, use_temporal_objects: tp.Optional[bool] = None) -> pd.DataFrame:

        _val.validate_signature(self.get_pandas_table, dataset_name, use_temporal_objects)

        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_dataset_defined_in_model(dataset_name)
        self.__val.check_dataset_available_in_context(dataset_name)

        static_schema = self.__get_static_schema(self.__model_def, dataset_name)
        data_view = self.__local_ctx.get(dataset_name)
        part_key = _data.DataPartKey.for_root()

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_dataset_schema_defined(dataset_name, data_view)
        self.__val.check_dataset_part_present(dataset_name, data_view, part_key)

        # If the model defines a static input schema, use that for schema conformance
        # Otherwise, take what is in the incoming dataset (schema is dynamic)

        if static_schema is not None:
            schema = _data.DataMapping.trac_to_arrow_schema(static_schema)
        else:
            schema = data_view.arrow_schema

        if use_temporal_objects is None:
            use_temporal_objects = self.__DEFAULT_TEMPORAL_OBJECTS

        return _data.DataMapping.view_to_pandas(data_view, part_key, schema, use_temporal_objects)

    def put_schema(self, dataset_name: str, schema: _meta.SchemaDefinition):

        _val.validate_signature(self.get_schema, dataset_name, schema)

        # Copy the schema - schema cannot be changed in model code after put_schema
        # If field ordering is not assigned by the model, assign it here (model code will not see the numbers)
        schema_copy = self.__assign_field_order(copy.deepcopy(schema))

        self.__val.check_dataset_valid_identifier(dataset_name)
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
        self.__val.check_dataset_schema_not_defined(dataset_name, data_view)
        self.__val.check_dataset_is_empty(dataset_name, data_view)

        updated_view = data_view.with_trac_schema(schema_copy)

        self.__local_ctx[dataset_name] = updated_view

    def put_pandas_table(self, dataset_name: str, dataset: pd.DataFrame):

        _val.validate_signature(self.put_pandas_table, dataset_name, dataset)

        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_dataset_is_model_output(dataset_name)
        self.__val.check_provided_dataset_type(dataset, pd.DataFrame)

        static_schema = self.__get_static_schema(self.__model_def, dataset_name)
        data_view = self.__local_ctx.get(dataset_name)
        part_key = _data.DataPartKey.for_root()

        if data_view is None:
            if static_schema is not None:
                data_view = _data.DataView.for_trac_schema(static_schema)
            else:
                data_view = _data.DataView.create_empty()

        self.__val.check_context_object_type(dataset_name, data_view, _data.DataView)
        self.__val.check_dataset_schema_defined(dataset_name, data_view)
        self.__val.check_dataset_part_not_present(dataset_name, data_view, part_key)

        # Prefer static schemas for data conformance

        if static_schema is not None:
            schema = _data.DataMapping.trac_to_arrow_schema(static_schema)
        else:
            schema = data_view.arrow_schema

        # Data conformance is applied inside these conversion functions

        updated_item = _data.DataMapping.pandas_to_item(dataset, schema)
        updated_view = _data.DataMapping.add_item_to_view(data_view, part_key, updated_item)

        self.__local_ctx[dataset_name] = updated_view

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


class TracContextValidator:

    __VALID_IDENTIFIER = re.compile("^[a-zA-Z_]\\w*$",)
    __RESERVED_IDENTIFIER = re.compile("^(trac_|_)\\w*")

    def __init__(
            self, log: logging.Logger,
            model_def: _meta.ModelDefinition,
            local_ctx: tp.Dict[str, tp.Any],
            checkout_directory: pathlib.Path):

        self.__log = log
        self.__model_def = model_def
        self.__local_ctx = local_ctx
        self.__checkout_directory = checkout_directory

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

    def check_param_valid_identifier(self, param_name: str):

        if param_name is None:
            self._report_error(f"Parameter name is null")

        if not self.__VALID_IDENTIFIER.match(param_name):
            self._report_error(f"Parameter name {param_name} is not a valid identifier")

    def check_param_defined_in_model(self, param_name: str):

        if param_name not in self.__model_def.parameters:
            self._report_error(f"Parameter {param_name} is not defined in the model")

    def check_param_available_in_context(self, param_name: str):

        if param_name not in self.__local_ctx:
            self._report_error(f"Parameter {param_name} is not available in the current context")

    def check_dataset_valid_identifier(self, dataset_name: str):

        if dataset_name is None:
            self._report_error(f"Dataset name is null")

        if not self.__VALID_IDENTIFIER.match(dataset_name):
            self._report_error(f"Dataset name {dataset_name} is not a valid identifier")

    def check_dataset_defined_in_model(self, dataset_name: str):

        if dataset_name not in self.__model_def.inputs and dataset_name not in self.__model_def.outputs:
            self._report_error(f"Dataset {dataset_name} is not defined in the model")

    def check_dataset_is_model_output(self, dataset_name: str):

        if dataset_name not in self.__model_def.outputs:
            self._report_error(f"Dataset {dataset_name} is not defined as a model output")

    def check_dataset_is_dynamic_output(self, dataset_name: str):

        model_output: _meta.ModelOutputSchema = self.__model_def.outputs.get(dataset_name)

        if model_output is None:
            self._report_error(f"Dataset {dataset_name} is not defined as a model output")

        if not model_output.dynamic:
            self._report_error(f"Model output {dataset_name} is not a dynamic output")

    def check_dataset_available_in_context(self, item_name: str):

        if item_name not in self.__local_ctx:
            self._report_error(f"Dataset {item_name} is not available in the current context")

    def check_dataset_schema_defined(self, dataset_name: str, data_view: _data.DataView):

        schema = data_view.trac_schema if data_view is not None else None

        if schema is None or schema.table is None or not schema.table.fields:
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

    @staticmethod
    def _type_name(type_: type):

        module = type_.__module__

        if module is None or module == str.__class__.__module__:
            return type_.__qualname__

        return module + '.' + type_.__name__
