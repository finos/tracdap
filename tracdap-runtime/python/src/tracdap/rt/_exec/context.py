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

import logging
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
    TracContextImpl is the main implementation of the API class TracContext (from trac.rt.api).
    It provides get/put operations the inputs, outputs and parameters of a model according to the model definition,
    as well as exposing other information needed by the model at runtime and offering a few utility functions.

    An instance of TracContextImpl is constructed by the runtime engine for each model node in the execution graph.
    Parameters and schemas will be pre-populated from the job definition. Normally, input data will be created by parent
    nodes in the execution graph and passed in when the context is constructed, so they are available when required. In
    the simplest case, a get call picks a dataset from the map and a put call puts a dataset into the map. Under this
    mechanism, no outputs are passed downstream until the model finishes executing.

    Optimizations for lazy loading and eager saving require the context to call back into the runtime engine. For lazy
    load, the graph node to prepare an input is injected when the data is requested and the model thread blocks until
    it is available; for eager save child nodes of individual outputs are triggered when those outputs are produced.
    In both cases this complexity is hidden from the model, which only sees one thread with synchronous get/put calls.

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
                 local_ctx: tp.Dict[str, _data.DataView],
                 schemas: tp.Dict[str, _meta.SchemaDefinition]):

        self.__ctx_log = _util.logger_for_object(self)
        self.__model_log = _util.logger_for_class(model_class)

        self.__model_def = model_def
        self.__model_class = model_class

        self.__parameters = local_ctx or {}
        self.__data = local_ctx or {}
        self.__schemas = schemas

        self.__val = TracContextValidator(
            self.__ctx_log,
            self.__parameters,
            self.__data)

    def get_parameter(self, parameter_name: str) -> tp.Any:

        _val.validate_signature(self.get_parameter, parameter_name)

        self.__val.check_param_not_null(parameter_name)
        self.__val.check_param_valid_identifier(parameter_name)
        self.__val.check_param_exists(parameter_name)

        value: _meta.Value = self.__parameters[parameter_name]  # noqa

        return _types.MetadataCodec.decode_value(value)

    def get_schema(self, dataset_name: str) -> _meta.SchemaDefinition:

        _val.validate_signature(self.get_schema, dataset_name)

        self.__val.check_dataset_name_not_null(dataset_name)
        self.__val.check_dataset_valid_identifier(dataset_name)

        # There is no need to look in the data map if the model has defined a static schema
        if dataset_name in self.__schemas:
            return self.__schemas[dataset_name]

        self.__val.check_context_item_exists(dataset_name)
        self.__val.check_context_item_is_dataset(dataset_name)
        self.__val.check_dataset_schema_defined(dataset_name)

        data_view = self.__data[dataset_name]

        return data_view.trac_schema

    def get_pandas_table(self, dataset_name: str, use_temporal_objects: tp.Optional[bool] = None) -> pd.DataFrame:

        _val.validate_signature(self.get_pandas_table, dataset_name, use_temporal_objects)

        part_key = _data.DataPartKey.for_root()

        self.__val.check_dataset_name_not_null(dataset_name)
        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_context_item_exists(dataset_name)
        self.__val.check_context_item_is_dataset(dataset_name)
        self.__val.check_dataset_schema_defined(dataset_name)
        self.__val.check_dataset_part_present(dataset_name, part_key)

        data_view = self.__data[dataset_name]

        # If the model defines a static input schema, use that for schema conformance
        # Otherwise, take what is in the incoming dataset (schema is dynamic)
        if dataset_name in self.__schemas:
            schema = _data.DataMapping.trac_to_arrow_schema(self.__schemas[dataset_name])
        else:
            schema = data_view.arrow_schema

        if use_temporal_objects is None:
            use_temporal_objects = self.__DEFAULT_TEMPORAL_OBJECTS

        return _data.DataMapping.view_to_pandas(data_view, part_key, schema, use_temporal_objects)

    def put_pandas_table(self, dataset_name: str, dataset: pd.DataFrame):

        _val.validate_signature(self.put_pandas_table, dataset_name, dataset)

        part_key = _data.DataPartKey.for_root()

        self.__val.check_dataset_name_not_null(dataset_name)
        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_context_item_exists(dataset_name)
        self.__val.check_context_item_is_dataset(dataset_name)
        self.__val.check_dataset_schema_defined(dataset_name)
        self.__val.check_dataset_part_not_present(dataset_name, part_key)
        self.__val.check_provided_dataset_not_null(dataset)
        self.__val.check_provided_dataset_type(dataset, pd.DataFrame)

        prior_view = self.__data[dataset_name]

        # If the model defines a static output schema, use that for schema conformance
        # Otherwise, use the schema in the data view for this output (this could be a dynamic schema)
        if dataset_name in self.__schemas:
            schema = _data.DataMapping.trac_to_arrow_schema(self.__schemas[dataset_name])
        else:
            schema = prior_view.arrow_schema

        data_item = _data.DataMapping.pandas_to_item(dataset, schema)
        data_view = _data.DataMapping.add_item_to_view(prior_view, part_key, data_item)

        self.__data[dataset_name] = data_view

    def log(self) -> logging.Logger:

        _val.validate_signature(self.log)

        return self.__model_log


class TracContextValidator:

    __VALID_IDENTIFIER = re.compile("^[a-zA-Z_]\\w*$",)
    __RESERVED_IDENTIFIER = re.compile("^(trac_|_)\\w*")

    __LAST_MODEL_FRAME = -4
    __FIRST_MODEL_FRAME_NAME = "run_model"
    __FIRST_MODEL_FRAME_TEST_NAME = "_callTestMethod"

    def __init__(
            self, log: logging.Logger,
            parameters: tp.Dict[str, tp.Any],
            data_ctx: tp.Dict[str, _data.DataView]):

        self.__log = log
        self.__parameters = parameters
        self.__data_ctx = data_ctx

    def _report_error(self, message):

        model_stack = self._build_model_stack_trace()
        model_stack_str = ''.join(traceback.format_list(list(reversed(model_stack))))

        self.__log.error(message)
        self.__log.error(f"Model stack trace:\n{model_stack_str}")

        raise _ex.ERuntimeValidation(message)

    def _build_model_stack_trace(self):

        full_stack = traceback.extract_stack()

        frame_names = list(map(lambda frame: frame.name, full_stack))

        if self.__FIRST_MODEL_FRAME_NAME in frame_names:
            first_model_frame = frame_names.index(self.__FIRST_MODEL_FRAME_NAME)
        elif self.__FIRST_MODEL_FRAME_TEST_NAME in frame_names:
            first_model_frame = frame_names.index(self.__FIRST_MODEL_FRAME_TEST_NAME)
        else:
            first_model_frame = 0

        return full_stack[first_model_frame:self.__LAST_MODEL_FRAME]

    def check_param_not_null(self, param_name):

        if param_name is None:
            self._report_error(f"Parameter name is null")

    def check_param_valid_identifier(self, param_name: str):

        if not self.__VALID_IDENTIFIER.match(param_name):
            self._report_error(f"Parameter name {param_name} is not a valid identifier")

    def check_param_exists(self, param_name: str):

        if param_name not in self.__parameters:
            self._report_error(f"Parameter {param_name} is not defined in the current context")

    def check_dataset_name_not_null(self, dataset_name):

        if dataset_name is None:
            self._report_error(f"Dataset name is null")

    def check_dataset_valid_identifier(self, dataset_name: str):

        if not self.__VALID_IDENTIFIER.match(dataset_name):
            self._report_error(f"Dataset name {dataset_name} is not a valid identifier")

    def check_context_item_exists(self, item_name: str):

        if item_name not in self.__data_ctx:
            self._report_error(f"The identifier {item_name} is not defined in the current context")

    def check_context_item_is_dataset(self, item_name: str):

        ctx_item = self.__data_ctx[item_name]

        if not isinstance(ctx_item, _data.DataView):
            self._report_error(f"The object referenced by {item_name} is not a dataset in the current context")

    def check_dataset_schema_defined(self, dataset_name: str):

        schema = self.__data_ctx[dataset_name].trac_schema

        if schema is None or not schema.table or not schema.table.fields:
            self._report_error(f"Schema not defined for dataset {dataset_name} in the current context")

    def check_dataset_schema_not_defined(self, dataset_name: str):

        schema = self.__data_ctx[dataset_name].trac_schema

        if schema is not None and (schema.table or schema.schemaType != _meta.SchemaType.SCHEMA_TYPE_NOT_SET):
            self._report_error(f"Schema already defined for dataset {dataset_name} in the current context")

    def check_dataset_part_present(self, dataset_name: str, part_key: _data.DataPartKey):

        part = self.__data_ctx[dataset_name].parts.get(part_key)

        if part is None or len(part) == 0:
            self._report_error(f"No data present for dataset {dataset_name} ({part_key}) in the current context")

    def check_dataset_part_not_present(self, dataset_name: str, part_key: _data.DataPartKey):

        part = self.__data_ctx[dataset_name].parts.get(part_key)

        if part is not None and len(part) > 0:
            self._report_error(f"Data already present for dataset {dataset_name} ({part_key}) in the current context")

    def check_provided_dataset_not_null(self, dataset):

        if dataset is None:
            self._report_error(f"Provided dataset is null")

    def check_provided_dataset_type(self, dataset: tp.Any, expected_type: type):

        if not isinstance(dataset, expected_type):

            expected_type_name = self._type_name(expected_type)
            actual_type_name = self._type_name(type(dataset))

            self._report_error(
                f"Provided dataset is the wrong type" +
                f" (expected {expected_type_name}, got {actual_type_name})")

    @staticmethod
    def _type_name(type_: type):

        module = type_.__module__

        if module is None or module == str.__class__.__module__:
            return type_.__qualname__

        return module + '.' + type_.__name__
