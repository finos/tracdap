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

from __future__ import annotations

import logging
import typing as tp
import copy
import re

import pandas as pd
import pyspark as pys
from pyspark import sql as pyss

import trac.rt.api as api
import trac.rt.metadata as meta
import trac.rt.impl.util as util
import trac.rt.impl.data as _data


# TODO: Exception hierarchy
class ModelRuntimeException(RuntimeError):

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class ModelContext(api.TracContext):

    def __init__(self,
                 model_def: meta.ModelDefinition,
                 model_class: api.TracModel.__class__,
                 parameters: tp.Dict[str, tp.Any],
                 data: tp.Dict[str, _data.DataView]):

        self.__ctx_log = util.logger_for_object(self)

        self.__model_def = model_def
        self.__model_class = model_class
        self.__model_log = util.logger_for_class(self.__model_class)

        self.__parameters = parameters or {}
        self.__data = data or {}

        self.__val = ModelRuntimeValidator(
            self.__ctx_log,
            self.__parameters,
            self.__data)

    def get_parameter(self, parameter_name: str) -> tp.Any:

        self.__val.check_param_valid_identifier(parameter_name)
        self.__val.check_param_exists(parameter_name)

        return self.__parameters[parameter_name]

    def get_table_schema(self, dataset_name: str) -> api.TableDefinition:

        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_context_item_exists(dataset_name)
        self.__val.check_context_item_is_dataset(dataset_name)

        data_view = self.__data[dataset_name]

        return data_view.schema

    def get_pandas_table(self, dataset_name: str) -> pd.DataFrame:

        part_key = _data.DataPartKey.for_root()

        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_context_item_exists(dataset_name)
        self.__val.check_context_item_is_dataset(dataset_name)
        self.__val.check_dataset_part_present(dataset_name, part_key)

        data_view = self.__data[dataset_name]
        deltas = data_view.parts[part_key]
        data_item = deltas[0]

        if data_item.pandas is not None:
            return data_item.pandas
        else:
            raise NotImplementedError("Spark / Pandas conversion not implemented yet")

    def get_spark_table(self, dataset_name: str) -> pyss.DataFrame:
        raise NotImplementedError()

    def get_spark_table_rdd(self, dataset_name: str) -> pys.RDD:
        raise NotImplementedError()

    def put_table_schema(self, dataset_name: str, schema: api.TableDefinition):
        raise NotImplementedError()

    def put_pandas_table(self, dataset_name: str, dataset: pd.DataFrame):

        part_key = _data.DataPartKey.for_root()

        self.__val.check_dataset_valid_identifier(dataset_name)
        self.__val.check_context_item_exists(dataset_name)
        self.__val.check_context_item_is_dataset(dataset_name)
        self.__val.check_dataset_schema_defined(dataset_name)
        self.__val.check_dataset_part_not_present(dataset_name, part_key)

        data_view = self.__data[dataset_name]
        data_item = _data.DataItem(pandas=dataset, column_filter=None)

        new_data_parts = copy.copy(data_view.parts)
        new_data_parts[part_key] = [data_item]  # List of a single delta
        new_data_view = _data.DataView(data_view.schema, new_data_parts)

        self.__data[dataset_name] = new_data_view

    def put_spark_table(self, dataset_name: str, dataset: pyss.DataFrame):
        raise NotImplementedError()

    def put_spark_table_rdd(self, dataset_name: str, dataset: pys.RDD):
        raise NotImplementedError()

    def get_spark_context(self) -> pys.SparkContext:
        raise NotImplementedError()

    def get_spark_sql_context(self) -> pyss.SQLContext:
        raise NotImplementedError()

    def log(self) -> logging.Logger:
        return self.__model_log


class ModelRuntimeValidator:

    __VALID_IDENTIFIER = re.compile("^[a-zA-Z_]\\w*$",)
    __RESERVED_IDENTIFIER = re.compile("^(trac_|_)\\w*")

    def __init__(
            self, log: logging.Logger,
            parameters: tp.Dict[str, tp.Any],
            data_ctx: tp.Dict[str, _data.DataView]):

        self.__log = log
        self.__parameters = parameters
        self.__data_ctx = data_ctx

    def _report_error(self, message):
        self.__log.error(message)
        raise ModelRuntimeException(message)

    def check_param_valid_identifier(self, param_name: str):

        if not self.__VALID_IDENTIFIER.match(param_name):
            self._report_error(f"Parameter name {param_name} is not a valid identifier")

    def check_param_exists(self, param_name: str):

        if param_name not in self.__parameters:
            self._report_error(f"Parameter {param_name} is not defined in the current context")

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

        schema = self.__data_ctx[dataset_name].schema

        if schema is None or not schema.field:
            self._report_error(f"Schema not defined for dataset {dataset_name} in the current context")

    def check_dataset_schema_not_defined(self, dataset_name: str):

        schema = self.__data_ctx[dataset_name].schema

        if schema is not None and schema.field:
            self._report_error(f"Schema already defined for dataset {dataset_name} in the current context")

    def check_dataset_part_present(self, dataset_name: str, part_key: _data.DataPartKey):

        part = self.__data_ctx[dataset_name].parts.get(part_key)

        if part is None or len(part) == 0:
            self._report_error(f"No data present for dataset {dataset_name} ({part_key}) in the current context")

    def check_dataset_part_not_present(self, dataset_name: str, part_key: _data.DataPartKey):

        part = self.__data_ctx[dataset_name].parts.get(part_key)

        if part is not None and len(part) > 0:
            self._report_error(f"Data already present for dataset {dataset_name} ({part_key}) in the current context")
