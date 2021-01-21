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
import re

import pandas as pd
import pyspark as pys
from pyspark import sql as pyss

import trac.rt.api as api
import trac.rt.metadata as meta
import trac.rt.impl.util as util


# TODO: Exception hierarchy
class ModelRuntimeException(RuntimeError):

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class ModelContext(api.TracContext):

    def __init__(self,
                 model_def: meta.ModelDefinition,
                 model_class: api.TracModel.__class__,
                 parameters: tp.Dict[str, tp.Any]):

        self.__model_def = model_def
        self.__model_class = model_class
        self.__model_log = util.logger_for_class(self.__model_class)

        self.__parameters = parameters or {}

        self.__val = ModelRuntimeValidator(frozenset(self.__parameters.keys()))

    def get_parameter(self, parameter_name: str) -> tp.Any:

        self.__val.check_param_valid_identifier(parameter_name)
        self.__val.check_param_exists(parameter_name)

        return self.__parameters[parameter_name]

    def get_table_schema(self, dataset_name: str) -> api.TableDefinition:
        raise NotImplementedError()

    def get_pandas_table(self, dataset_name: str) -> pd.DataFrame:
        raise NotImplementedError()

    def get_spark_table(self, dataset_name: str) -> pyss.DataFrame:
        raise NotImplementedError()

    def get_spark_table_rdd(self, dataset_name: str) -> pys.RDD:
        raise NotImplementedError()

    def put_table_schema(self, dataset_name: str, schema: api.TableDefinition):
        raise NotImplementedError()

    def put_pandas_table(self, dataset_name: str, dataset: pd.DataFrame):
        raise NotImplementedError()

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

    def __init__(self, param_names: tp.FrozenSet[str]):
        self.__param_names = param_names

    @classmethod
    def check_param_valid_identifier(cls, param_name: str):

        if not cls.__VALID_IDENTIFIER.match(param_name):
            raise ModelRuntimeException(f"Parameter name {param_name} is not a valid identifier")

    def check_param_exists(self, param_name: str):

        if param_name not in self.__param_names:
            raise ModelRuntimeException(f"Parameter {param_name} does not exist in the current context")
