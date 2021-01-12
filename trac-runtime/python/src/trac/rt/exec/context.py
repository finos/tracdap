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

import logging
import typing as tp

import pandas as pd
import pyspark as pys
from pyspark import sql as pyss

import trac.rt.api as api


class ModelContext(api.TracContext):

    def get_parameter(self, parameter_name: str) -> tp.Any:
        raise NotImplementedError()

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

    def get_logger(self) -> logging.Logger:
        raise NotImplementedError()
