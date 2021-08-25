#  Copyright 2020 Accenture Global Solutions Limited
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

import abc
import typing as tp
import logging

import trac.rt.metadata as _meta

import pandas as pd
import pyspark as pys
import pyspark.sql as pyss


class TracContext:

    @abc.abstractmethod
    def get_parameter(self, parameter_name: str) -> tp.Any:
        pass

    @abc.abstractmethod
    def get_table_schema(self, dataset_name: str) -> _meta.TableDefinition:
        pass

    @abc.abstractmethod
    def get_pandas_table(self, dataset_name: str) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def get_spark_table(self, dataset_name: str) -> pyss.DataFrame:
        pass

    @abc.abstractmethod
    def get_spark_table_rdd(self, dataset_name: str) -> pys.RDD:
        pass

    @abc.abstractmethod
    def put_table_schema(self, dataset_name: str, schema: _meta.TableDefinition):
        pass

    @abc.abstractmethod
    def put_pandas_table(self, dataset_name: str, dataset: pd.DataFrame):
        pass

    @abc.abstractmethod
    def put_spark_table(self, dataset_name: str, dataset: pyss.DataFrame):
        pass

    @abc.abstractmethod
    def put_spark_table_rdd(self, dataset_name: str, dataset: pys.RDD):
        pass

    @abc.abstractmethod
    def get_spark_context(self) -> pys.SparkContext:
        pass

    @abc.abstractmethod
    def get_spark_sql_context(self) -> pyss.SQLContext:
        pass

    @abc.abstractmethod
    def log(self) -> logging.Logger:
        pass


class TracModel:

    @abc.abstractmethod
    def define_parameters(self) -> tp.Dict[str, _meta.ModelParameter]:
        pass

    @abc.abstractmethod
    def define_inputs(self) -> tp.Dict[str, _meta.TableDefinition]:
        pass

    @abc.abstractmethod
    def define_outputs(self) -> tp.Dict[str, _meta.TableDefinition]:
        pass

    @abc.abstractmethod
    def run_model(self, ctx: TracContext):
        pass
