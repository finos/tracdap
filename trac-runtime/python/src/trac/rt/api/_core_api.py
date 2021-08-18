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

import abc as _abc
import typing as _tp
import logging as _logging

import trac.rt.metadata as _meta

import pandas as _pd
import pyspark as _pys
import pyspark.sql as _pyss


class TracContext:

    @_abc.abstractmethod
    def get_parameter(self, parameter_name: str) -> _tp.Any:
        pass

    @_abc.abstractmethod
    def get_table_schema(self, dataset_name: str) -> _meta.TableDefinition:
        pass

    @_abc.abstractmethod
    def get_pandas_table(self, dataset_name: str) -> _pd.DataFrame:
        pass

    @_abc.abstractmethod
    def get_spark_table(self, dataset_name: str) -> _pyss.DataFrame:
        pass

    @_abc.abstractmethod
    def get_spark_table_rdd(self, dataset_name: str) -> _pys.RDD:
        pass

    @_abc.abstractmethod
    def put_table_schema(self, dataset_name: str, schema: _meta.TableDefinition):
        pass

    @_abc.abstractmethod
    def put_pandas_table(self, dataset_name: str, dataset: _pd.DataFrame):
        pass

    @_abc.abstractmethod
    def put_spark_table(self, dataset_name: str, dataset: _pyss.DataFrame):
        pass

    @_abc.abstractmethod
    def put_spark_table_rdd(self, dataset_name: str, dataset: _pys.RDD):
        pass

    @_abc.abstractmethod
    def get_spark_context(self) -> _pys.SparkContext:
        pass

    @_abc.abstractmethod
    def get_spark_sql_context(self) -> _pyss.SQLContext:
        pass

    @_abc.abstractmethod
    def log(self) -> _logging.Logger:
        pass


class TracModel:

    @_abc.abstractmethod
    def define_parameters(self) -> _tp.Dict[str, _meta.ModelParameter]:
        pass

    @_abc.abstractmethod
    def define_inputs(self) -> _tp.Dict[str, _meta.TableDefinition]:
        pass

    @_abc.abstractmethod
    def define_outputs(self) -> _tp.Dict[str, _meta.TableDefinition]:
        pass

    @_abc.abstractmethod
    def run_model(self, ctx: TracContext):
        pass
