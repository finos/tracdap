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

    """
    Interface that allows model components to interact with the platform at runtime

    TRAC supplies every model with a context when the model is run. The context allows
    models to access parameters, inputs, outputs and schemas, as well as other resources
    such as the Spark context (if the model is using Spark) and model logs.

    TRAC guarantees that everything defined in the model (parameters, inputs and outputs)
    will be available in the context when the model is running. So, if a model defines a
    parameter called "param1" as an integer, the model will be able to call get_parameter("param1")
    and will receive an integer value.

    When a model is running on a production deployment of the TRAC platform, parameters, inputs and
    outputs will be supplied by TRAC as part of the job. These could be coming from entries selected
    by a user in the UI or settings configured as part of a scheduled task. To develop models locally,
    a job config file can be supplied (typically in YAML or JSON) to set the required parameters, inputs
    and output locations. In either case, TRAC will validate the supplied configuration against the
    model definition to make sure the context always includes exactly what the model requires.

    All the context API methods are validated at runtime and will raise ERuntimeValidation if a model
    tries to access an unknown identifier or perform some other invalid operation.

    .. seealso:: TracModel
    """

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

    """
    Base class that model components inherit from to be recognised by the platform

    The TracModel API is designed to be as simple and un-opinionated as possible.
    Models inherit from TracModel and implement the :meth:run_model() method to provide their model logic.
    :meth:run_model() has one parameter, a :class:TracContext object which is supplied to the model at
    runtime, allowing it to access parameters, inputs and outputs.

    Models must also as a minimum implement three methods to define their schema, :meth:define_parameters(),
    :meth:define_inputs() and :meth:define_outputs(). The parameters, inputs and outputs that are
    defined will be available in the context at runtime. The :py:mod:trac.api package includes a
    number of helper functions to implement these methods in a clear and robust way.

    While model components can largely do what they like, there are three rules that should be followed
    to ensure models are deterministic. These are:

        1. No threading
        2. Use TRAC for random number generation
        3. Use TRAC to access the current time

    Threading should never be needed in model code, Python only runs one execution thread at a time and TRAC
    already handles IO masking and model ordering. Both Pandas and PySpark provide compute concurrency.
    Random numbers and time will be made available in the TracContext API in a future version of TRAC.

    Models should also avoid system calls, or using the Python builtins exec() or eval().
    """

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
