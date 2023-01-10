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

from __future__ import annotations

import abc as _abc
import typing as _tp
import logging as _logging

# Import metadata domain objects into the API namespace
# This significantly improves type hinting, inline documentation and auto-complete in JetBrains IDEs
from tracdap.rt.metadata import *  # DOCGEN_REMOVE

if _tp.TYPE_CHECKING:
    import pandas


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

    .. seealso:: :py:class:`TracModel`
    """

    @_abc.abstractmethod
    def get_parameter(self, parameter_name: str) -> _tp.Any:

        """
        Get the value of a model parameter

        Model parameters defined in :py:meth:`TracModel.define_parameters` can be retrieved at runtime
        by this method. Values are returned as native Python types. Parameter names are case-sensitive.

        Attempting to retrieve parameters not defined by the model will result in a runtime validation
        error, even if those parameters are supplied in the job config and used by other models.

        :param parameter_name: The name of the parameter to get
        :return: The parameter value, as a native Python data type
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """

        pass

    @_abc.abstractmethod
    def get_schema(self, dataset_name: str) -> SchemaDefinition:

        """
        Get the schema of a model input or output

        The schema of an input or output can be retrieved and examined at runtime using this method.
        Inputs must be defined in :py:meth:`TracModel.define_inputs`
        and outputs in :py:meth:`TracModel.define_outputs`.
        Input and output names are case-sensitive.

        In the current version of the runtime all model inputs and outputs are defined statically,
        :py:meth:`get_schema` will return the schema as it was defined.

        Attempting to retrieve the schema for a dataset that is not defined as a model input or output
        will result in a runtime validation error, even if that dataset exists in the job config and
        is used by other models.

        :param dataset_name: The name of the input or output to get the schema for
        :return: The schema definition for the named dataset
        :rtype: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """

        pass

    @_abc.abstractmethod
    def get_pandas_table(self, dataset_name: str, use_temporal_objects: _tp.Optional[bool] = None) -> pandas.DataFrame:

        """
        Get the data for a model input or output as a Pandas dataframe

        The data for both inputs and outputs can be retrieved as a Pandas dataframe using this method.
        Inputs must be defined in :py:meth:`TracModel.define_inputs`
        and outputs in :py:meth:`TracModel.define_outputs`.
        Input and output names are case-sensitive.

        The TRAC runtime will handle loading the data and assembling it into a Pandas dataframe.
        This may happen before the model runs or when a dataset is requested. Models should take
        care not to request very large datasets as Pandas tables, doing so is likely to cause a
        memory overflow. Use :py:meth:`get_spark_table` instead to work with big data.

        Model inputs are always available and can be queried by this method. Outputs are only available
        after they have been saved to the context using :py:meth:`put_pandas_table` (or another
        put_XXX_table method). Attempting to retrieve an output before it has been saved will cause a
        runtime validation error.

        Attempting to retrieve a dataset that is not defined as a model input or output will result
        in a runtime validation error, even if that dataset exists in the job config and is used by
        other models.

        :param dataset_name: The name of the model input or output to get data for
        :param use_temporal_objects: Use Python objects for date/time fields instead of the NumPy *datetime64* type
        :return: A pandas dataframe containing the data for the named dataset
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """
        pass

    @_abc.abstractmethod
    def put_pandas_table(self, dataset_name: str, dataset: pandas.DataFrame):

        """
        Save the data for a model output as a Pandas dataframe

        The data for model outputs can be saved as a Pandas dataframe using this method.
        Outputs must be defined in :py:meth:`TracModel.define_outputs`.
        Output names are case-sensitive.

        The supplied data must match the schema of the named output. Missing fields or fields
        of the wrong type will result in a data validation error. Extra fields will be discarded
        with a warning. The schema of an output dataset can be checked using :py:meth:`get_schema`.

        Each model output can only be saved once. Attempting to save the same output twice will
        cause a runtime validation error. Once an output has been saved, it can be retrieved by
        calling :py:meth:`get_pandas_table` (or another get_XXX_table method). Attempting to save
        a dataset that is not defined as a model output will also cause a runtime validation error.

        :param dataset_name: The name of the model output to save data for
        :param dataset: A pandas dataframe containing the data for the named dataset
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`,
                 :py:class:`EDataValidation <tracdap.rt.exceptions.EDataValidation>`
        """

        pass

    @_abc.abstractmethod
    def log(self) -> _logging.Logger:

        """
        Get a Python logger that can be used for writing model logs

        Logs written to this logger are recorded by TRAC. When models are run on the platform,
        these logs are assembled and saved with the job outputs as a dataset, that can be queried
        through the regular TRAC data and metadata APIs.

        :return: A Python logger that can be used for writing model logs
        """

        pass


class TracModel:

    """
    Base class that model components inherit from to be recognised by the platform

    The modelling API is designed to be as simple and un-opinionated as possible.
    Models inherit from :py:class:`TracModel` and implement the :py:meth:`run_model` method to provide their
    model logic. :py:meth:`run_model` has one parameter, a :class:`TracContext` object which is supplied to
    the model at runtime, allowing it to access parameters, inputs and outputs.

    Models must also as a minimum implement three methods to define the model schema,
    :py:meth:`define_parameters()<TracModel.define_parameters>`,
    :py:meth:`define_inputs()<TracModel.define_inputs>` and
    :py:meth:`define_outputs()<TracModel.define_outputs>`.
    The parameters, inputs and outputs that are defined will be available in the context at runtime.
    The :py:mod:`tracdap.rt.api` package includes a number of helper functions to implement these methods in
    a clear and robust way.

    While model components can largely do what they like, there are three rules that should be followed
    to ensure models are deterministic. These are:

        1. No threading
        2. Use TRAC for random number generation
        3. Use TRAC to access the current time

    Threading should never be needed in model code, Python only runs one execution thread at a time and TRAC
    already handles IO masking and model ordering. Both Pandas and PySpark provide compute concurrency.
    Random numbers and time will be made available in the :py:class:`TracContext` API in a future version of TRAC.

    Models should also avoid making system calls, or using the Python builtins exec() or eval().

    .. seealso:: :py:class:`TracContext`
    """

    def define_attributes(self) -> _tp.Dict[str, Value]:  # noqa

        """
        Define attributes that will be associated with the model when it is loaded into the TRAC platform

        .. note::
            This is an experimental API that is not yet stabilised, expect changes in future versions of TRAC

        These attributes can be used to index or describe the model, they will be available for metadata searches.
        Attributes must be primitive (scalar) values that can be expressed in the TRAC type system.
        Multivalued attributes can be supplied as lists, in which case the attribute type must be given explicitly.
        Controlled attributes (starting with trac\\_ or \\_) are not allowed and will fail validation.

        To define attributes in code, always use the define_* functions in the :py:mod:`tracdap.rt.api` package.
        This will ensure attributes are defined in the correct format with all the required fields.
        Attributes that are defined in the wrong format or with required fields missing
        will result in a model validation failure.

        :return: A set of attributes that will be applied to the model when it is loaded into the TRAC platform
        :rtype: Dict[str, :py:class:`Value <tracdap.rt.metadata.Value>`]
        """

        return {}

    @_abc.abstractmethod
    def define_parameters(self) -> _tp.Dict[str, ModelParameter]:

        """
        Define parameters that will be available to the model at runtime

        Implement this method to define the model's parameters, every parameter that the
        model uses must be defined. Models may choose to ignore some parameters,
        it is ok to define parameters that are not always used.

        To define model parameters in code, always use the define_* functions in the :py:mod:`tracdap.rt.api` package.
        This will ensure parameters are defined in the correct format with all the required fields.
        Parameters that are defined in the wrong format or with required fields missing
        will result in a model validation failure.

        :return: The full set of parameters that will be available to the model at
        :rtype: Dict[str, :py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
        """

        pass

    @_abc.abstractmethod
    def define_inputs(self) -> _tp.Dict[str, ModelInputSchema]:

        """
        Define data inputs that will be available to the model at runtime

        Implement this method to define the model's inputs, every data input that the
        model uses must be defined. Models may choose to ignore some inputs,
        it is ok to define inputs that are not always used.

        To define model inputs in code, always use the define_* functions in the :py:mod:`tracdap.rt.api` package.
        This will ensure inputs are defined in the correct format with all the required fields.
        Model inputs that are defined in the wrong format or with required fields missing
        will result in a model validation failure.

        :return: The full set of inputs that will be available to the model at runtime
        :rtype: Dict[str, :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`]
        """

        pass

    @_abc.abstractmethod
    def define_outputs(self) -> _tp.Dict[str, ModelOutputSchema]:

        """
        Define data outputs that will be produced by the model at runtime

        Implement this method to define the model's outputs, every data output that the
        model produces must be defined and every output that is defined must be
        produced. If a model defines an output which is not produced, a runtime
        validation error will be raised after the model completes.

        To define model outputs in code, always use the define_* functions in the :py:mod:`tracdap.rt.api` package.
        This will ensure outputs are defined in the correct format with all the required fields.
        Model outputs that are defined in the wrong format or with required fields missing
        will result in a model validation failure.

        :return: The full set of outputs that will be produced by the model at runtime
        :rtype: Dict[str, :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`]
        """

        pass

    @_abc.abstractmethod
    def run_model(self, ctx: TracContext):

        """
        Entry point for running model code

        Implement this method to provide the model logic. A :py:class:`TracContext` is provided
        at runtime, which makes parameters and inputs available and provides a means to save outputs.
        All the outputs defined in :py:meth:`define_outputs` must be saved before this method returns,
        otherwise a runtime validation error will be raised.

        Model code can raise exceptions, either in a controlled way by detecting error conditions and raising
        errors explicitly, or in an uncontrolled way as a result of bugs in the model code. Exceptions may also
        originate inside libraries the model code is using. If an exception escapes from :py:meth`run_model`
        TRAC will mark the model as failed, the job that contains the model will also fail.

        :param ctx: A context use to access model inputs, outputs and parameters
                    and communicate with the TRAC platform
        """

        pass
