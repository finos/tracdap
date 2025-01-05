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

import abc as _abc
import typing as _tp
import logging as _logging

# Import metadata domain objects into the API namespace
# This significantly improves type hinting, inline documentation and auto-complete in JetBrains IDEs
from tracdap.rt.metadata import *  # DOCGEN_REMOVE


if _tp.TYPE_CHECKING:

    try:
        import pandas
    except ModuleNotFoundError:
        pass

    try:
        import polars
    except ModuleNotFoundError:
        pass


class TracContext(metaclass=_abc.ABCMeta):

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

    .. seealso:: :py:class:`TracModel <tracdap.rt.api.TracModel>`
    """

    def get_parameter(self, parameter_name: str) -> _tp.Any:

        """
        Get the value of a model parameter.

        Model parameters defined using :py:meth:`define_parameters() <tracdap.rt.api.TracModel.define_parameters>`
        can be retrieved at runtime by this method. Values are returned as native Python types. Parameter names
        are case-sensitive.

        Attempting to retrieve parameters not defined by the model will result in a runtime validation
        error, even if those parameters are supplied in the job config and used by other models.

        :param parameter_name: The name of the parameter to get
        :return: The parameter value, as a native Python data type
        :type parameter_name: str
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """

        pass

    def has_dataset(self, dataset_name: str) -> bool:

        """
        Check whether a dataset is available in the current context.

        This method can be used to check whether optional model inputs have been supplied or not.
        Models should use this method before calling get methods on optional inputs.
        For inputs not marked as optional, this method will always return true. For outputs,
        this method will return true after the model calls a put method for the dataset.

        A runtime validation error will be raised if the dataset name is not defined
        as a model input or output.

        :param dataset_name: The name of the dataset to check
        :return: True if the dataset exists in the current context, False otherwise
        :type dataset_name: str
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """

        pass

    def get_schema(self, dataset_name: str) -> SchemaDefinition:

        """
        Get the schema of a model input or output.

        Use this method to get the :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
        for any input or output of the current model.
        For datasets with static schemas, these will be the same schemas that were defined using
        :py:meth:`define_inputs() <tracdap.rt.api.TracModel.define_inputs>` and
        :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`.

        For inputs with dynamic schemas, the schema of the provided input dataset will be returned.
        For outputs with dynamic schemas the schema must be set by calling
        :py:meth:`put_schema() <tracdap.rt.api.TracContext.put_schema>`, after which this method
        will return that schema. Calling :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>`
        for a dynamic output before the schema is set will result in a runtime validation error.

        For optional inputs, use :py:meth:`has_dataset() <tracdap.rt.api.TracContext.has_dataset>`
        to check whether the input was provided. Calling :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>`
        for an optional input that was not provided will always result in a validation error,
        regardless of whether the input has a static or dynamic schema. For optional outputs
        :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>` can be called, however if an
        output is both optional and dynamic then the schema must first be set by calling
        :py:meth:`put_schema() <tracdap.rt.api.TracContext.put_schema>`.

        Attempting to retrieve the schema for a dataset that is not defined as a model input or output
        will result in a runtime validation error, even if that dataset exists in the job config and
        is used by other models.

        :param dataset_name: The name of the input or output to get the schema for
        :return: The schema definition for the named dataset
        :type dataset_name: str
        :rtype: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """

        pass

    def get_pandas_table(self, dataset_name: str, use_temporal_objects: _tp.Optional[bool] = None) \
            -> "pandas.DataFrame":

        """
        Get the data for a model input or output as a Pandas dataframe.

        Model inputs can be accessed as Pandas dataframes using this method.
        The TRAC runtime will handle fetching data from storage and apply any necessary
        format conversions (to improve performance, data may be preloaded).
        Only defined inputs can be accessed, use
        :py:meth:`define_inputs() <tracdap.rt.api.TracModel.define_inputs>`
        to define the inputs of a model. Input names are case-sensitive.

        Model inputs are always available and can be accessed at any time inside
        :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>`.
        Model outputs can also be retrieved using this method, however they are
        only available after they have been saved using
        :py:meth:`put_pandas_table() <tracdap.rt.api.TracContext.put_pandas_table>`
        (or another put method). Calling this method will simply return the
        saved dataset.

        Attempting to retrieve a dataset that is not defined as a model input or
        output will result in a runtime validation error, even if that dataset
        exists in the job config and is used by other models. Attempting to retrieve
        an output before it has been saved will also cause a validation error.

        :param dataset_name: The name of the model input or output to get data for
        :param use_temporal_objects: Use Python objects for date/time fields instead of the NumPy *datetime64* type
        :return: A pandas dataframe containing the data for the named dataset
        :type dataset_name: str
        :type use_temporal_objects: bool | None
        :rtype: :py:class:`pandas.DataFrame`
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """
        pass

    def get_polars_table(self, dataset_name: str) -> "polars.DataFrame":

        """
        Get the data for a model input or output as a Polars dataframe.

        This method has equivalent semantics to :py:meth:`get_pandas_table`, but returns
        a Polars dataframe.

        :param dataset_name: The name of the model input or output to get data for
        :return: A polars dataframe containing the data for the named dataset
        :type dataset_name: str
        :rtype: :py:class:`polars.DataFrame`
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """

        pass

    def get_file(self, file_name: str) -> bytes:

        pass

    def get_file_stream(self, file_name: str) -> _tp.ContextManager[_tp.BinaryIO]:

        pass

    def put_schema(self, dataset_name: str, schema: SchemaDefinition):

        """
        Set the schema of a dynamic model output.

        For outputs marked as dynamic in :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`,
        a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` must be supplied using this
        method before attempting to save the data. Once a schema has been set, it can be retrieved by calling
        :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>` and data can be saved using
        :py:meth:`put_pandas_table() <tracdap.rt.api.TracContext.put_pandas_table>` or another put method.

        TRAC API functions are available to help with building schemas, such as
        :py:func:`trac.F() <tracdap.rt.api.F>` to define individual fields or
        :py:func:`load_schema() <tracdap.rt.api.load_schema>` to load predefined schemas.
        See the :py:mod:`tracdap.rt.api` package for a full list of functions that can be used
        to build and manipulate schemas.

        Each schema can only be set once and the schema will be validated using the normal
        validation rules. If :py:meth:`put_schema() <tracdap.rt.api.TracContext.put_schema>` is called for
        an optional output the model must supply data for that output, otherwise TRAC will report a
        validation error after the model completes.

        Attempting to set the schema for a dataset that is not defined as a dynamic model output
        for the current model will result in a runtime validation error. Supplying a schema that
        fails validation will also result in a validation error.


        :param dataset_name: The name of the output to set the schema for
        :param schema: A TRAC schema definition to use for the named output
        :type dataset_name: str
        :type schema: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`
        """

        pass

    def put_pandas_table(self, dataset_name: str, dataset: "pandas.DataFrame"):

        """
        Save the data for a model output as a Pandas dataframe.

        Model outputs can then be saved as Pandas dataframes using this method.
        The TRAC runtime will validate the supplied data and send it to storage,
        applying any necessary format conversions. Only defined outputs can be
        saved, use :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`
        to define the outputs of a model. Output names are case-sensitive. Once
        an output has been saved it can be retrieved by calling
        :py:meth:`get_pandas_table() <tracdap.rt.api.TracContext.get_pandas_table>`
        (or another get method).

        Each model output can only be saved once and the supplied data must match the schema of
        the named output. Missing fields or fields of the wrong type will result in a data
        conformance error. Extra fields will be discarded with a warning. The schema of an output
        dataset can be checked using :py:meth:`get_schema() <tracdap.rt.api.TracContext.get_schema>`.
        For dynamic outputs, the schema must first be set using
        :py:meth:`put_schema() <tracdap.rt.api.TracContext.put_schema>`

        Attempting to save a dataset that is not defined as a model output will cause a runtime
        validation error. Attempting to save an output twice, or save a dynamic output before its
        schema is set will also cause a validation error.

        :param dataset_name: The name of the model output to save data for
        :param dataset: A pandas dataframe containing the data for the named dataset
        :type dataset_name: str
        :type dataset: :py:class:`pandas.Dataframe`
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`,
                 :py:class:`EDataConformance <tracdap.rt.exceptions.EDataConformance>`
        """

        pass

    def put_polars_table(self, dataset_name: str, dataset: "polars.DataFrame"):

        """
        Save the data for a model output as a Polars dataframe.

        This method has equivalent semantics to :py:meth:`put_pandas_table`, but accepts
        a Polars dataframe.

        :param dataset_name: The name of the model output to save data for
        :param dataset: A polars dataframe containing the data for the named dataset
        :type dataset_name: str
        :type dataset: :py:class:`polars.DataFrame`
        :raises: :py:class:`ERuntimeValidation <tracdap.rt.exceptions.ERuntimeValidation>`,
                 :py:class:`EDataConformance <tracdap.rt.exceptions.EDataConformance>`
        """

        pass

    def put_file(self, file_name: str, file_content: _tp.Union[bytes, bytearray]):

        pass

    def put_file_stream(self, file_name: str) -> _tp.ContextManager[_tp.BinaryIO]:

        pass

    def log(self) -> _logging.Logger:

        """
        Get a Python logger that can be used for writing model logs.

        Logs written to this logger are recorded by TRAC. When models are run on the platform,
        these logs are assembled and saved with the job outputs as a dataset, that can be queried
        through the regular TRAC data and metadata APIs.

        :return: A Python logger that can be used for writing model logs
        :rtype: :py:class:`logging.Logger`
        """

        pass


class TracModel(metaclass=_abc.ABCMeta):

    """
    Base class that model components inherit from to be recognised by the platform

    The modelling API is designed to be as simple and un-opinionated as possible.
    Models inherit from :py:class:`TracModel` and implement the :py:meth:`run_model()` method to provide their
    model logic. :py:meth:`run_model()` has one parameter, a :class:`TracContext` object which is supplied to
    the model at runtime, allowing it to access parameters, inputs and outputs.

    Models must also as a minimum implement three methods to define the model schema,
    :py:meth:`define_parameters()`, :py:meth:`define_inputs()` and :py:meth:`define_outputs()`.
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

    .. seealso:: :py:class:`TracContext <tracdap.rt.api.TracContext>`
    """

    def define_attributes(self) -> _tp.Dict[str, Value]:  # noqa

        """
        Define attributes that will be associated with the model when it is loaded into the TRAC platform.

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
        Define parameters that will be available to the model at runtime.

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
        Define data inputs that will be available to the model at runtime.

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
        Define data outputs that will be produced by the model at runtime.

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
        Entry point for running model code.

        Implement this method to provide the model logic. A
        :py:class:`TracContext <tracdap.rt.api.TracContext>` is provided
        at runtime, which makes parameters and inputs available and provides a means to save outputs.
        All the outputs defined in :py:meth:`define_outputs` must be saved before this method returns,
        otherwise a runtime validation error will be raised.

        Model code can raise exceptions, either in a controlled way by detecting error conditions and raising
        errors explicitly, or in an uncontrolled way as a result of bugs in the model code. Exceptions may also
        originate inside libraries the model code is using. If an exception escapes from
        :py:meth:`run_model() <tracdap.rt.api.TracModel.run_model>`
        TRAC will mark the model as failed, the job that contains the model will also fail.

        :param ctx: A context use to access model inputs, outputs and parameters
                    and communicate with the TRAC platform
        :type ctx: :py:class:`TracContext <tracdap.rt.api.TracContext>`
        """

        pass
