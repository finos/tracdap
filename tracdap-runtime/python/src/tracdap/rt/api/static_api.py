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

import typing as _tp

if _tp.TYPE_CHECKING:
    import pathlib as _path
    import types as _ts

from .hook import RuntimeHook as _RuntimeHook
from .hook import Named as _Named

# Import metadata domain objects into the API namespace
# This significantly improves type hinting, inline documentation and auto-complete in JetBrains IDEs
from tracdap.rt.metadata import *  # DOCGEN_REMOVE


def define_parameter(
        param_name: str, param_type: _tp.Union[TypeDescriptor, BasicType],
        label: str, default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[ModelParameter]:

    """
    Define an individual model parameter

    Individual model parameters can be defined using this method (or :py:func:`trac.P<P>`).
    The name, type and label are required fields to define a parameter. Name is used as the identifier
    to work with the parameter in code, e.g. when calling :py:meth:`get_parameter` or defining parameters
    in a job config.

    If a default value is specified, the model parameter becomes optional. It is ok to omit optional parameters
    when running models or setting up jobs, in which case the default value will be used. If no default is
    specified then the model parameter becomes mandatory, a value must always be supplied in order to execute
    the model.

    Once defined model parameters can be passed to :py:func:`define_parameters`,
    either as a list or as individual arguments, to create the set of parameters for a model.

    :param param_name: The parameter name, used to identify the parameter in code (must be a valid identifier)
    :param param_type: The parameter type, expressed in the TRAC type system
    :param label: A descriptive label for the parameter (required)
    :param default_value: A default value to use if no explicit value is supplied (optional)
    :return: A named model parameter, suitable for passing to :py:func:`define_parameters`
    """

    rh = _RuntimeHook.runtime()
    return rh.define_parameter(param_name, param_type, label, default_value)


def declare_parameter(
        param_name: str,
        param_type: _tp.Union[TypeDescriptor, BasicType],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[ModelParameter]:

    """
    Alias for :py:func:`define_parameter` (deprecated)

    .. deprecated:: 0.5
       Use :py:func:`define_parameter` or :py:func:`P` instead.
    """

    return define_parameter(param_name, param_type, label, default_value)


def P(  # noqa
        param_name: str,
        param_type: _tp.Union[TypeDescriptor, BasicType],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[ModelParameter]:

    """Shorthand alias for :py:func:`define_parameter`"""

    return declare_parameter(
        param_name, param_type, label,
        default_value)


def define_parameters(
        *params: _tp.Union[_Named[ModelParameter], _tp.List[_Named[ModelParameter]]]) \
        -> _tp.Dict[str, ModelParameter]:

    """
    Defined all the parameters used by a model

    Parameters can be supplied either as individual arguments to this function or as a list.
    In either case, each parameter should be defined using :py:func:`define_parameter`
    (or :py:func:`trac.P <tracdap.rt.api.P>`).

    :param params: The parameters that will be defined, either as individual arguments or as a list
    :return: A set of model parameters, in the correct format to return from :py:meth:TracModel.define_parameters
    """

    rh = _RuntimeHook.runtime()
    return rh.define_parameters(*params)


def declare_parameters(
        *params: _tp.Union[_Named[ModelParameter], _tp.List[_Named[ModelParameter]]]) \
        -> _tp.Dict[str, ModelParameter]:

    """
    Alias for :py:func:`define_parameters` (deprecated)

    .. deprecated:: 0.5
       Use :py:func:`define_parameters` instead.
    """

    return define_parameters(*params)


def define_field(
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """
    Define the schema for an individual field, which can be used in a model input or output schema.

    Individual fields in a dataset can be defined using this method or the shorthand alias :py:func:`F`.
    The name, type and label of a field are always required.
    The business_key and categorical flags are false by default.
    Format code is optional.

    If no field ordering is supplied, fields will automatically be assigned a contiguous ordering starting at 0.
    In this case care must be taken when creating an updated version of a model, that the order of existing
    fields is not disturbed. Adding fields to the end of a list is always safe.
    If field orders are specified explicitly, the must for a contiguous ordering starting at 0.

    Once defined field schemas can be passed to :py:func:`define_input_table` or :py:func:`define_output_table`,
    either as a list or as individual arguments, to create the full schema for an input or output.

    :param field_name: The field's name, used as the field identifier in code and queries (must be a valid identifier)
    :param field_type: The data type of the field, only primitive types are allowed
    :param label: A descriptive label for the field (required)
    :param business_key: Flag indicating whether this field is a business key for its dataset (default: False)
    :param categorical: Flag indicating whether this is a categorical field (default: False)
    :param format_code: A code that can be interpreted by client applications to format the field (optional)
    :param field_order: Explicit field ordering (optional)
    :return: A field schema, suitable for use in a schema definition
    """

    rh = _RuntimeHook.runtime()

    return rh.define_field(
        field_name, field_type, label,
        business_key, categorical,
        format_code, field_order)


def declare_field(
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """
    Alias for :py:func:`define_field` (deprecated)

    .. deprecated:: 0.5
       Use :py:func:`define_field` or :py:func:`F` instead.
    """

    return define_field(
        field_name, field_type, label,
        business_key, categorical,
        format_code, field_order)


def F(  # noqa
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """Shorthand alias for :py:func:`define_field`"""

    return define_field(
        field_name, field_type, label,
        business_key, categorical,
        format_code, field_order)


def define_schema(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]],
        schema_type: SchemaType = SchemaType.TABLE) \
        -> SchemaDefinition:

    # TODO: Doc comment

    rh = _RuntimeHook.runtime()
    return rh.define_schema(*fields, schema_type=schema_type)


def load_schema(
        package: _tp.Union[_ts.ModuleType, str], schema_file: _tp.Union[str, _path.Path],
        schema_type: SchemaType = SchemaType.TABLE) \
        -> SchemaDefinition:

    # TODO: Doc comment

    rh = _RuntimeHook.runtime()
    return rh.load_schema(package, schema_file, schema_type=schema_type)


def define_input_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelInputSchema:

    """
    Define a model input with a table schema.

    Fields can be supplied either as individual arguments to this function or as a list.
    Individual fields should be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.

    :param fields: A set of fields to make up a :py:class:`TableSchema <trac.rt.metadata.TableSchema>`

    :return: A model input schema, suitable for returning from :py:meth:`TracModel.define_inputs`
    """

    rh = _RuntimeHook.runtime()
    return rh.define_input_table(*fields)


def declare_input_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelInputSchema:

    """
    Alias for :py:func:`define_input_table` (deprecated)

    .. deprecated:: 0.5
       Use :py:func:`define_input_table` instead.
    """

    return define_input_table(*fields)


def define_output_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelOutputSchema:

    """
    Define a model output with a table schema.

    Fields can be supplied either as individual arguments to this function or as a list.
    Individual fields should be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.

    :param fields: A set of fields to make up a :py:class:`TableSchema <trac.rt.metadata.TableSchema>`

    :return: A model output schema, suitable for returning from :py:meth:`TracModel.define_outputs`
    """

    rh = _RuntimeHook.runtime()
    return rh.define_output_table(*fields)


def declare_output_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelOutputSchema:

    """
    Alias for :py:func:`define_output_table` (deprecated)

    .. deprecated:: 0.5
       Use :py:func:`define_output_table` instead.
    """

    return define_output_table(*fields)
