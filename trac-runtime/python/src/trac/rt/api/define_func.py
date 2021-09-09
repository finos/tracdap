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

from __future__ import annotations

import typing as _tp
import dataclasses as _dc
import inspect as _inspect
import trac.rt.metadata as _meta


__field_def_params = _inspect.signature(_meta.FieldSchema.__init__).parameters


_T = _tp.TypeVar("_T")


# Utility class for passing named items between declare_ funcs
# Using a private class avoids creating noise in the doc gen / doc comments
@_dc.dataclass
class _Named(_tp.Generic[_T]):

    itemName: str
    item: _T


def declare_parameters(
        *params: _tp.Union[_Named[_meta.ModelParameter], _tp.List[_Named[_meta.ModelParameter]]]) \
        -> _tp.Dict[str, _meta.ModelParameter]:

    """
    Declare all the parameters used by a model

    Parameters can be supplied either as individual arguments to this function or as a list.
    In either case, each parameter should be declared using
    :py:func:`declare_parameter` (or :py:func:`trac.P<P>`).

    :param params: The parameters that will be defined, either as individual arguments or as a list
    :return: A set of model parameters, in the correct format to return from :py:meth:TracModel.define_parameters
    """

    if len(params) == 1 and isinstance(params[0], list):
        return {p.itemName: p.item for p in params[0]}
    else:
        return {p.itemName: p.item for p in params}


def declare_parameter(
        param_name: str,
        param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[_meta.ModelParameter]:

    """
    Declare an individual model parameter

    Individual model parameters can be declared using this method (or :py:func:`trac.P<P>`).
    The name, type and label are required fields to declare a parameter. Name is used as the identifier
    to work with the parameter in code, e.g. when calling :py:meth:`get_parameter` or defining parameters
    in a job config.

    If a default value is specified, the model parameter becomes optional. It is ok to omit optional parameters
    when running models or setting up jobs, in which case the default value will be used. If no default is
    specified then the model parameter becomes mandatory, a value must always be supplied in order to execute
    the model.

    Declared parameters should be passed to :py:func:`declare_parameters`, either individually
    or as a list, to create the set of parameters for a model.

    :param param_name: The parameter name, used to identify the parameter in code (must be a valid identifier)
    :param param_type: The parameter type, expressed in the TRAC type system
    :param label: A descriptive label for the parameter (required)
    :param default_value: A default value to use if no explicit value is supplied (optional)
    :return: A named model parameter, suitable for passing to :py:func:`declare_parameters`
    """

    if isinstance(param_type, _meta.TypeDescriptor):
        param_type_descriptor = param_type
    else:
        param_type_descriptor = _meta.TypeDescriptor(param_type, None, None)

    return _Named(param_name, _meta.ModelParameter(param_type_descriptor, label, default_value))


def declare_input_table(
        *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
        -> _meta.ModelInputSchema:

    """
    Declare a model input with a table schema

    Fields can be supplied either as individual arguments to this function or as a list.
    In either case, each field should be declared using
    :py:func:`declare_field` (or :py:func:`trac.F<F>`).

    :param fields: A set of fields to make up a :py:class:`TableSchema<trac.rt.metadata.TableSchema>`

    :return: A model input schema, suitable for returning from :py:meth:`TracModel.define_inputs`
    """

    if len(fields) == 1 and isinstance(fields[0], list):
        fields_ = fields[0]
    else:
        fields_ = fields

    table_def = _meta.TableSchema([*fields_])
    schema_def = _meta.SchemaDefinition(_meta.SchemaType.TABLE, table=table_def)

    return _meta.ModelInputSchema(schema=schema_def)


def declare_output_table(
        *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
        -> _meta.ModelOutputSchema:

    """
    Declare a model output with a table schema

    Fields can be supplied either as individual arguments to this function or as a list.
    In either case, each field should be declared using
    :py:func:`declare_field` (or :py:func:`trac.F<F>`).

    :param fields: A set of fields to make up a :py:class:`TableSchema<trac.rt.metadata.TableSchema>`

    :return: A model output schema, suitable for returning from :py:meth:`TracModel.define_outputs`
    """

    if len(fields) == 1 and isinstance(fields[0], list):
        fields_ = fields[0]
    else:
        fields_ = fields

    table_def = _meta.TableSchema([*fields_])
    schema_def = _meta.SchemaDefinition(_meta.SchemaType.TABLE, table=table_def)

    return _meta.ModelOutputSchema(schema=schema_def)


def declare_field(
        field_name: str,
        field_type: _meta.BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> _meta.FieldSchema:

    """
    Declare an individual field, for use in a model input or output schema

    Individual fields in a dataset can be declared using this method (or :py:func:`trac.F<F>`).
    The name, type and label of a field are required parameters. The business_key and categorical
    flags are false by default. Format code is optional.

    If no field ordering is supplied, fields will automatically be assigned a contiguous ordering starting at 0.
    In this case care must be taken when creating an updated version of a model, that the order of existing
    fields is not disturbed. Adding fields to the end of a list is always safe.
    If field orders are specified explicitly, the must for a contiguous ordering starting at 0.

    Declared fields should be passed to :py:func:`declare_input_table` or :py:func:`declare_output_table`,
    either individually or as a list, to create the full schema for an input or output.

    :param field_name: The field's name, used as the field identifier in code and queries (must be a valid identifier)
    :param field_type: The data type of the field, only primitive types are allowed
    :param label: A descriptive label for the field (required)
    :param business_key: Flag indicating whether this field is a business key for its dataset (default: False)
    :param categorical: Flag indicating whether this is a categorical field (default: False)
    :param format_code: A code that can be interpreted by client applications to format the field (optional)
    :param field_order: Explicit field ordering (optional)
    :return: A field schema, suitable for use in a schema definition
    """

    return _meta.FieldSchema(
        field_name,
        field_order,
        field_type,
        label,
        businessKey=business_key,
        categorical=categorical,
        formatCode=format_code)


def P(*args, **kwargs):  # noqa
    """Shorthand alias for :py:func:`declare_parameter`"""
    return declare_parameter(*args, **kwargs)


def F(*args, **kwargs):  # noqa
    """Shorthand alias for :py:func:`declare_field`"""
    return declare_field(*args, **kwargs)
