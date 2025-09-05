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

from __future__ import annotations

import sys
import typing as _tp
import types as _ts

from .hook import _StaticApiHook
from .hook import _Named

# Import metadata domain objects into the API namespace
# This significantly improves type hinting, inline documentation and auto-complete in JetBrains IDEs
from tracdap.rt.metadata import *  # DOCGEN_REMOVE


def define_attributes(*attributes: _tp.Union[TagUpdate, _tp.List[TagUpdate]]) -> _tp.List[TagUpdate]:

    """
    Define a set of attributes to catalogue and describe a model

    .. note::
        This is an experimental API that is not yet stabilised, expect changes in future versions of TRAC

    Model attributes can be defined using :py:func:`define_attribute` or the shorthand alias :py:func:`A`.
    This function takes a number of model attributes, either as individual arguments or as a list,
    and arranges them in the format required by
    :py:meth:`TracModel.define_attributes() <tracdap.rt.api.TracModel.define_attributes>`.

    :param attributes: The attributes that will be defined, either as individual arguments or as a list
    :return: A set of model attributes, in the correct format to return from
             :py:meth:`TracModel.define_attributes() <tracdap.rt.api.TracModel.define_attributes>`

    :type attributes: :py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>` |
                  List[:py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>`]
    :rtype: List[:py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>`]
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_attributes(*attributes)


def define_attribute(
        attr_name: str, attr_value: _tp.Any,
        attr_type: _tp.Optional[BasicType] = None,
        categorical: bool = False) \
        -> TagUpdate:

    """
    Define an individual model attribute

    .. note::
        This is an experimental API that is not yet stabilised, expect changes in future versions of TRAC

    Model attributes can be defined using this function or the shorthand alias :py:func:`A`.
    A name and value are always required to define an attribute.
    Attribute type is required for multivalued attributes but is optional otherwise.
    The categorical flag can be applied to STRING attributes to mark them as categorical.

    Model attributes can be passed to :py:func:`define_attributes`,
    either as individual arguments or as a list, to create the set of attributes for a model.

    :param attr_name: The attribute name
    :param attr_value: The attribute value (as a raw Python value)
    :param attr_type: The TRAC type for this attribute (optional, except for multivalued attributes)
    :param categorical: A flag to indicate whether this attribute is categorical
    :return: A model attribute, in the format understood by the TRAC platform

    :type attr_name: str
    :type attr_value: Any
    :type attr_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>` | None
    :type categorical: bool
    :rtype: :py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_attribute(attr_name, attr_value, attr_type, categorical)


def A(  # noqa
        attr_name: str, attr_value: _tp.Any,
        attr_type: _tp.Optional[BasicType] = None,
        categorical: bool = False) \
        -> TagUpdate:

    """
    Shorthand alias for :py:func:`define_attribute`

    .. note::
        This is an experimental API that is not yet stabilised, expect changes in future versions of TRAC

    :type attr_name: str
    :type attr_value: Any
    :type attr_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>` | None
    :type categorical: bool
    :rtype: :py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>`
    """

    return define_attribute(attr_name, attr_value, attr_type, categorical)


def define_parameter(
        param_name: str, param_type: _tp.Union[BasicType, TypeDescriptor],
        label: str, default_value: _tp.Optional[_tp.Any] = None,
        *, param_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
        -> _Named[ModelParameter]:

    """
    Define an individual model parameter

    Model parameters can be defined using this method or the shorthand alias :py:func:`P`.
    Name, type and label are always required to define a parameter. The parameter name
    is used to set up parameters in a job and to access parameter values at runtime using
    :py:meth:`TracContext.get_parameter() <tracdap.rt.api.TracContext.get_parameter>`.

    Use the label property to add a descriptive label to a model parameter. If a default value
    is specified, the model parameter becomes optional. It is ok to omit optional parameters
    when running models or setting up jobs, in which case the default value will be used.
    If no default is specified then the model parameter becomes mandatory, a value must always
    be supplied in order to execute the model. TRAC will apply type coercion where possible to
    ensure the default value matches the parameter type, if the default value cannot be coerced
    to match the parameter type then model validation will fail.

    You can use param_props to associate arbitrary key-value properties with this model parameter.
    These properties are not used by the TRAC engine, but are stored in the model metadata for
    the parameter and can be used as needed in 3rd-party applications.

    Model parameters can be passed to :py:func:`define_parameters`,
    either as individual arguments or as a list, to create the set of parameters for a model.

    :param param_name: The parameter name, used to identify the parameter in code (must be a valid identifier)
    :param param_type: The parameter type, expressed in the TRAC type system
    :param label: A descriptive label for the parameter (required)
    :param default_value: A default value to use if no explicit value is supplied (optional)
    :param param_props: Associate key-value properties with this parameter (not used by the TRAC engine)
    :return: A named model parameter, suitable for passing to :py:func:`define_parameters`

    :type param_name: str
    :type param_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>` |
                      :py:class:`TypeDescriptor <tracdap.rt.metadata.TypeDescriptor>`

    :type label: str
    :type default_value: Any | None
    :type param_props: Dict[str, Any] | None
    :rtype: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_parameter(param_name, param_type, label, default_value, param_props=param_props)



def P(  # noqa
        param_name: str,
        param_type: _tp.Union[BasicType, TypeDescriptor],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None,
        *, param_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
        -> _Named[ModelParameter]:

    """
    Shorthand alias for :py:func:`define_parameter`

    :type param_name: str
    :type param_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>` |
                      :py:class:`TypeDescriptor <tracdap.rt.metadata.TypeDescriptor>`
    :type label: str
    :type default_value: Any | None
    :type param_props: Dict[str, Any] | None

    :rtype: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

    return define_parameter(param_name, param_type, label, default_value, param_props=param_props)


def define_parameters(
        *parameters: _tp.Union[_Named[ModelParameter], _tp.List[_Named[ModelParameter]]]) \
        -> _tp.Dict[str, ModelParameter]:

    """
    Defined the set of parameters used by a model

    Model parameters can be defined using :py:func:`define_parameter` or the shorthand alias :py:func:`P`.
    This function takes a number of parameters, either as individual arguments or as a list,
    and arranges them in the format required by
    :py:meth:`TracModel.define_parameters() <tracdap.rt.api.TracModel.define_parameters>`

    :param parameters: The parameters that will be defined, either as individual arguments or as a list
    :return: A set of model parameters, in the correct format to return from
             :py:meth:`TracModel.define_parameters() <tracdap.rt.api.TracModel.define_parameters>`

    :type parameters: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`] |
                  List[_Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]]
    :rtype: Dict[str, :py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_parameters(*parameters)


def define_field(
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        not_null: bool = False,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """
    Define an individual field for use in a schema

    Individual fields in a schema can be defined using this method or the shorthand alias :py:func:`F`.
    The name, type and label of a field are always required.
    The business_key, categorical and not_null flags are false by default.
    TRAC will always set not_null = True for business keys, even if not_null = False is passed explicitly.
    Explicitly specifying not_null=False for a business key will cause a validation error.
    Format code is optional.

    So long as field order is not specified for any field in a schema, field ordering will
    be assigned automatically. If field orders are specified explicitly, the fields in a schema
    must have a contiguous ordering starting at 0. When updating a model it is good practice
    to leave existing fields in order and add any new fields to the end of the list.

    Schema fields can be passed to :py:func:`define_schema`, either as individual arguments or as a list,
    to create a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`. To define the
    inputs or outputs of a :py:class:`TracModel <tracdap.rt.api.TracModel>`, fields can also be
    passed directly to :py:func:`define_input_table` or :py:func:`define_output_table`.

    :param field_name: The field's name, used as the field identifier in code and queries (must be a valid identifier)
    :param field_type: The data type of the field, only primitive types are allowed
    :param label: A descriptive label for the field (required)
    :param business_key: Flag indicating whether this field is a business key for its dataset (default: False)
    :param categorical: Flag indicating whether this is a categorical field (default: False)
    :param not_null: Whether this field has a not null constraint (default: False)
    :param format_code: A code that can be interpreted by client applications to format the field (optional)
    :param field_order: Explicit field ordering (optional)
    :return: A field schema, suitable for use in a schema definition

    :type field_name: str
    :type field_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type business_key: bool
    :type categorical: bool
    :type not_null: bool
    :type format_code: str | None
    :type field_order: int | None
    :rtype: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`
    """

    sa = _StaticApiHook.get_instance()

    return sa.define_field(
        field_name, field_type, label,
        business_key, categorical, not_null,
        format_code, field_order)


def F(  # noqa
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        not_null: bool = False,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """
    Shorthand alias for :py:func:`define_field`

    :type field_name: str
    :type field_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type business_key: bool
    :type categorical: bool
    :type not_null: bool
    :type format_code: str | None
    :type field_order: int | None
    :rtype: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`
    """

    return define_field(
        field_name, field_type, label,
        business_key, categorical, not_null,
        format_code, field_order)


def define_schema(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]],
        schema_type: SchemaType = SchemaType.TABLE_SCHEMA,
        dynamic: bool = False) \
        -> SchemaDefinition:

    """
    Create a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` from a list of fields

    Individual fields can be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.
    This function takes a number of fields, either as individual arguments or as a list, and arranges
    them into a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`.

    A schema type can be specified explicitly using the schema_type parameter,
    if unspecified the default is :py:attr:`TABLE_SCHEMA <tracdap.rt.metadata.SchemaType.TABLE_SCHEMA>`.

    If the schema is not known in advance, setting dynamic = True will create a schema that is resolved
    at runtime. Dynamic schemas still have a :py:class:`SchemaType <tracdap.rt.metadata.SchemaType>`,
    so e.g. :py:attr:`STRUCT_SCHEMA <tracdap.rt.metadata.SchemaType.STRUCT_SCHEMA>` and
    :py:attr:`TABLE_SCHEMA <tracdap.rt.metadata.SchemaType.TABLE_SCHEMA>` and cannot be used
    interchangeably at runtime. A schema marked as dynamic must not include any fields.

    :param fields: The list of fields to include in the schema
    :param schema_type: The type of schema to create (currently only TABLE schemas are supported)
    :param dynamic: Define a dynamic schema (fields list should be empty)
    :return: A schema definition built from the supplied fields

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :type schema_type: :py:class:`SchemaType <tracdap.rt.metadata.SchemaType>`
    :type dynamic: bool
    :rtype: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_schema(*fields, schema_type=schema_type, dynamic=dynamic)


def define_table(*fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) -> SchemaDefinition:

    """
    Define a :py:attr:`TABLE_SCHEMA <tracdap.rt.metadata.SchemaType.TABLE_SCHEMA>`from a list of fields

    Individual fields can be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.
    This function takes a number of fields, either as individual arguments or as a list, and arranges
    them into a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`.

    TRAC performs validation on the supplied fields and then creates a matching
    :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`.
    To create model inputs and outputs for this schema, use
    :py:func:`define_input() <tracdap.rt.api.define_input>` and
    :py:func:`define_output() <tracdap.rt.api.define_output>`.

    :param fields: The list of fields to include in the schema
    :return: A schema definition built from the supplied fields

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :rtype: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_schema(*fields, schema_type=SchemaType.TABLE_SCHEMA, dynamic=False)


def define_struct(struct_type: _tp.Type) -> SchemaDefinition:

    """
    Define a :py:attr:`STRUCT_SCHEMA <tracdap.rt.metadata.SchemaType.STRUCT_SCHEMA>` from a Python
    class, which can be either a *dataclass* or a Pydantic model.

    TRAC performs validation on the supplied type and then creates a matching
    :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`.
    To create model inputs and outputs for this schema, use
    :py:func:`define_input() <tracdap.rt.api.define_input>` and
    :py:func:`define_output() <tracdap.rt.api.define_output>`.
    To access these inputs and outputs at runtime, use
    :py:meth:`get_struct() <tracdap.rt.api.TracContext.get_struct>` and
    :py:meth:`put_struct() <tracdap.rt.api.TracContext.put_struct>`.

    :param struct_type: The dataclass or Pydantic model type to build a schema for
    :return: A schema definition built from the supplied Python type

    :type struct_type: type
    :rtype: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_struct(struct_type)


def define_file_type(extension: str, mime_type: str) -> FileType:

    """
    Define a :py:class:`FileType <tracdap.rt.metadata.FileType>` for a given extension and mime type.

    :type extension: str
    :type mime_type: str
    :rtype: :py:class:`FileType <tracdap.rt.metadata.FileType>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_file_type(extension, mime_type)


def define_input(
        requirement: _tp.Union[SchemaDefinition, FileType], *,
        label: _tp.Optional[str] = None,
        optional: bool = False, dynamic: bool = False,
        input_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None):

    """
    Define a model input, which can be any type of dataset or file

    :type requirement: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` | :py:class:`FileType <tracdap.rt.metadata.FileType>`
    :type label: str | None
    :type optional: bool
    :type dynamic: bool
    :type input_props: dict[str, :py:class:`Value <tracdap.rt.metadata.Value>`
    :rtype: :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_input(requirement, label=label, optional=optional, dynamic=dynamic, input_props=input_props)


def define_output(
        requirement: _tp.Union[SchemaDefinition, FileType], *,
        label: _tp.Optional[str] = None,
        optional: bool = False, dynamic: bool = False,
        output_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None):

    """
    Define a model output, which can be any type of dataset or file

    :type requirement: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` | :py:class:`FileType <tracdap.rt.metadata.FileType>`
    :type label: str | None
    :type optional: bool
    :type dynamic: bool
    :type output_props: dict[str, :py:class:`Value <tracdap.rt.metadata.Value>`
    :rtype: :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_output(requirement, label=label, optional=optional, dynamic=dynamic, output_props=output_props)


def define_input_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]],
        label: _tp.Optional[str] = None, optional: bool = False, dynamic: bool = False,
        input_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
        -> ModelInputSchema:

    """
    Define a model input for a :py:attr:`TABLE_SCHEMA <tracdap.rt.metadata.SchemaType.TABLE_SCHEMA>`.

    Individual fields can be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.
    This function takes a number of fields, either as individual arguments or as a list, and uses them
    to create a :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`.

    Use the label property to add a descriptive label to a model input. Inputs can be marked as
    optional in which case they are not required when running a job, use
    :py:meth:`TracContext.has_dataset() <tracdap.rt.api.TracContext.has_dataset>` to determine
    whether an optional input has been provided. Inputs can be marked as dynamic in which
    case the schema is not defined until the model runs, use
    :py:meth:`TracContext.get_schema() <tracdap.rt.api.TracContext.get_schema>` to get the schema
    of a dynamic input.

    You can use input_props to associate arbitrary key-value properties with this model input.
    These properties are not used by the TRAC engine, but are stored in the model metadata for
    the input and can be used as needed in 3rd-party applications.

    :param fields: A set of fields to make up a table schema
    :param label: Human readable label for the model input
    :param optional: Whether this model input is optional (default = False)
    :param dynamic: Whether this model input has a dynamic schema (the list of fields must be empty)
    :param input_props: An optional set of user-defined properties to set on the model input
    :return: A valid output schema suitable to return from :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :type label: str | None
    :type optional: bool
    :type dynamic: bool
    :type input_props: Dict[str, Any] | None
    :rtype: :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    """

    schema = define_schema(*fields, schema_type=SchemaType.TABLE, dynamic=dynamic)
    return define_input(schema, label=label, optional=optional, dynamic=dynamic, input_props=input_props)


def define_input_struct(
        struct_type: _tp.Type, *,
        label: _tp.Optional[str] = None, optional: bool = False,
        input_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) -> ModelInputSchema:

    """
    Define a model input for a :py:attr:`STRUCT_SCHEMA <tracdap.rt.metadata.SchemaType.STRUCT_SCHEMA>`.
    Validation is performed and the result is a :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    suitable to return from :py:meth:`define_inputs() <tracdap.rt.api.TracModel.define_inputs>`.

    This method is equivalent to calling :py:func:`define_struct() <tracdap.rt.api.define_struct>`
    and then :py:func:`define_input() <tracdap.rt.api.define_input>`.

    :param struct_type: The dataclass or Pydantic model type to build a struct schema for
    :param label: Human readable label for the model input
    :param optional: Whether this model input is optional (default = False)
    :param input_props: An optional set of user-defined properties to set on the model input
    :return: A valid input schema suitable to return from :py:meth:`define_inputs() <tracdap.rt.api.TracModel.define_inputs>`

    :type struct_type: type
    :type label: str | None
    :type optional: bool
    :type input_props: dict[str, any] | None
    :rtype: :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    """

    schema = define_struct(struct_type)
    return define_input(schema, label=label, optional=optional, input_props=input_props)


def define_input_file(
        extension: str, mime_type: str, *,
        label: _tp.Optional[str] = None, optional: bool = False,
        input_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
        -> ModelInputSchema:

    """
    Define a model input for a :py:class:`FileType <tracdap.rt.metadata.FileType>`

    :type extension str
    :type mime_type: sr
    :type label: str | None
    :type optional: bool
    :type input_props: dict[str, Any] | None
    :rtype: :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    """

    file_type = define_file_type(extension, mime_type)
    return define_input(file_type, label=label, optional=optional, input_props=input_props)


def define_output_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]],
        label: _tp.Optional[str] = None, optional: bool = False, dynamic: bool = False,
        output_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
        -> ModelOutputSchema:

    """
    Define a model output for a :py:attr:`TABLE_SCHEMA <tracdap.rt.metadata.SchemaType.TABLE_SCHEMA>`.

    Individual fields can be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.
    This function takes a number of fields, either as individual arguments or as a list, and uses them
    to create a :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`.

    Use the label property to add a descriptive label to a model output. Outputs can be marked as
    optional, a model can decide not to provide an optional output without causing an error.
    Outputs can be marked as dynamic in which case the schema is not defined until the model runs, use
    :py:meth:`TracContext.put_schema() <tracdap.rt.api.TracContext.put_schema>` to set the schema
    of a dynamic output before saving it.

    You can use output_props to associate arbitrary key-value properties with this model output.
    These properties are not used by the TRAC engine, but are stored in the model metadata for
    the output and can be used as needed in 3rd-party applications.

    :param fields: A set of fields to make up a table schema
    :param label: Human readable label for the model output
    :param optional: Whether this model output is optional (default = False)
    :param dynamic: Whether this model output has a dynamic schema (the list of fields must be empty)
    :param output_props: An optional set of user-defined properties to set on the model output
    :return: A valid output schema suitable to return from :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :type label: str | None
    :type optional: bool
    :type dynamic: bool
    :type output_props: Dict[str, Any] | None
    :rtype: :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`
    """

    schema = define_schema(*fields, schema_type=SchemaType.TABLE, dynamic=dynamic)
    return define_output(schema, label=label, optional=optional, dynamic=dynamic, output_props=output_props)


def define_output_struct(
        struct_type: _tp.Type, *,
        label: _tp.Optional[str] = None, optional: bool = False,
        output_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) -> ModelOutputSchema:

    """
    Define a model output for a :py:attr:`STRUCT_SCHEMA <tracdap.rt.metadata.SchemaType.STRUCT_SCHEMA>`.
    Validation is performed and the result is a :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`
    suitable to return from :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`.

    This method is equivalent to calling :py:func:`define_struct() <tracdap.rt.api.define_struct>`
    and then :py:func:`define_output() <tracdap.rt.api.define_output>`.

    :param struct_type: The dataclass or Pydantic model type to build a struct schema for
    :param label: Human readable label for the model output
    :param optional: Whether this model output is optional (default = False)
    :param output_props: An optional set of user-defined properties to set on the model output
    :return: A valid output schema suitable to return from :py:meth:`define_outputs() <tracdap.rt.api.TracModel.define_outputs>`

    :type struct_type: type
    :type label: str | None
    :type optional: bool
    :type output_props: dict[str, any] | None
    :rtype: :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`
    """

    schema = define_struct(struct_type)
    return define_output(schema, label=label, optional=optional, output_props=output_props)


def define_output_file(
        extension: str, mime_type: str, *,
        label: _tp.Optional[str] = None, optional: bool = False,
        output_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
        -> ModelOutputSchema:

    """
    Define a model output for a :py:class:`FileType <tracdap.rt.metadata.FileType>`

    :type extension str
    :type mime_type: sr
    :type label: str | None
    :type optional: bool
    :type output_props: dict[str, Any] | None
    :rtype: :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`
    """

    file_type = define_file_type(extension, mime_type)
    return define_output(file_type, label=label, optional=optional, output_props=output_props)


def define_external_system(
        protocol: str, client_type: _tp.Type, *,
        sub_protocol: _tp.Optional[str] = None) -> ModelResource:

    """
    Define a model resource for an external system.

    :param protocol: The protocol for the external system
    :param client_type: The client type for the external system
    :param sub_protocol: If specified, the target resource must match both protocol and sub-protocol
    :return: A model resource suitable to return from :py:meth:`define_resources() <tracdap.rt.api.TracModel.define_resources>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_external_system(protocol, client_type, sub_protocol=sub_protocol)


def load_schema(
        package: _tp.Union[_ts.ModuleType, str], schema_file: str,
        schema_type: SchemaType = SchemaType.TABLE) \
        -> SchemaDefinition:

    """
    Load a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` from a CSV file in the model package

    The schema CSV file must contain the following columns:

    * field_name (string, required)
    * field_type (:py:class:`BasicType <tracdap.rt.metadata.BasicType>`, required)
    * label (string, required)
    * business_key (boolean, optional)
    * categorical (boolean, optional)
    * format_code (string, optional)

    Field ordering is assigned by the order the fields are listed in the CSV file.
    A schema type can be specified explicitly using the schema_type parameter, currently only
    :py:attr:`TABLE <tracdap.rt.metadata.SchemaType.TABLE>` is supported and this
    is also the default.

    .. note::
       To define the inputs or outputs of a :py:class:`TracModel <tracdap.rt.api.TracModel>`,
       a schema can be loaded with this function and used to construct a
       :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>` or
       :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`.

    :param package: Package (or package name) in the model repository that contains the schema file
    :param schema_file: Name of the schema file to load, which must be in the specified package
    :param schema_type: The type of schema to create (currently only TABLE schemas are supported)
    :return: A schema definition loaded from the schema file

    :type package: ModuleType | str
    :type schema_file: str
    :type schema_type: :py:class:`SchemaType <tracdap.rt.metadata.SchemaType>`
    :rtype: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.load_schema(package, schema_file, schema_type=schema_type)


def load_resource(
        package: _tp.Union[_ts.ModuleType, str], resource_file: str) \
        -> bytes:

    """
    Load a binary resource from the model repository

    Resources are files included in the Python package structure of the model repository.
    This method loads a binary resource file into memory as a bytes object.

    .. note::
        Checking large resource files into the model repository has a significant performance impact.
        For this reason, TRAC enforces a limit on the size of resource files. In production deployments
        of TRAC the resource size limit is set by the system administrator. The default limit is 256 KB.
        Larger files can be passed into a model as model inputs.

    :param package: The package to load resources from (either a module object or the package name)
    :param resource_file: The name of the resource file to load
    :return: The loaded resource as a bytes object

    :type package: ModuleType | str
    :type resource_file: str
    :rtype: bytes
    """

    sa = _StaticApiHook.get_instance()
    return sa.load_resource(package, resource_file)


def load_resource_stream(
        package: _tp.Union[_ts.ModuleType, str], resource_file: str) \
        -> _tp.ContextManager[_tp.BinaryIO]:

    """
    Load a binary resource from the model repository as a readable stream

    Resources are files included in the Python package structure of the model repository.
    This method makes a binary resource available as a readable byte stream.
    The stream can only be used in a Python `with` block.

    .. note::
        Checking large resource files into the model repository has a significant performance impact.
        For this reason, TRAC enforces a limit on the size of resource files. In production deployments
        of TRAC the resource size limit is set by the system administrator. The default limit is 256 KB.
        Larger files can be passed into a model as model inputs.

    :param package: The package to load resources from (either a module object or the package name)
    :param resource_file: The name of the resource file to load
    :return: A readable binary stream for the resource

    :type package: ModuleType | str
    :type resource_file: str
    :rtype: BinaryIO
    """

    sa = _StaticApiHook.get_instance()
    return sa.load_resource_stream(package, resource_file)


def load_text_resource(
        package: _tp.Union[_ts.ModuleType, str], resource_file: str,
        encoding: str = "utf-8") \
        -> str:

    """
    Load a text resource from the model repository

    Resources are files included in the Python package structure of the model repository.
    This method loads a text resource file into memory as a str object.
    The resource is loaded with utf-8 encoding by default,
    different encodings can be specified using the encoding parameter.

    .. note::
        Checking large resource files into the model repository has a significant performance impact.
        For this reason, TRAC enforces a limit on the size of resource files. In production deployments
        of TRAC the resource size limit is set by the system administrator. The default limit is 256 KB.
        Larger files can be passed into a model as model inputs.

    :param package: The package to load resources from (either a module object or the package name)
    :param resource_file: The name of the resource file to load
    :param encoding: Resource encoding (default: utf-8)
    :return: The loaded resource, as a string if text = True, otherwise as bytes

    :type package: ModuleType | str
    :type resource_file: str
    :type encoding: str
    :rtype: bytes
    """

    sa = _StaticApiHook.get_instance()
    return sa.load_text_resource(package, resource_file, encoding)


def load_text_resource_stream(
        package: _tp.Union[_ts.ModuleType, str], resource_file: str,
        encoding: str = "utf-8") \
        -> _tp.ContextManager[_tp.TextIO]:

    """
    Load a text resource from the model repository as a text stream

    Resources are files included in the Python package structure of the model repository.
    This method makes a binary resource available as a readable text stream.
    The resource is loaded with utf-8 encoding by default,
    different encodings can be specified using the encoding parameter.
    The stream can only be used in a Python `with` block.

    .. note::
        Checking large resource files into the model repository has a significant performance impact.
        For this reason, TRAC enforces a limit on the size of resource files. In production deployments
        of TRAC the resource size limit is set by the system administrator. The default limit is 256 KB.
        Larger files can be passed into a model as model inputs.

    :param package: The package to load resources from (either a module object or the package name)
    :param resource_file: The name of the resource file to load
    :param encoding: Resource encoding (default: utf-8)
    :return: A readable text stream for the resource

    :type package: ModuleType | str
    :type resource_file: str
    :type encoding: str
    :rtype: TextIO
    """

    sa = _StaticApiHook.get_instance()
    return sa.load_text_resource_stream(package, resource_file, encoding)


def ModelInputSchema(  # noqa
        schema: SchemaDefinition,
        label: _tp.Optional[str] = None,
        optional: bool = False,
        dynamic: bool = False,
        inputProps: _tp.Optional[_tp.Dict[str, Value]] = None):  # noqa

    """
    .. deprecated:: 0.8.0
       Use :py:func:`define_input` instead.

    This function is provided for compatibility with TRAC versions before 0.8.0.
    Please use :py:func:`define_input() <tracdap.rt.api.define_input>` instead.

    :display: False
    """

    input_props = inputProps or dict()
    return define_input(schema, label=label, optional=optional, dynamic=dynamic, input_props=input_props)


def ModelOutputSchema(  # noqa
        schema: SchemaDefinition,
        label: _tp.Optional[str] = None,
        optional: bool = False,
        dynamic: bool = False,
        outputProps: _tp.Optional[_tp.Dict[str, Value]] = None):  # noqa

    """
    .. deprecated:: 0.8.0
       Use :py:func:`define_output` instead.

    This function is provided for compatibility with TRAC versions before 0.8.0.
    Please use :py:func:`define_output() <tracdap.rt.api.define_output>` instead.

    :display: False
    """

    output_props = outputProps or dict()
    return define_output(schema, label=label, optional=optional, dynamic=dynamic, output_props=output_props)



def declare_parameter(
        param_name: str,
        param_type: _tp.Union[BasicType, TypeDescriptor],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[ModelParameter]:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_parameter` or :py:func:`P` instead.

    This function is deprecated and will be removed in a future version.
    Please use :py:func:`define_parameter() <tracdap.rt.api.define_parameter>` instead.

    :type param_name: str
    :type param_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>` |
                      :py:class:`TypeDescriptor <tracdap.rt.metadata.TypeDescriptor>`

    :type label: str
    :type default_value: Any | None
    :rtype: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]

    :display: False
    """

    print("TRAC Warning: declare_parameter() is deprecated, please use define_parameter()", file=sys.stderr)

    return define_parameter(param_name, param_type, label, default_value)


def declare_parameters(
        *params: _tp.Union[_Named[ModelParameter], _tp.List[_Named[ModelParameter]]]) \
        -> _tp.Dict[str, ModelParameter]:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_parameters` instead

    This function is deprecated and will be removed in a future version.
    Please use :py:func:`define_parameters() <tracdap.rt.api.define_parameters>` instead.

    :type params: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`] |
                  List[_Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]]
    :rtype: Dict[str, :py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]

    :display: False
    """

    print("TRAC Warning: declare_parameters() is deprecated, please use define_parameters()", file=sys.stderr)

    return define_parameters(*params)


def declare_field(
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        not_null: bool = False,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_field` or :py:func:`F` instead.

    This function is deprecated and will be removed in a future version.
    Please use :py:func:`define_field() <tracdap.rt.api.define_field>` instead.

    :type field_name: str
    :type field_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type business_key: bool
    :type categorical: bool
    :type not_null: bool
    :type format_code: str | None
    :type field_order: int | None
    :rtype: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`

    :display: False
    """

    print("TRAC Warning: declare_field() is deprecated, please use define_field()", file=sys.stderr)

    return define_field(
        field_name, field_type, label,
        business_key, categorical, not_null,
        format_code, field_order)


def declare_input_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelInputSchema:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_input_table` instead.

    This function is deprecated and will be removed in a future version.
    Please use :py:func:`define_input_table() <tracdap.rt.api.define_input_table>` instead.

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :rtype: :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`

    :display: False
    """

    print("TRAC Warning: declare_input_table() is deprecated, please use define_input_table()", file=sys.stderr)

    return define_input_table(*fields)


def declare_output_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelOutputSchema:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_output_table` instead.

    This function is deprecated and will be removed in a future version.
    Please use :py:func:`define_output_table() <tracdap.rt.api.define_output_table>` instead.

    This function is deprecated and will be removed in a future version.
    Please use define_output_table() instead.

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :rtype: :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`

    :display: False
    """

    print("TRAC Warning: declare_output_table() is deprecated, please use define_output_table()", file==sys.stderr)

    return define_output_table(*fields)
