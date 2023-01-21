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
import types as _ts

from .hook import _StaticApiHook
from .hook import _Named

# Import metadata domain objects into the API namespace
# This significantly improves type hinting, inline documentation and auto-complete in JetBrains IDEs
from tracdap.rt.metadata import *  # DOCGEN_REMOVE


def define_attributes(*attrs: _tp.Union[TagUpdate, _tp.List[TagUpdate]]) -> _tp.List[TagUpdate]:

    """
    Defined a set of attributes to catalogue and describe a model

    .. note::
        This is an experimental API that is not yet stabilised, expect changes in future versions of TRAC

    Attributes can be supplied either as individual arguments to this function or as a list.
    In either case, each attribute should be defined using :py:func:`define_attribute`
    (or :py:func:`trac.A <tracdap.rt.api.A>`).

    :param attrs: The attributes that will be defined, either as individual arguments or as a list
    :return: A set of model attributes, in the correct format to return from
             :py:meth:`TracModel.define_attributes`

    :type attrs: :py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>` |
                  List[:py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>`]
    :rtype: List[:py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>`]
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_attributes(*attrs)


def define_attribute(
        attr_name: str, attr_value: _tp.Any,
        attr_type: _tp.Optional[BasicType] = None,
        categorical: bool = False) \
        -> TagUpdate:

    """
    Define an individual model attribute

    .. note::
        This is an experimental API that is not yet stabilised, expect changes in future versions of TRAC

    Model attributes can be defined using this method (or :py:func:`trac.A <A>`).
    The attr_name and attr_value are always required to define an attribute.
    attr_type is always required for multivalued attributes but is optional otherwise.
    The categorical flag can be applied to STRING attributes if required.

    Once defined attributes can be passed to :py:func:`define_attributes`,
    either as a list or as individual arguments, to create the set of attributes for a model.

    :param attr_name: The attribute name
    :param attr_value: The attribute value (as a raw Python value)
    :param attr_type: The TRAC type for this attribute (optional, except for multivalued attributes)
    :param categorical: A flag to indicate whether this attribute is categorical
    :return: An attribute for the model, ready for loading into the TRAC platform

    :type attr_name: str
    :type attr_value: Any
    :type attr_type: Optional[:py:class:`BasicType <tracdap.rt.metadata.BasicType>`]
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
    :type attr_type: Optional[:py:class:`BasicType <tracdap.rt.metadata.BasicType>`]
    :type categorical: bool
    :rtype: :py:class:`TagUpdate <tracdap.rt.metadata.TagUpdate>`
    """

    return define_attribute(attr_name, attr_value, attr_type, categorical)


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

    :type param_name: str
    :type param_type: :py:class:`TypeDescriptor <tracdap.rt.metadata.TypeDescriptor>` |
                      :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type default_value: Optional[Any]
    :rtype: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_parameter(param_name, param_type, label, default_value)


def declare_parameter(
        param_name: str,
        param_type: _tp.Union[TypeDescriptor, BasicType],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[ModelParameter]:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_parameter` or :py:func:`P` instead.

    :type param_name: str
    :type param_type: :py:class:`TypeDescriptor <tracdap.rt.metadata.TypeDescriptor>` |
                      :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type default_value: Optional[Any]
    :rtype: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

    return define_parameter(param_name, param_type, label, default_value)


def P(  # noqa
        param_name: str,
        param_type: _tp.Union[TypeDescriptor, BasicType],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[ModelParameter]:

    """
    Shorthand alias for :py:func:`define_parameter`

    :type param_name: str
    :type param_type: :py:class:`TypeDescriptor <tracdap.rt.metadata.TypeDescriptor>` |
                      :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type default_value: Optional[Any]
    :rtype: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

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
    :return: A set of model parameters, in the correct format to return from
             :py:meth:`TracModel.define_parameters`

    :type params: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`] |
                  List[_Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]]
    :rtype: Dict[str, :py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_parameters(*params)


def declare_parameters(
        *params: _tp.Union[_Named[ModelParameter], _tp.List[_Named[ModelParameter]]]) \
        -> _tp.Dict[str, ModelParameter]:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_parameters` instead.

    :type params: _Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`] |
                  List[_Named[:py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]]
    :rtype: Dict[str, :py:class:`ModelParameter <tracdap.rt.metadata.ModelParameter>`]
    """

    return define_parameters(*params)


def define_field(
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        not_null: _tp.Optional[bool] = None,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """
    Define the schema for an individual field, which can be used in a model input or output schema.

    Individual fields in a dataset can be defined using this method or the shorthand alias :py:func:`F`.
    The name, type and label of a field are always required.
    The business_key and categorical flags are false by default.
    The not_null flag is false by default unless the field is a business key, in which case it is true by default.
    Explicitly specifying not_null=False for a business key will cause a validation error.
    Format code is optional.

    If no field ordering is supplied, fields will automatically be assigned a contiguous ordering starting at 0.
    In this case care must be taken when creating an updated version of a model, that the order of existing
    fields is not disturbed. Adding fields to the end of a list is always safe.
    If field orders are specified explicitly, they must form a contiguous ordering starting at 0.

    Once defined field schemas can be passed to :py:func:`define_input_table` or :py:func:`define_output_table`,
    either as a list or as individual arguments, to create the full schema for an input or output.

    :param field_name: The field's name, used as the field identifier in code and queries (must be a valid identifier)
    :param field_type: The data type of the field, only primitive types are allowed
    :param label: A descriptive label for the field (required)
    :param business_key: Flag indicating whether this field is a business key for its dataset (default: False)
    :param categorical: Flag indicating whether this is a categorical field (default: False)
    :param not_null: Whether this field has a not null constraint (default: True for business keys, false otherwise)
    :param format_code: A code that can be interpreted by client applications to format the field (optional)
    :param field_order: Explicit field ordering (optional)
    :return: A field schema, suitable for use in a schema definition

    :type field_name: str
    :type field_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type business_key: bool
    :type categorical: bool
    :type not_null: _tp.Optional[bool]
    :type format_code: _tp.Optional[str]
    :type field_order: _tp.Optional[int]
    :rtype: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`
    """

    sa = _StaticApiHook.get_instance()

    return sa.define_field(
        field_name, field_type, label,
        business_key, categorical, not_null,
        format_code, field_order)


def declare_field(
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        not_null: _tp.Optional[bool] = None,
        format_code: _tp.Optional[str] = None,
        field_order: _tp.Optional[int] = None) \
        -> FieldSchema:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_field` or :py:func:`F` instead.

    :type field_name: str
    :type field_type: :py:class:`BasicType <tracdap.rt.metadata.BasicType>`
    :type label: str
    :type business_key: bool
    :type categorical: bool
    :type not_null: _tp.Optional[bool]
    :type format_code: _tp.Optional[str]
    :type field_order: _tp.Optional[int]
    :rtype: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`
    """

    return define_field(
        field_name, field_type, label,
        business_key, categorical, not_null,
        format_code, field_order)


def F(  # noqa
        field_name: str,
        field_type: BasicType,
        label: str,
        business_key: bool = False,
        categorical: bool = False,
        not_null: _tp.Optional[bool] = None,
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
    :type not_null: _tp.Optional[bool]
    :type format_code: _tp.Optional[str]
    :type field_order: _tp.Optional[int]
    :rtype: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`
    """

    return define_field(
        field_name, field_type, label,
        business_key, categorical, not_null,
        format_code, field_order)


def define_schema(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]],
        schema_type: SchemaType = SchemaType.TABLE) \
        -> SchemaDefinition:

    """
    Create a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` from a list of fields.

    Fields can be supplied either as individual arguments to this function or as a list.
    Individual fields should be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.
    Schema type can be specified using the schema_type parameter, currently only TABLE schemas are supported.

    Model inputs and outputs must be specified as :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    and :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>` respectively. The input/output schema
    classes both require a schema definition than can be created with this method. Alternatively, you can use
    :py:func:`define_input_table` or :py:func:`define_output_table` to create the input/output schema classes directly.


    :param fields: The list of fields to include in the schema
    :param schema_type: The type of schema to create (currently only TABLE schemas are supported)
    :return: A schema definition built from the supplied fields and schema type

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :type schema_type: :py:class:`SchemaType <tracdap.rt.metadata.SchemaType>`
    :rtype: :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_schema(*fields, schema_type=schema_type)


def load_schema(
        package: _tp.Union[_ts.ModuleType, str], schema_file: str,
        schema_type: SchemaType = SchemaType.TABLE) \
        -> SchemaDefinition:

    """
    load a :py:class:`SchemaDefinition <tracdap.rt.metadata.SchemaDefinition>` from a CSV file or package resource.

    The schema CSV file must contain the following columns:

    * field_name (string, required)
    * field_type (:py:class:`BasicType <tracdap.rt.metadata.BasicType>`, required)
    * label (string, required)
    * business_key (boolean, optional)
    * categorical (boolean, optional)
    * format_code (string, optional)

    Field order is taken from the order in which the fields are listed.
    Schema type can be specified using the schema_type parameter, currently only TABLE schemas are supported.

    Model inputs and outputs must be specified as :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    and :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>` respectively. The input/output schema
    classes both require a schema definition than can be created with this method.

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


def define_input_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelInputSchema:

    """
    Define a model input with a table schema.

    Fields can be supplied either as individual arguments to this function or as a list.
    Individual fields should be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.

    :param fields: A set of fields to make up a :py:class:`TableSchema <tracdap.rt.metadata.TableSchema>`
    :return: A model input schema, suitable for returning from :py:meth:`TracModel.define_inputs`

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :rtype: :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_input_table(*fields)


def declare_input_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelInputSchema:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_input_table` instead.

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :rtype: :py:class:`ModelInputSchema <tracdap.rt.metadata.ModelInputSchema>`
    """

    return define_input_table(*fields)


def define_output_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelOutputSchema:

    """
    Define a model output with a table schema.

    Fields can be supplied either as individual arguments to this function or as a list.
    Individual fields should be defined using :py:func:`define_field` or the shorthand alias :py:func:`F`.

    :param fields: A set of fields to make up a :py:class:`TableSchema <tracdap.rt.metadata.TableSchema>`
    :return: A model output schema, suitable for returning from :py:meth:`TracModel.define_outputs`

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :rtype: :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`
    """

    sa = _StaticApiHook.get_instance()
    return sa.define_output_table(*fields)


def declare_output_table(
        *fields: _tp.Union[FieldSchema, _tp.List[FieldSchema]]) \
        -> ModelOutputSchema:

    """
    .. deprecated:: 0.4.4
       Use :py:func:`define_output_table` instead.

    :type fields: :py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>` |
                  List[:py:class:`FieldSchema <tracdap.rt.metadata.FieldSchema>`]
    :rtype: :py:class:`ModelOutputSchema <tracdap.rt.metadata.ModelOutputSchema>`
    """

    return define_output_table(*fields)
