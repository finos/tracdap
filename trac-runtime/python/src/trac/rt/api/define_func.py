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

import typing as _tp
import dataclasses as _dc
import inspect as _inspect
import trac.rt.metadata as _meta


__field_def_params = _inspect.signature(_meta.FieldDefinition.__init__).parameters


_T = _tp.TypeVar("_T")


# Utility class for passing named items between define_ funcs
# Using a private class avoids creating noise in the doc gen / doc comments
@_dc.dataclass
class _Named(_tp.Generic[_T]):

    itemName: str
    item: _T


def define_parameters(
        *params: _tp.Union[_Named[_meta.ModelParameter], _tp.List[_Named[_meta.ModelParameter]]]) \
        -> _tp.Dict[str, _meta.ModelParameter]:

    if len(params) == 1 and isinstance(params[0], list):
        return {p.itemName: p.item for p in params[0]}
    else:
        return {p.itemName: p.item for p in params}


def define_parameter(
        param_name: str,
        param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
        label: str,
        default_value: _tp.Optional[_tp.Any] = None) \
        -> _Named[_meta.ModelParameter]:

    if isinstance(param_type, _meta.TypeDescriptor):
        param_type_descriptor = param_type
    else:
        param_type_descriptor = _meta.TypeDescriptor(param_type, None, None)

    return _Named(param_name, _meta.ModelParameter(label, param_type_descriptor, default_value))


def define_table(*fields: _meta.FieldDefinition):
    return _meta.TableDefinition([*fields])


def define_field(*args, **kwargs):

    arg_names = list(kwargs.keys())

    for arg_name in arg_names:

        if arg_name in __field_def_params:
            continue

        # Convert arg names starting with "field", e.g. label -> fieldLabel
        prefix_name = "field" + arg_name[0].upper() + arg_name[1:]

        if prefix_name in __field_def_params:
            kwargs[prefix_name] = kwargs[arg_name]
            kwargs.pop(arg_name)
            continue

        # Convert snake-case to camel-case
        camel_words = arg_name.split('_')
        camel_name = camel_words[0] + ''.join(word.title() for word in camel_words[1:])

        if camel_name in __field_def_params:
            kwargs[camel_name] = kwargs[arg_name]
            kwargs.pop(arg_name)

    return _meta.FieldDefinition(*args, **kwargs)


def P(*args, **kwargs):  # noqa
    """Shorthand alias for :py:func:`define_parameter`"""
    return define_parameter(*args, **kwargs)


def F(*args, **kwargs):  # noqa
    """Shorthand alias for :py:func:`define_field`"""
    return define_field(*args, **kwargs)
