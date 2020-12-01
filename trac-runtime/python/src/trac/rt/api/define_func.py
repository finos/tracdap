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


import typing as tp
from trac.rt.metadata import *


def define_parameter(
        label: str,
        param_type: tp.Union[TypeDescriptor, BasicType],
        default_value: tp.Optional[tp.Any] = None):

    if isinstance(param_type, TypeDescriptor):
        param_type_descriptor = param_type
    else:
        param_type_descriptor = TypeDescriptor(param_type, None, None)

    return ModelParameter(label, param_type_descriptor, default_value)


def define_table(*args, **kwargs):
    return TableDefinition(*args, **kwargs)


def define_field(*args, **kwargs):
    return FieldDefinition(*args, **kwargs)


P = define_parameter
F = define_field
