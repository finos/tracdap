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
import dataclasses as _dc
import typing as _tp

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex


_T = _tp.TypeVar("_T")


# Utility class for passing named items between define_ funcs
@_dc.dataclass
class Named(_tp.Generic[_T]):

    itemName: str
    item: _T


class RuntimeHook:

    __runtime_hook: RuntimeHook = None

    @classmethod
    def _is_registered(cls) -> bool:
        return cls.__runtime_hook is not None

    @classmethod
    def _register(cls, hook: RuntimeHook):

        if cls._is_registered():
            raise _ex.ETracInternal(f"TRAC runtime API initialized twice")

        cls.__runtime_hook = hook

    @classmethod
    def runtime(cls) -> RuntimeHook:

        if not cls._is_registered():
            raise _ex.ETracInternal(f"TRAC runtime API is not initialized")

        return cls.__runtime_hook

    @_abc.abstractmethod
    def define_parameter(
            self, param_name: str, param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
            label: str, default_value: _tp.Optional[_tp.Any] = None) \
            -> Named[_meta.ModelParameter]:

        pass

    @_abc.abstractmethod
    def define_parameters(
            self, *params: _tp.Union[Named[_meta.ModelParameter], _tp.List[Named[_meta.ModelParameter]]]) \
            -> _tp.Dict[str, _meta.ModelParameter]:

        pass

    @_abc.abstractmethod
    def define_field(
            self, field_name: str, field_type: _meta.BasicType, label: str,
            business_key: bool = False, categorical: bool = False,
            format_code: _tp.Optional[str] = None, field_order: _tp.Optional[int] = None) \
            -> _meta.FieldSchema:

        pass

    @_abc.abstractmethod
    def define_input_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelInputSchema:

        pass

    @_abc.abstractmethod
    def define_output_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelOutputSchema:

        pass
