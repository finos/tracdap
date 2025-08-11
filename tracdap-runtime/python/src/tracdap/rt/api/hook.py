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

import abc as _abc
import dataclasses as _dc
import typing as _tp
import types as _ts

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex

# This module contains hooks for connecting the static API to the runtime implementation
# To avoid noise in the API package, everything in this package is named with an underscore


_T = _tp.TypeVar("_T")


# Utility class for passing named items between define_ funcs
@_dc.dataclass
class _Named(_tp.Generic[_T]):

    item_name: str
    item: _T


class _StaticApiHook(_abc.ABC):

    __static_api_hook: _StaticApiHook = None

    @classmethod
    def _is_registered(cls) -> bool:
        return cls.__static_api_hook is not None

    @classmethod
    def _register(cls, hook: _StaticApiHook):

        if cls._is_registered():
            raise _ex.ETracInternal(f"TRAC API hook registered twice")

        cls.__static_api_hook = hook

    @classmethod
    def get_instance(cls) -> _StaticApiHook:

        if not cls._is_registered():
            raise _ex.ETracInternal(f"TRAC API hook is not initialized")

        return cls.__static_api_hook

    @_abc.abstractmethod
    def array_type(self, item_type: _meta.BasicType) -> _meta.TypeDescriptor:

        pass

    @_abc.abstractmethod
    def map_type(self, entry_type: _meta.BasicType) -> _meta.TypeDescriptor:

        pass

    @_abc.abstractmethod
    def define_attributes(
            self, *attrs: _tp.Union[_meta.TagUpdate, _tp.List[_meta.TagUpdate]]) \
            -> _tp.List[_meta.TagUpdate]:

        pass

    @_abc.abstractmethod
    def define_attribute(
            self, attr_name: str, attr_value: _tp.Any,
            attr_type: _tp.Optional[_meta.BasicType] = None,
            categorical: bool = False) \
            -> _meta.TagUpdate:

        pass

    @_abc.abstractmethod
    def define_parameter(
            self, param_name: str, param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
            label: str, default_value: _tp.Optional[_tp.Any] = None,
            *, param_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
            -> _Named[_meta.ModelParameter]:

        pass

    @_abc.abstractmethod
    def define_parameters(
            self, *params: _tp.Union[_Named[_meta.ModelParameter], _tp.List[_Named[_meta.ModelParameter]]]) \
            -> _tp.Dict[str, _meta.ModelParameter]:

        pass

    @_abc.abstractmethod
    def define_field(
            self, field_name: str, field_type: _meta.BasicType, label: str,
            business_key: bool = False, categorical: bool = False, not_null: bool = False,
            format_code: _tp.Optional[str] = None, field_order: _tp.Optional[int] = None) \
            -> _meta.FieldSchema:

        pass

    @_abc.abstractmethod
    def define_schema(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]],
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE, dynamic: bool = False) \
            -> _meta.SchemaDefinition:

        pass

    @_abc.abstractmethod
    def define_struct(self, python_type: type) -> _meta.SchemaDefinition:

        pass

    @_abc.abstractmethod
    def infer_schema(self, dataset: _tp.Any) -> _meta.SchemaDefinition:

        pass

    @_abc.abstractmethod
    def define_file_type(self, extension: str, mime_type: str) -> _meta.FileType:

        pass

    @_abc.abstractmethod
    def define_input(
            self, requirement: _tp.Union[_meta.SchemaDefinition, _meta.FileType], *,
            label: _tp.Optional[str] = None,
            optional: bool = False, dynamic: bool = False,
            input_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
            -> _meta.ModelInputSchema:

        pass

    @_abc.abstractmethod
    def define_output(
            self, requirement: _tp.Union[_meta.SchemaDefinition, _meta.FileType], *,
            label: _tp.Optional[str] = None,
            optional: bool = False, dynamic: bool = False,
            output_props: _tp.Optional[_tp.Dict[str, _tp.Any]] = None) \
            -> _meta.ModelOutputSchema:

        pass

    @_abc.abstractmethod
    def load_schema(
            self, package: _tp.Union[_ts.ModuleType, str], schema_file: str,
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) \
            -> _meta.SchemaDefinition:

        pass

    @_abc.abstractmethod
    def load_resource(
            self, package: _tp.Union[_ts.ModuleType, str], resource_file: str) \
            -> bytes:

        pass

    @_abc.abstractmethod
    def load_resource_stream(
            self, package: _tp.Union[_ts.ModuleType, str], resource_file: str) \
            -> _tp.ContextManager[_tp.BinaryIO]:

        pass

    @_abc.abstractmethod
    def load_text_resource(
            self, package: _tp.Union[_ts.ModuleType, str], resource_file: str, encoding: str = "utf-8") \
            -> str:

        pass

    @_abc.abstractmethod
    def load_text_resource_stream(
            self, package: _tp.Union[_ts.ModuleType, str], resource_file: str, encoding: str = "utf-8") \
            -> _tp.ContextManager[_tp.TextIO]:

        pass


class _ApiContextHook(_abc.ABC):

    @_abc.abstractmethod
    def register_context_manager(self, name: str, open_func: _tp.Callable, close_func: _tp.Callable) -> int:
        pass

    @_abc.abstractmethod
    def enter_context_manager(self, hook_id: int):
        pass

    @_abc.abstractmethod
    def exit_context_manager(self, hook_id: int, exc_type, exc_val, exc_tb):
        pass


class _ApiContextWrapper(_tp.ContextManager[_T]):

    def __init__(self, hook: _ApiContextHook, delegate: _tp.ContextManager[_T], name: str):
        super().__init__()
        self.__hook = hook
        self.__hook_id = hook.register_context_manager(name, delegate.__enter__, delegate.__exit__)  # noqa

    def __enter__(self):
        return self.__hook.enter_context_manager(self.__hook_id)  # noqa

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__hook.exit_context_manager(self.__hook_id, exc_type, exc_val, exc_tb)  # noqa
