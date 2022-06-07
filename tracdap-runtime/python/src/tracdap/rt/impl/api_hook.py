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

import inspect
import logging
import typing as _tp
import types as _ts
import pathlib as _path

import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.schemas as _schemas
import tracdap.rt.impl.util as _util

# Import hook interfaces into this module namespace
from tracdap.rt.api.hook import *


class ApiGuard:

    # The metaclass for generic types varies between versions of the typing library
    # To work around this, detect the correct metaclass by inspecting a generic type variable
    __generic_metaclass = type(_tp.List[object])

    _log: logging.Logger

    @classmethod
    def validate_signature(cls, method_name: str, method: inspect.Signature, *args, **kwargs):

        positional_index = 0

        for param_name, param in method.parameters.items():

            values = cls._select_arg(method_name, param, positional_index, *args, **kwargs)
            positional_index += len(values)

            for value in values:
                cls._validate_arg(method_name, param, value)

    @classmethod
    def _select_arg(
            cls, method_name: str, parameter: inspect.Parameter, positional_index,
            *args, **kwargs) -> _tp.List[_tp.Any]:

        if parameter.kind == inspect.Parameter.POSITIONAL_ONLY:

            if positional_index < len(args):
                return [args[positional_index]]

            else:
                err = f"Invalid API call [{method_name}()]: Missing required parameter [{parameter.name}]"
                cls._log.error(err)
                raise _ex.ERuntimeValidation(err)

        if parameter.kind == inspect.Parameter.KEYWORD_ONLY:

            if parameter.name in kwargs:
                return [kwargs[parameter.name]]

            else:
                err = f"Invalid API call [{method_name}()]: Missing required parameter [{parameter.name}]"
                cls._log.error(err)
                raise _ex.ERuntimeValidation(err)

        if parameter.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:

            if positional_index < len(args):
                return [args[positional_index]]

            elif parameter.name in kwargs:
                return [kwargs[parameter.name]]

            else:
                err = f"Invalid API call [{method_name}()]: Missing required parameter [{parameter.name}]"
                cls._log.error(err)
                raise _ex.ERuntimeValidation(err)

        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:

            if positional_index < len(args):
                return [args[i] for i in range(positional_index, len(args))]
            else:
                return []

        if parameter.kind == inspect.Parameter.VAR_KEYWORD:

            raise _ex.ETracInternal("Validation of VAR_KEYWORD params is not supported yet")

        raise _ex.EUnexpected("Invalid method signature in runtime API (this is a bug)")

    @classmethod
    def _validate_arg(cls, method_name: str, parameter: inspect.Parameter, value: _tp.Any):

        if not cls._validate_arg_inner(parameter.annotation, value):

            expected_type = cls._type_name(parameter.annotation)
            actual_type = cls._type_name(type(value)) if value is not None else str(None)

            err = f"Invalid API call [{method_name}()]: Wrong type for [{parameter.name}]" \
                  + f" (expected [{expected_type}], got [{actual_type}])"

            cls._log.error(err)
            raise _ex.ERuntimeValidation(err)

    @classmethod
    def _validate_arg_inner(cls, param_type: _tp.Type, value: _tp.Any) -> bool:

        if param_type == _tp.Any:
            return True

        if isinstance(param_type, cls.__generic_metaclass):

            origin = _util.get_origin(param_type)
            args = _util.get_args(param_type)

            # The generic type "Named" is defined in the TRAC API so needs to be supported
            # This type is used for passing named intermediate values between the define_ methods
            if origin is Named:

                named_type = args[0]
                return isinstance(value, Named) and isinstance(value.item, named_type)

            # _tp.Union also covers _tp.Optional, which is shorthand for _tp.Union[_type, None]
            if origin is _tp.Union:

                for union_type in args:
                    if cls._validate_arg_inner(union_type, value):
                        return True

                return False

            if origin is _tp.List:

                list_type = args[0]
                return isinstance(value, list) and all(map(lambda v: isinstance(v, list_type), value))

            raise _ex.ETracInternal(f"Validation of [{origin.__name__}] generic parameters is not supported yet")

        # Validate everything else as a concrete type

        return isinstance(value, param_type)

    @classmethod
    def _type_name(cls, type_var: _tp.Type) -> str:

        if isinstance(type_var, cls.__generic_metaclass):

            origin = _util.get_origin(type_var)
            args = _util.get_args(type_var)

            if origin is Named:
                named_type = cls._type_name(args[0])
                return f"Named[{named_type}]"

            if origin is _tp.Union:
                return "|".join(map(cls._type_name, args))

            if origin is _tp.List:
                list_type = cls._type_name(args[0])
                return f"List[{list_type}]"

            raise _ex.ETracInternal(f"Validation of [{origin.__name__}] generic parameters is not supported yet")

        return type_var.__name__


ApiGuard._log = _util.logger_for_class(ApiGuard)


class RuntimeHookImpl(RuntimeHook):  # noqa

    __define_parameter_signature: inspect.Signature
    __define_parameters_signature: inspect.Signature
    __define_field_signature: inspect.Signature
    __define_schema_signature: inspect.Signature
    __load_schema_signature: inspect.Signature
    __define_input_table_signature: inspect.Signature
    __define_output_table_signature: inspect.Signature

    def __init__(self):
        self._prepare_signatures()

    @classmethod
    def register_impl(cls):

        log = _util.logger_for_class(cls)

        if not RuntimeHook._is_registered():

            log.info("Registering runtime API hook...")
            RuntimeHook._register(RuntimeHookImpl())

        else:

            log.warning("Runtime API hook is already registered")

    def define_parameter(
            self, param_name: str, param_type: _tp.Union[_meta.TypeDescriptor, _meta.BasicType],
            label: str, default_value: _tp.Optional[_tp.Any] = None) \
            -> Named[_meta.ModelParameter]:

        ApiGuard.validate_signature(
            self.define_parameter.__name__, self.__define_parameter_signature,
            param_name, param_type, label, default_value)

        if isinstance(param_type, _meta.TypeDescriptor):
            param_type_descriptor = param_type
        else:
            param_type_descriptor = _meta.TypeDescriptor(param_type, None, None)

        return Named(param_name, _meta.ModelParameter(param_type_descriptor, label, default_value))

    def define_parameters(
            self, *params: _tp.Union[Named[_meta.ModelParameter], _tp.List[Named[_meta.ModelParameter]]]) \
            -> _tp.Dict[str, _meta.ModelParameter]:

        ApiGuard.validate_signature(
            self.define_parameters.__name__, self.__define_parameters_signature,
            *params)

        if len(params) == 1 and isinstance(params[0], list):
            return {p.itemName: p.item for p in params[0]}
        else:
            return {p.itemName: p.item for p in params}

    def define_field(
            self, field_name: str, field_type: _meta.BasicType, label: str,
            business_key: bool = False, categorical: bool = False,
            format_code: _tp.Optional[str] = None, field_order: _tp.Optional[int] = None) \
            -> _meta.FieldSchema:

        ApiGuard.validate_signature(
            self.define_field.__name__, self.__define_field_signature,
            field_name, field_type, label, business_key, categorical, format_code, field_order)

        return _meta.FieldSchema(
            field_name,
            field_order,
            field_type,
            label,
            businessKey=business_key,
            categorical=categorical,
            formatCode=format_code)

    def define_schema(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]],
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) \
            -> _meta.SchemaDefinition:

        ApiGuard.validate_signature(
            self.define_schema.__name__, self.__define_schema_signature,
            *fields, schema_type)

        if schema_type == _meta.SchemaType.TABLE:

            table_schema = self._build_table_schema(*fields)
            return _meta.SchemaDefinition(_meta.SchemaType.TABLE, table=table_schema)

        raise _ex.ERuntimeValidation(f"Invalid schema type [{schema_type.name}]")

    def load_schema(
            self, package: _tp.Union[_ts.ModuleType, str], schema_file: _tp.Union[str, _path.Path],
            schema_type: _meta.SchemaType = _meta.SchemaType.TABLE) \
            -> _meta.SchemaDefinition:

        ApiGuard.validate_signature(
            self.load_schema.__name__, self.__load_schema_signature,
            package, schema_file, schema_type)

        return _schemas.SchemaLoader.load_schema(package, schema_file)

    def define_input_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelInputSchema:

        ApiGuard.validate_signature(
            self.define_input_table.__name__, self.__define_input_table_signature,
            *fields)

        schema_def = self.define_schema(*fields, schema_type=_meta.SchemaType.TABLE)
        return _meta.ModelInputSchema(schema=schema_def)

    def define_output_table(
            self, *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.ModelOutputSchema:

        ApiGuard.validate_signature(
            self.define_output_table.__name__, self.__define_output_table_signature,
            *fields)

        schema_def = self.define_schema(*fields, schema_type=_meta.SchemaType.TABLE)
        return _meta.ModelOutputSchema(schema=schema_def)

    @staticmethod
    def _build_table_schema(
            *fields: _tp.Union[_meta.FieldSchema, _tp.List[_meta.FieldSchema]]) \
            -> _meta.TableSchema:

        if len(fields) == 1 and isinstance(fields[0], list):
            fields_ = fields[0]
        else:
            fields_ = fields

        if all(map(lambda f: f.fieldOrder is None, fields_)):
            for index, field in enumerate(fields_):
                field.fieldOrder = index

        return _meta.TableSchema([*fields_])

    def _prepare_signatures(self):

        self.__define_parameter_signature = inspect.signature(self.define_parameter)
        self.__define_parameters_signature = inspect.signature(self.define_parameters)
        self.__define_field_signature = inspect.signature(self.define_field)
        self.__define_schema_signature = inspect.signature(self.define_schema)
        self.__load_schema_signature = inspect.signature(self.load_schema)
        self.__define_input_table_signature = inspect.signature(self.define_input_table)
        self.__define_output_table_signature = inspect.signature(self.define_output_table)
