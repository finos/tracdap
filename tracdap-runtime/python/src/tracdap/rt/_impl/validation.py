#  Copyright 2023 Accenture Global Solutions Limited
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

import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.util as _util

from tracdap.rt.api.hook import _Named  # noqa


def validate_signature(method: _tp.Callable, *args, **kwargs):
    _TypeValidator.validate_signature(method, *args, **kwargs)


def validate_return_type(method: _tp.Callable, value: _tp.Any):
    _TypeValidator.validate_return_type(method, value)


def check_type(expected_type: _tp.Type, value: _tp.Any) -> bool:
    return _TypeValidator.check_type(expected_type, value)


class _TypeValidator:

    # The metaclass for generic types varies between versions of the typing library
    # To work around this, detect the correct metaclass by inspecting a generic type variable
    __generic_metaclass = type(_tp.List[object])

    # Cache method signatures to avoid inspection on every call
    # Inspecting a function signature can take ~ half a second in Python 3.7
    __method_cache: _tp.Dict[str, inspect.Signature] = dict()

    _log: logging.Logger = _util.logger_for_namespace(__package__)

    @classmethod
    def validate_signature(cls, method: _tp.Callable, *args, **kwargs):

        if method.__name__ in cls.__method_cache:
            signature = cls.__method_cache[method.__name__]
        else:
            signature = inspect.signature(method)
            cls.__method_cache[method.__name__] = signature

        positional_index = 0

        for param_name, param in signature.parameters.items():

            values = cls._select_arg(method.__name__, param, positional_index, *args, **kwargs)
            positional_index += len(values)

            for value in values:
                cls._validate_arg(method.__name__, param, value)

    @classmethod
    def validate_return_type(cls, method: _tp.Callable, value: _tp.Any):

        if method.__name__ in cls.__method_cache:
            signature = cls.__method_cache[method.__name__]
        else:
            signature = inspect.signature(method)
            cls.__method_cache[method.__name__] = signature

        correct_type = cls._validate_type(signature.return_annotation, value)

        if not correct_type:
            err = f"Invalid API return type for [{method.__name__}()]: " + \
                  f"Expected [{signature.return_annotation}], got [{type(value)}]"
            cls._log.error(err)
            raise _ex.ERuntimeValidation(err)

    @classmethod
    def check_type(cls, expected_type: _tp.Type, value: _tp.Any) -> bool:

        return cls._validate_type(expected_type, value)

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

        if not cls._validate_type(parameter.annotation, value):

            expected_type = cls._type_name(parameter.annotation)
            actual_type = cls._type_name(type(value)) if value is not None else str(None)

            err = f"Invalid API call [{method_name}()]: Wrong type for [{parameter.name}]" \
                  + f" (expected [{expected_type}], got [{actual_type}])"

            cls._log.error(err)
            raise _ex.ERuntimeValidation(err)

    @classmethod
    def _validate_type(cls, expected_type: _tp.Type, value: _tp.Any) -> bool:

        if expected_type == _tp.Any:
            return True

        if isinstance(expected_type, cls.__generic_metaclass):

            origin = _util.get_origin(expected_type)
            args = _util.get_args(expected_type)

            # The generic type "_Named" is defined in the TRAC API so needs to be supported
            # This type is used for passing named intermediate values between the define_ methods
            if origin is _Named:

                named_type = args[0]
                return isinstance(value, _Named) and cls._validate_type(named_type, value.item)

            # _tp.Union also covers _tp.Optional, which is shorthand for _tp.Union[_type, None]
            if origin is _tp.Union:

                for union_type in args:
                    if cls._validate_type(union_type, value):
                        return True

                return False

            if origin is list:

                list_type = args[0]
                return isinstance(value, list) and \
                    all(map(lambda v: cls._validate_type(list_type, v), value))

            if origin is dict:

                key_type = args[0]
                value_type = args[1]

                return isinstance(value, dict) and \
                    all(map(lambda k: cls._validate_type(key_type, k), value.keys())) and \
                    all(map(lambda v: cls._validate_type(value_type, v), value.values()))

            raise _ex.ETracInternal(f"Validation of [{origin.__name__}] generic parameters is not supported yet")

        # Validate everything else as a concrete type

        # TODO: Recursive validation of types for class members using field annotations

        return isinstance(value, expected_type)

    @classmethod
    def _type_name(cls, type_var: _tp.Type) -> str:

        if isinstance(type_var, cls.__generic_metaclass):

            origin = _util.get_origin(type_var)
            args = _util.get_args(type_var)

            if origin is _Named:
                named_type = cls._type_name(args[0])
                return f"Named[{named_type}]"

            if origin is _tp.Union:
                return "|".join(map(cls._type_name, args))

            if origin is list:
                list_type = cls._type_name(args[0])
                return f"List[{list_type}]"

            raise _ex.ETracInternal(f"Validation of [{origin.__name__}] generic parameters is not supported yet")

        return type_var.__name__
