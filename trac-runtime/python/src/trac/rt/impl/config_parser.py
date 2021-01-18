#  Copyright 2021 Accenture Global Solutions Limited
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

import typing as tp
import decimal
import enum
import inspect

import pathlib
import json
import yaml


class ConfigParser:

    # The metaclass for generic types varies between versions of the typing library
    # To work around this, detect the correct metaclass by inspecting a generic type variable
    __generic_metaclass = type(tp.List[object])

    __primitive_types: tp.Dict[type, callable] = {
        bool: bool,
        int: int,
        float: float,
        str: str,
        decimal.Decimal: decimal.Decimal
        # TODO: Date (requires type system)
        # TODO: Datetime (requires type system)
    }

    def __init__(self, config_class: type):
        self._config_class = config_class
        self._errors = []

    @classmethod
    def load_raw_config(cls, config_file: str):

        config_path = pathlib.Path(config_file)
        extension = config_path.suffix.lower()

        # self._log.info(f"Loading config file: {str(config_path)}")

        if not config_path.exists() or not config_path.is_file():
            raise RuntimeError()  # TODO: Error

        with config_path.open('r') as config_stream:
            if extension == ".yaml" or extension == ".yml":
                config_dict = yaml.safe_load(config_stream)
            elif extension == ".json":
                config_dict = json.load(config_stream)
            else:
                raise RuntimeError()  # TODO: Error

            return config_dict

    def parse(self, config_dict: dict, config_file: str = None) -> object:

        config = self._parse_value("", config_dict, self._config_class)

        if any(self._errors):

            message = f"Errors found in config file" + (" " + config_file if config_file is not None else "")

            for (location, error) in self._errors:
                location_info = f" (in {location})" if location else ""
                message = message + f"\n{error}{location_info}"

            raise RuntimeError(message)

        return config

    def _parse_value(self, location: str, raw_value: tp.Any, annotation: type):

        if raw_value is None:
            return None

        if annotation in ConfigParser.__primitive_types:
            return self._parse_primitive(location, raw_value, annotation)

        if isinstance(annotation, enum.EnumMeta):
            return self._parse_enum(location, raw_value, annotation)

        if isinstance(annotation, self.__generic_metaclass):
            return self._parse_generic_class(location, raw_value, annotation)

        # If there is no special handling for the given type, try to interpret as a simple Python class
        return self._parse_simple_class(location, raw_value, annotation)

    def _parse_primitive(self, location: str, raw_value: tp.Any, metaclass: type):

        parse_func = ConfigParser.__primitive_types[metaclass]

        try:
            return parse_func(raw_value)
        except (ValueError, TypeError):
            return self._error(location, f"Expected primitive type {metaclass.__name__}, got {str(raw_value)}")

    def _parse_enum(self, location: str, raw_value: tp.Any, metaclass: enum.EnumMeta):

        if not isinstance(raw_value, str):
            return self._error(location, f"Expected {metaclass.__name__} (string), got {str(raw_value)}")

        try:
            enum_value = self._parse_enum_value(raw_value, metaclass)

            if isinstance(enum_value.value, tuple):
                enum_value._value_ = enum_value.value[0]

            return metaclass.__new__(metaclass, enum_value)

        except KeyError:
            return self._error(location, f"Invalid value for {metaclass.__name__}: {raw_value}")

    def _parse_enum_value(self, raw_value: str, metaclass: enum.EnumMeta):

        try:
            return metaclass.__members__[raw_value]

        except KeyError:

            # Try a case-insensitive match as a fallback
            for enum_member in metaclass.__members__:
                if enum_member.upper() == raw_value.upper():
                    return metaclass.__members__[enum_member]

            # Re-raise the exception if case-insensitive match fails
            raise

    def _parse_simple_class(self, location: str, raw_dict: tp.Any, metaclass: type) -> object:

        if raw_dict is not None and not isinstance(raw_dict, dict):
            pass

        obj = metaclass.__new__(metaclass, object())  # noqa

        init_signature = inspect.signature(metaclass.__init__)
        init_types = tp.get_type_hints(metaclass.__init__)
        init_params = iter(init_signature.parameters.items())
        init_values: tp.List[tp.Any] = list()

        # Do not process 'self'
        next(init_params)

        for param_name, param in init_params:

            param_location = self._child_location(location, param_name)
            param_type = init_types.get(param_name)

            if param_type is None:
                message = f"Class {metaclass.__name__} does not support config decoding: " + \
                          f"Missing type information for init parameter '{param_name}'"
                self._error(location, message)
                init_values.append(None)

            elif param_name in raw_dict:
                param_value = self._parse_value(param_location, raw_dict[param_name], param_type)
                init_values.append(param_value)

            elif param.default != inspect._empty:  # noqa
                init_values.append(param.default)

            else:
                self._error(location, f"Missing required value '{param_name}'")
                init_values.append(None)

        binding = init_signature.bind(obj, *init_values)
        metaclass.__init__(*binding.args, **binding.kwargs)

        # Now go back over the members and look for any that weren't declared in __init__
        # Members with non-generic default values can still be read from the input stream

        for member_name in obj.__dict__:

            member_location = self._child_location(location, member_name)
            default_value = obj.__dict__[member_name]

            if member_name in init_signature.parameters or member_name.startswith("_"):
                continue

            # Members not declared in __init__ must have a non-null default in order to get type info
            if default_value is None:
                message = f"Class {metaclass.__name__} does not support config decoding: " + \
                          f"Generic member must be declared in __init__: '{member_name}'"
                self._error(location, message)

            # Generic members must be declared in __init__ since that is the only way to get the full annotation
            if isinstance(type(default_value), self.__generic_metaclass):
                message = f"Class {metaclass.__name__} does not support config decoding: " + \
                          f"Members with no default value must be declared in __init__: '{member_name}'"
                self._error(location, message)

            # Use default value if none supplied
            if member_name not in raw_dict:
                continue

            member_value = self._parse_value(member_location, raw_dict[member_name], type(default_value))
            setattr(obj, member_name, member_value)

        # Check for illegal or unrecognised values in the supplied config (would indicate a config error)

        for raw_name in raw_dict.keys():

            if raw_name not in obj.__dict__:
                self._error(location, f"Unrecognised config parameter '{raw_name}'")

            if raw_name == "self" or raw_name.startswith("_"):
                self._error(location, f"Invalid config parameter '{raw_name}'")

        # All good, return the config object

        return obj

    def _parse_generic_class(self, location: str, raw_value: tp.Any, metaclass:  __generic_metaclass):

        origin = tp.get_origin(metaclass)
        args = tp.get_args(metaclass)

        if origin == tp.List or origin == list:

            list_type = args[0]

            if not isinstance(raw_value, list):
                return self._error(location, f"Expected a list, got {type(raw_value)}")

            return [
                self._parse_value(self._child_location(location, str(idx)), item, list_type)
                for (idx, item) in enumerate(raw_value)]

        if origin == tp.Dict or origin == dict:

            key_type = args[0]
            value_type = args[1]

            if not isinstance(raw_value, dict):
                return self._error(location, f"Expected {metaclass} (dict), got {type(raw_value)}")

            return {
                self._parse_value(self._child_location(location, key), key, key_type):
                self._parse_value(self._child_location(location, key), value, value_type)
                for key, value in raw_value.items()}

        # Handle Optional, which is a shorthand for tp.Union[type, None]
        if origin == tp.Union and len(args) == 2 and args[1] == type(None):  # noqa

            if raw_value is not None:
                return self._parse_value(location, raw_value, args[0])
            else:
                return None

        return self._error(location, f"Config parser does not support generic type '{str(origin)}'")

    def _error(self, location: str, error: str) -> None:
        self._errors.append((location, error))
        return None

    def _child_location(self, parent_location: str, item: str):

        if parent_location is None or parent_location == "":
            return item
        else:
            return parent_location + "." + item
