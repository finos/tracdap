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

import re
import typing as tp
import decimal
import enum
import uuid
import inspect
import dataclasses as _dc

import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.util as _util

import pathlib
import json
import yaml
import yaml.parser


_T = tp.TypeVar('_T')


class ConfigParser(tp.Generic[_T]):

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

    def __init__(self, config_class: _T.__class__, dev_mode_locations: tp.List[str] = None):
        self._log = _util.logger_for_object(self)
        self._config_class = config_class
        self._dev_mode_locations = dev_mode_locations or []
        self._errors = []

    def load_raw_config(self, config_file: tp.Union[str, pathlib.Path], config_file_name: str = None):

        if config_file_name is not None:
            self._log.info(f"Loading {config_file_name} config: {str(config_file)}")
        else:
            self._log.info(f"Loading config file: {str(config_file)}")

        # Construct a Path for config_file and make sure the file exists
        # (For now, config must be on a locally mounted filesystem)

        if isinstance(config_file, str):
            config_path = pathlib.Path(config_file)

        elif isinstance(config_file, pathlib.Path):
            config_path = config_file

        else:
            config_file_type = type(config_file) if config_file is not None else "None"
            err = f"Attempt to load an invalid config file, expected a path, got {config_file_type}"
            self._log.error(err)
            raise _ex.EConfigLoad(err)

        if not config_path.exists():
            msg = f"Config file not found: [{config_file}]"
            self._log.error(msg)
            raise _ex.EConfigLoad(msg)

        if not config_path.is_file():
            msg = f"Config path does not point to a regular file: [{config_file}]"
            self._log.error(msg)
            raise _ex.EConfigLoad(msg)

        return self._parse_raw_config(config_path)

    def _parse_raw_config(self, config_path: pathlib.Path):

        # Read in the raw config, use the file extension to decide which format to expect

        try:

            with config_path.open('r') as config_stream:

                extension = config_path.suffix.lower()

                if extension == ".yaml" or extension == ".yml":
                    config_dict = yaml.safe_load(config_stream)

                elif extension == ".json":
                    config_dict = json.load(config_stream)

                else:
                    msg = f"Format not recognised for config file [{config_path.name}]"
                    self._log.error(msg)
                    raise _ex.EConfigLoad(msg)

                return config_dict

        except UnicodeDecodeError as e:
            err = f"Contents of the config file is garbled and cannot be read ({str(e)})"
            self._log.error(err)
            raise _ex.EConfigParse(err) from e

        except json.decoder.JSONDecodeError as e:
            err = f"Config file contains invalid JSON ({str(e)})"
            self._log.error(err)
            raise _ex.EConfigParse(err) from e

        except yaml.parser.ParserError as e:
            err = f"Config file contains invalid YAML ({str(e)})"
            self._log.error(err)
            raise _ex.EConfigParse(err) from e

    def parse(self, config_dict: dict, config_file: tp.Union[str, pathlib.Path] = None) -> _T:

        # If config is empty, return a default (blank) config
        if config_dict is None or len(config_dict) == 0:
            return self._config_class()

        config = self._parse_value("", config_dict, self._config_class)

        if any(self._errors):

            message = "Errors found in config file" + (f" [{str(config_file)}]" if config_file is not None else "")

            for (location, error) in self._errors:
                location_info = f" (in {location})" if location else ""
                message = message + f"\n{error}{location_info}"

            raise _ex.EConfigParse(message)

        return config

    def _parse_value(self, location: str, raw_value: tp.Any, annotation: type):

        if raw_value is None:
            return None

        if annotation in ConfigParser.__primitive_types:
            return self._parse_primitive(location, raw_value, annotation)

        # Allow parsing of generic primitives, this allows for e.g. param maps of mixed primitive types
        if annotation == tp.Any:

            if type(raw_value) in ConfigParser.__primitive_types:
                return self._parse_primitive(location, raw_value, type(raw_value))
            else:
                return self._error(location, f"Expected a primitive value, got '{str(raw_value)}'")

        if isinstance(annotation, enum.EnumMeta):
            return self._parse_enum(location, raw_value, annotation)

        if _dc.is_dataclass(annotation):

            if isinstance(raw_value, tp.Dict):
                return self._parse_simple_class(location, raw_value, annotation)
            elif self._is_dev_mode_location(location) and type(raw_value) in ConfigParser.__primitive_types:
                return self._parse_primitive(location, raw_value, type(raw_value))
            else:
                return self._error(location, f"Expected type {annotation.__name__}, got '{str(raw_value)}'")

        if isinstance(annotation, self.__generic_metaclass):
            return self._parse_generic_class(location, raw_value, annotation)  # noqa

    def _is_dev_mode_location(self, location):

        return any(map(lambda pattern: re.match(pattern, location), self._dev_mode_locations))

    def _parse_primitive(self, location: str, raw_value: tp.Any, metaclass: type):

        parse_func = ConfigParser.__primitive_types[metaclass]

        try:
            if isinstance(raw_value, metaclass):
                return raw_value

            elif isinstance(raw_value, str):
                return parse_func(raw_value)

            elif metaclass == str:
                return str(raw_value)

            else:
                raise TypeError

        except (ValueError, TypeError):
            return self._error(location, f"Expected primitive type {metaclass.__name__}, got '{str(raw_value)}'")

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

    @staticmethod
    def _parse_enum_value(raw_value: str, metaclass: enum.EnumMeta):

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

            elif param_name in raw_dict and raw_dict[param_name] is not None:
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

        origin = _util.get_origin(metaclass)
        args = _util.get_args(metaclass)

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

    @staticmethod
    def _child_location(parent_location: str, item: str):

        if parent_location is None or parent_location == "":
            return item
        else:
            return parent_location + "." + item


class ConfigQuoter:

    JSON_FORMAT = "json"
    YAML_FORMAT = "yaml"

    @classmethod
    def quote(cls, obj: tp.Any, quote_format: str) -> str:

        if quote_format.lower() == cls.JSON_FORMAT:
            return cls.quote_json(obj)

        if quote_format.lower() == cls.YAML_FORMAT:
            return cls.quote_yaml(obj)

        # TODO: This is probably an error in the user-supplied parameters
        raise _ex.EUnexpected(f"Unsupported output format [{quote_format}]")

    @classmethod
    def quote_json(cls, obj: tp.Any) -> str:

        return json.dumps(obj, cls=ConfigQuoter._JsonEncoder, indent=4)

    @classmethod
    def quote_yaml(cls, obj: tp.Any) -> str:

        return yaml.dump(obj)

    class _JsonEncoder(json.JSONEncoder):

        def __init__(self, **kwargs):

            super().__init__(**kwargs)

            # Do not force-escape non-ascii characters, output UTF-8 instead
            self.ensure_ascii = False

        def default(self, o: tp.Any) -> str:

            if isinstance(o, enum.Enum):
                return o.name
            if isinstance(o, uuid.UUID):
                return str(o)
            elif type(o).__module__.startswith("tracdap."):
                return {**o.__dict__}  # noqa
            else:
                return super().default(o)
