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

import dataclasses as _dc
import decimal
import enum
import io
import inspect
import json
import os
import pathlib
import re
import types as ts
import typing as tp
import urllib.parse as _urlp
import uuid

import tracdap.rt.config as _config
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.plugins as _plugins
import tracdap.rt.ext.config as _config_ext
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.util as _util

import yaml
import yaml.parser

try:
    import pydantic as _pyd  # noqa
except ModuleNotFoundError:
    _pyd = None

_T = tp.TypeVar('_T')


class ConfigManager:

    @classmethod
    def for_root_config(cls, root_config_file: tp.Union[str, pathlib.Path, None]) -> "ConfigManager":

        if isinstance(root_config_file, pathlib.Path):
            root_file_path = cls._resolve_scheme(root_config_file)
            root_dir_path = cls._resolve_scheme(root_config_file.parent)
            if root_dir_path[-1] not in ["/", "\\"]:
                root_dir_path += os.sep
            root_file_url = _urlp.urlparse(root_file_path, scheme="file")
            root_dir_url = _urlp.urlparse(root_dir_path, scheme="file")
            return ConfigManager(root_dir_url, root_file_url, )

        elif isinstance(root_config_file, str):
            root_file_with_scheme = cls._resolve_scheme(root_config_file)
            root_file_url = _urlp.urlparse(root_file_with_scheme, scheme="file")
            root_dir_path = str(pathlib.Path(root_file_url.path).parent)
            if root_dir_path[-1] not in ["/", "\\"]:
                root_dir_path += os.sep if root_file_url.scheme == "file" else "/"
            root_dir_url = _urlp.urlparse(_urlp.urljoin(root_file_url.geturl(), root_dir_path))
            return ConfigManager(root_dir_url, root_file_url)

        else:
            working_dir_path = str(pathlib.Path.cwd().resolve())
            working_dir_url = _urlp.urlparse(str(working_dir_path), scheme="file")
            return ConfigManager(working_dir_url, None)

    @classmethod
    def for_root_dir(cls, root_config_dir: tp.Union[str, pathlib.Path]) -> "ConfigManager":

        if isinstance(root_config_dir, pathlib.Path):
            root_dir_path = cls._resolve_scheme(root_config_dir)
            if root_dir_path[-1] not in ["/", "\\"]:
                root_dir_path += os.sep
            root_dir_url = _urlp.urlparse(root_dir_path, scheme="file")
            return ConfigManager(root_dir_url, None)

        elif isinstance(root_config_dir, str):
            root_dir_with_scheme = cls._resolve_scheme(root_config_dir)
            if root_dir_with_scheme[-1] not in ["/", "\\"]:
                root_dir_with_scheme += "/"
            root_dir_url = _urlp.urlparse(root_dir_with_scheme, scheme="file")
            return ConfigManager(root_dir_url, None)

        # Should never happen since root dir is specified explicitly
        else:
            raise _ex.ETracInternal("Wrong parameter type for root_config_dir")

    @classmethod
    def _resolve_scheme(cls, raw_url: tp.Union[str, pathlib.Path]) -> str:

        if isinstance(raw_url, pathlib.Path):
            return "file:" + str(raw_url.resolve())

        # Look for drive letters on Windows - these can be mis-interpreted as URL scheme
        # If there is a drive letter, explicitly set scheme = file instead
        if len(raw_url) > 1 and raw_url[1] == ":":
            return "file:" + raw_url
        else:
            return raw_url

    def __init__(self, root_dir_url: _urlp.ParseResult, root_file_url: tp.Optional[_urlp.ParseResult]):
        self._log = _logging.logger_for_object(self)
        self._root_dir_url = root_dir_url
        self._root_file_url = root_file_url

    def config_dir_path(self):
        if self._root_dir_url.scheme == "file":
            return pathlib.Path(self._root_dir_url.path).resolve()
        else:
            return None

    def load_root_object(
            self, config_class: type(_T),
            dev_mode_locations: tp.List[str] = None,
            config_file_name: tp.Optional[str] = None) -> _T:

        # Root config not available normally means you're using embedded config
        # In which case this method should not be called
        if self._root_file_url is None:
            message = f"Root config file not available"
            self._log.error(message)
            raise _ex.EConfigLoad(message)

        resolved_url = self._root_file_url

        if config_file_name is not None:
            self._log.info(f"Loading {config_file_name} config: {self._url_to_str(resolved_url)}")
        else:
            self._log.info(f"Loading config file: {self._url_to_str(resolved_url)}")

        config_dict = self._load_config_dict(resolved_url)

        parser = ConfigParser(config_class, dev_mode_locations)
        return parser.parse(config_dict, resolved_url.path)

    def load_config_object(
            self, config_url: tp.Union[str, pathlib.Path],
            config_class: type(_T),
            dev_mode_locations: tp.List[str] = None,
            config_file_name: tp.Optional[str] = None) -> _T:

        resolved_url = self._resolve_config_file(config_url)

        if config_file_name is not None:
            self._log.info(f"Loading {config_file_name} config: {self._url_to_str(resolved_url)}")
        else:
            self._log.info(f"Loading config file: {self._url_to_str(resolved_url)}")

        config_dict = self._load_config_dict(resolved_url)

        parser = ConfigParser(config_class, dev_mode_locations)
        return parser.parse(config_dict, config_url)

    def load_config_file(
            self, config_url: tp.Union[str, pathlib.Path],
            config_file_name: tp.Optional[str] = None) -> bytes:

        resolved_url = self._resolve_config_file(config_url)

        if config_file_name is not None:
            self._log.info(f"Loading {config_file_name} config: {self._url_to_str(resolved_url)}")
        else:
            self._log.info(f"Loading config file: {self._url_to_str(resolved_url)}")

        return self._load_config_file(resolved_url)

    def _resolve_config_file(self, config_url: tp.Union[str, pathlib.Path]) -> _urlp.ParseResult:

        # If the config URL defines a scheme, treat it as absolute
        # (This also works for Windows paths, C:\ is an absolute path)
        if ":" in str(config_url):
            absolute_url = str(config_url)
        # If the root URL is a path, resolve using path logic (this allows for config_url to be an absolute path)
        elif self._root_dir_url.scheme == "file":
            absolute_url = str(pathlib.Path(self._root_dir_url.path).joinpath(str(config_url)))
        # Otherwise resolve relative to the root URL
        else:
            absolute_url = _urlp.urljoin(self._root_dir_url.geturl(), str(config_url))

        # Look for drive letters on Windows - these can be mis-interpreted as URL scheme
        # If there is a drive letter, explicitly set scheme = file instead
        if len(absolute_url) > 1 and absolute_url[1] == ":":
            absolute_url = "file:" + absolute_url

        return _urlp.urlparse(absolute_url, scheme="file")

    def _load_config_file(self, resolved_url: _urlp.ParseResult) -> bytes:

        loader = self._get_loader(resolved_url)
        config_url = self._url_to_str(resolved_url)

        if not loader.has_config_file(config_url):
            message = f"Config file not found: {config_url}"
            self._log.error(message)
            raise _ex.EConfigLoad(message)

        return loader.load_config_file(config_url)

    def _load_config_dict(self, resolved_url: _urlp.ParseResult) -> dict:

        loader = self._get_loader(resolved_url)
        config_url = self._url_to_str(resolved_url)

        if loader.has_config_dict(config_url):
            return loader.load_config_dict(config_url)

        elif loader.has_config_file(config_url):
            config_bytes = loader.load_config_file(config_url)
            config_path = pathlib.Path(resolved_url.path)
            return self._parse_config_dict(config_bytes, config_path)

        else:
            message = f"Config file not found: {config_url}"
            self._log.error(message)
            raise _ex.EConfigLoad(message)

    def _parse_config_dict(self, config_bytes: bytes, config_path: pathlib.Path):

        # Read in the raw config, use the file extension to decide which format to expect

        try:

            with io.BytesIO(config_bytes) as config_stream:

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

        except (yaml.parser.ParserError, yaml.reader.ReaderError) as e:
            err = f"Config file contains invalid YAML ({str(e)})"
            self._log.error(err)
            raise _ex.EConfigParse(err) from e

    def _get_loader(self, resolved_url: _urlp.ParseResult) -> _config_ext.IConfigLoader:

        protocol = resolved_url.scheme
        loader_config = _config.PluginConfig(protocol)

        if not _plugins.PluginManager.is_plugin_available(_config_ext.IConfigLoader, protocol):
            message = f"No config loader available for protocol [{protocol}]: {self._url_to_str(resolved_url)}"
            self._log.error(message)
            raise _ex.EConfigLoad(message)

        return _plugins.PluginManager.load_config_plugin(_config_ext.IConfigLoader, loader_config)

    @staticmethod
    def _url_to_str(url: _urlp.ParseResult) -> str:

        if url.scheme == "file" and not url.netloc:
            return url.path
        else:
            return url.geturl()


class ConfigParser(tp.Generic[_T]):

    # Support both new and old styles for generic, union and optional types
    # Old-style annotations are still valid, even when the new style is fully supported
    __generic_types: list[type] = [
        ts.GenericAlias,
        type(tp.List[int]),
        type(tp.Optional[int])
    ]

    # UnionType was added to the types module in Python 3.10, we support 3.9 (Jan 2025)
    if hasattr(ts, "UnionType"):
        __generic_types.append(ts.UnionType)

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
        self._log = _logging.logger_for_object(self)
        self._config_class = config_class
        self._dev_mode_locations = dev_mode_locations or []
        self._errors = []

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

        if self._is_dev_mode_location(location):

            if type(raw_value) in ConfigParser.__primitive_types:
                return self._parse_primitive(location, raw_value, type(raw_value))

            if isinstance(raw_value, list):
                if len(raw_value) == 0:
                    return []
                items = iter((self._child_location(location, i), x) for i, x in enumerate(raw_value))
                return list(self._parse_value(loc, x, tp.Any) for loc, x in items)

            if isinstance(raw_value, dict):
                if len(raw_value) == 0:
                    return {}
                items = iter((self._child_location(location, k), k, v) for k, v in raw_value.items())
                return dict((k, self._parse_value(loc, v, tp.Any)) for loc, k, v in items)

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
            return self._parse_simple_class(location, raw_value, annotation)

        # Basic support for Pydantic, if it is installed
        if _pyd and isinstance(annotation, type) and issubclass(annotation, _pyd.BaseModel):
            return self._parse_simple_class(location, raw_value, annotation)

        if any(map(lambda _t: isinstance(annotation, _t), self.__generic_types)):
            return self._parse_generic_class(location, raw_value, annotation)  # noqa

        return self._error(location, f"Cannot parse value of type {annotation.__name__}")

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
            return self._error(location, f"Expected type {metaclass.__name__}, got '{str(raw_dict)}'")

        obj = metaclass.__new__(metaclass, object())  # noqa

        init_signature = inspect.signature(metaclass.__init__)
        init_types = tp.get_type_hints(metaclass.__init__)
        init_params = iter(init_signature.parameters.items())
        init_values: tp.Dict[str, tp.Any] = dict()

        # Do not process 'self'
        next(init_params)

        for param_name, param in init_params:

            param_location = self._child_location(location, param_name)
            param_type = init_types.get(param_name)

            if param_type is None:
                message = f"Class {metaclass.__name__} does not support config decoding: " + \
                          f"Missing type information for init parameter '{param_name}'"
                self._error(location, message)
                init_values[param_name] = None

            elif param_name in raw_dict and raw_dict[param_name] is not None:
                param_value = self._parse_value(param_location, raw_dict[param_name], param_type)
                init_values[param_name] = param_value

            elif param.default != inspect._empty:  # noqa
                init_values[param_name] = param.default

            else:
                self._error(location, f"Missing required value '{param_name}'")
                init_values[param_name] = None

        binding = init_signature.bind(obj, **init_values)
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
            if any(map(lambda _t: isinstance(type(default_value), _t), self.__generic_types)):
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

    def _parse_generic_class(self, location: str, raw_value: tp.Any,  metaclass: type):

        origin = _util.get_origin(metaclass)
        args = _util.get_args(metaclass)

        if origin == tp.List or origin == list:

            list_type = args[0]

            if not isinstance(raw_value, list):
                return self._error(location, f"Expected a list, got {type(raw_value)}")

            return [
                self._parse_value(self._child_location(location, idx), item, list_type)
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
    def _child_location(parent_location: str, item: tp.Union[str, int]):

        if parent_location is None or parent_location == "":
            return item
        elif isinstance(item, int):
            return f"{parent_location}[{item}]"
        else:
            return f"{parent_location}.{item}"


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
