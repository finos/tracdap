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

import typing as _tp
import pkgutil as _pkg
import importlib as _il
import inspect as _inspect

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.guard_rails as _guard
import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.util as _util
import tracdap.rt._impl.core.validation as _val

from tracdap.rt.ext.plugins import *


class PluginManagerImpl(PluginManager):

    T_SERVICE = PluginManager.T_SERVICE

    __log = _logging.getLogger(f"{__name__}.PluginManager")

    __core_registered = False
    __ext_registered = False
    __3rd_party_registered = list()

    __plugins = {}

    @classmethod
    def register_core_plugins(cls):

        _guard.run_model_guard()

        if cls.__core_registered:
            return

        cls.__log.info("Register core plugins...")
        cls.__register_plugin_package("tracdap.rt._plugins")

        cls.__core_registered = True

    @classmethod
    def register_trac_extensions(cls):

        _guard.run_model_guard()

        if cls.__ext_registered:
            return

        cls.__log.info("Register TRAC extensions...")

        try:

            ext_package = _il.import_module("tracdap.ext")

            for module in _pkg.iter_modules(ext_package.__path__):
                cls.__register_plugin_package(f"tracdap.ext.{module.name}")

        except ModuleNotFoundError:
            cls.__log.info("No TRAC extensions are installed")

        cls.__ext_registered = True

    @classmethod
    def register_plugin_package(cls, plugin_package_name: str):

        _guard.run_model_guard()

        if plugin_package_name in cls.__3rd_party_registered:
            return

        if plugin_package_name.startswith("tracdap."):
            raise _ex.EStartup("3rd party plugins cannot be registered from the tracdap namespace")

        cls.__log.info(f"Register plugins from package [{plugin_package_name}]...")
        cls.__register_plugin_package(plugin_package_name)

        cls.__3rd_party_registered.append(plugin_package_name)

    @classmethod
    def __register_plugin_package(cls, plugin_package_name: str):

        _guard.run_model_guard()

        plugins_package = _il.import_module(plugin_package_name)

        for module in _pkg.iter_modules(plugins_package.__path__):

            module_name = f"{plugin_package_name}.{module.name}"

            try:
                _il.import_module(module_name)
            except ImportError:
                # It is not a fatal error if some plugins fail to load
                cls.__log.warning(f"Failed to load plugins from module [{module_name}]")

    @classmethod
    def register_plugin(
            cls,
            service_type: _tp.Type[T_SERVICE],
            service_class: _tp.Type[T_SERVICE],
            protocols: _tp.List[str]):

        _guard.run_model_guard()

        cls.__log.info(f"Register {service_type.__name__}: [{service_class.__name__}] ({', '.join(protocols)})")

        for protocol in protocols:
            plugin_key = (service_type, protocol)
            cls.__plugins[plugin_key] = service_class

    @classmethod
    def is_plugin_available(cls, service_type: _tp.Type[T_SERVICE], protocol: str):

        _guard.run_model_guard()

        plugin_key = (service_type, protocol)
        return plugin_key in cls.__plugins

    @classmethod
    def load_plugin(
            cls,
            service_type: _tp.Type[T_SERVICE],
            config: _cfg.PluginConfig,
            resource_name: _tp.Optional[str] = None) \
            -> T_SERVICE:

        _guard.run_model_guard()

        plugin_key = (service_type, config.protocol)
        plugin_class = cls.__plugins.get(plugin_key)

        if plugin_class is None:
            plugin_type = service_type.__name__
            raise _ex.EPluginNotAvailable(f"No plugin available for [{plugin_type}] with protocol [{config.protocol}]")

        plugin_args = cls.__build_plugin_args(plugin_class, config, resource_name)

        try:
            return plugin_class(**plugin_args)

        except TypeError as e:
            # Plugins that do not correctly implement the abstract base will get this error
            plugin_type_name = f"{plugin_class.__module__}.{plugin_class.__qualname__}"
            detail = "(plugin does not match the interface)"
            message = f"Invalid plugin: [{plugin_type_name}] {detail}"
            raise _ex.EPluginConformance(message) from e

    @classmethod
    def load_config_plugin(cls,
            service_type: _tp.Type[T_SERVICE],
            config: _cfg.PluginConfig) \
            -> T_SERVICE:

        # Currently config plugins are loaded the same way as regular plugins
        # However, regular plugins can be modified to take ConfigManager as an init parameter
        # This is useful for loading secondary config files needed in particularly plugins
        # Config plugins can never do this, because the config manager is not yet initialized

        return cls.load_plugin(service_type, config)

    @classmethod
    def __build_plugin_args(
            cls, plugin_class: _tp.Type[T_SERVICE],
            config: _cfg.PluginConfig,
            resource_name: _tp.Optional[str]):

        # Plugins that don't define __init__ don't need any constructor args
        # hasattr(cls, "__init__") still returns true, because object.__init__ is inherited
        if plugin_class.__init__ is object.__init__:
            return {}

        signature = _inspect.signature(plugin_class.__init__)
        hints = _tp.get_type_hints(plugin_class.__init__)
        args = dict()

        for arg_name in signature.parameters:

            if arg_name == "self":
                continue

            if arg_name == "resource_name":
                cls.__check_plugin_arg(plugin_class, "resource_name", str, hints)
                args["resource_name"] = resource_name or f"trac_plugin_{plugin_class.__name__}"

            elif arg_name == "config":
                cls.__check_plugin_arg(plugin_class, "config", _cfg.PluginConfig, hints)
                args["config"] = config

            elif arg_name == "properties":
                cls.__check_plugin_arg(plugin_class, "properties", _tp.Dict[str, str], hints)
                properties = dict()
                if config.publicProperties:
                    properties.update(config.publicProperties)
                if config.properties:
                    properties.update(config.properties)
                args["properties"] = properties

            else:
                detail = f"(unknown argument)"
                message = f"Invalid argument [{arg_name}] for plugin [{_util.qualified_type_name(plugin_class)}] {detail}"
                raise _ex.EPluginConformance(message)

        return args

    @classmethod
    def __check_plugin_arg(
            cls, plugin_class: _tp.Type[T_SERVICE], arg_name: str,
            expected_type: type, plugin_hints: dict):

        hint_type = plugin_hints.get(arg_name)

        if hint_type is not None and not _val.check_type_hint(expected_type, hint_type):
            detail = f"(wrong type, expected {_val.type_name(expected_type)}, got {_val.type_name(hint_type)})"
            message = f"Invalid argument [{arg_name}] for plugin [{_util.qualified_type_name(plugin_class)}] {detail}"
            raise _ex.EPluginConformance(message)
