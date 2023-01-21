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
import logging as _log
import pkgutil as _pkg
import importlib as _il

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext._guard as _guard


class PluginManager:

    T_SERVICE = _tp.TypeVar("T_SERVICE")

    __log = _log.getLogger(f"{__name__}.PluginManager")

    __core_registered = False
    __plugins = {}

    @classmethod
    def register_core_plugins(cls):

        _guard.run_model_guard()

        if cls.__core_registered:
            return

        cls.__log.info("Register core plugins...")

        plugins_package = _il.import_module("tracdap.rt._plugins")

        for module in _pkg.iter_modules(plugins_package.__path__):
            try:
                module_name = f"tracdap.rt._plugins.{module.name}"
                _il.import_module(module_name)
            except ImportError:
                pass  # Ignore plugins that fail to load

        cls.__core_registered = True

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
            config: _cfg.PluginConfig) \
            -> T_SERVICE:

        _guard.run_model_guard()

        plugin_key = (service_type, config.protocol)
        plugin_class = cls.__plugins.get(plugin_key)

        if plugin_class is None:
            plugin_type = service_type.__name__
            raise _ex.EPluginNotAvailable(f"No plugin available for [{plugin_type}] with protocol [{config.protocol}]")

        plugin = plugin_class(config.properties)

        return plugin
