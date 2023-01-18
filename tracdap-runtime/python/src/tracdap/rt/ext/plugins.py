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

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex


class PluginManager:

    T_SERVICE = _tp.TypeVar("T_SERVICE")

    __plugins = {}

    @classmethod
    def register_plugin(
            cls,
            service_type: _tp.Type[T_SERVICE],
            service_class: _tp.Type[T_SERVICE],
            protocols: _tp.List[str]):

        for protocol in protocols:

            plugin_key = (service_type, protocol)
            cls.__plugins[plugin_key] = service_class

    @classmethod
    def get_plugin(
            cls,
            service_type: _tp.Type[T_SERVICE],
            config: _cfg.PluginConfig) \
            -> T_SERVICE:

        plugin_key = (service_type, config.protocol)
        plugin_class = cls.__plugins.get(plugin_key)

        if plugin_class is None:
            raise _ex.EStartup(f"No plugin available for [{service_type}] with protocol [{config.protocol}]")

        plugin = plugin_class(config.properties)

        return plugin
