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

import tracdap.rt.config as _cfg


class PluginManager:

    T_SERVICE = _tp.TypeVar("T_SERVICE")

    __hook: _tp.Optional[_tp.Type["PluginManager"]] = None

    @classmethod
    def __get_hook(cls) -> _tp.Type["PluginManager"]:

        if cls.__hook is None:
            import tracdap.rt._impl.core.plugins as _plugins  # noqa
            cls.__hook = _plugins.PluginManagerImpl

        return cls.__hook

    @classmethod
    def register_plugin(
            cls,
            service_type: _tp.Type[T_SERVICE],
            service_class: _tp.Type[T_SERVICE],
            protocols: _tp.List[str]):

        hook = cls.__get_hook()
        hook.register_plugin(service_type, service_class, protocols)

    @classmethod
    def is_plugin_available(cls, service_type: _tp.Type[T_SERVICE], protocol: str):

        hook = cls.__get_hook()
        return hook.is_plugin_available(service_type, protocol)

    @classmethod
    def load_plugin(
            cls,
            service_type: _tp.Type[T_SERVICE],
            config: _cfg.PluginConfig) \
            -> T_SERVICE:

        hook = cls.__get_hook()
        return hook.load_plugin(service_type, config)

    @classmethod
    def load_config_plugin(cls,
            service_type: _tp.Type[T_SERVICE],
            config: _cfg.PluginConfig) \
            -> T_SERVICE:

        hook = cls.__get_hook()
        return hook.load_config_plugin(service_type, config)
