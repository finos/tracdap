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

import http.client as _hc

try:
    import urllib3 as _ul3  # noqa
except ModuleNotFoundError:
    _ul3 = None

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.external as _external
import tracdap.rt.ext.plugins as _plugins
import tracdap.rt.ext.util as _util


class HttpPlugin(_external.IExternalSystem):

    HOST_KEY = "host"
    PORT_KEY = "port"
    TLS_KEY = "tls"
    TIMEOUT_KEY = "timeout"

    def __init__(self, resource_name: str, config: _cfg.PluginConfig, network_manager: _plugins.INetworkManager):

        self.__resource_name = resource_name
        self.__config = config

        self.__host = _util.read_plugin_config(config, self.HOST_KEY)
        self.__port = _util.read_plugin_config(config, self.PORT_KEY, optional=True, convert=int)
        self.__tls = _util.read_plugin_config(config, self.TLS_KEY, default=True, convert=bool)
        self.__timeout = _util.read_plugin_config(config, self.TIMEOUT_KEY, optional=True, convert=int)

        self.__network_manager = network_manager

    def supported_types(self) -> list[type]:

        supported_types: list[type] = list()

        supported_types.append(_hc.HTTPConnection)
        supported_types.append(_hc.HTTPSConnection)

        if _ul3:
            supported_types.append(_ul3.HTTPConnectionPool)
            supported_types.append(_ul3.HTTPSConnectionPool)

        return supported_types

    def supported_args(self) -> dict[str, type] | None:

        return {
            "timeout": float
        }

    def create_client(self, client_type: type, **client_args) -> object:

        common_args = self._build_common_args(**client_args)

        if client_type == _hc.HTTPConnection or client_type == _hc.HTTPSConnection:

            if not self.__tls and client_type == _hc.HTTPSConnection:
                raise self._error_tls_not_enabled()

            return self.__network_manager.create_http_client_connection(
                self.__host, self.__port, self.__tls,
                self.__config, **common_args)

        if _ul3 and client_type == _ul3.HTTPConnectionPool or client_type == _ul3.HTTPSConnectionPool:

            if not self.__tls and client_type == _ul3.HTTPSConnectionPool:
                raise self._error_tls_not_enabled()

            return self.__network_manager.create_urllib3_connection_pool(
                self.__host, self.__port, self.__tls,
                self.__config, **common_args)

        raise _ex.EPluginNotAvailable(f"Client type [{client_type.__qualname__}] is not available in {self.__class__.__name__}")

    def _build_common_args(self, **client_args):

        args = dict()

        if self.TIMEOUT_KEY in client_args and self.__timeout is not None:
            args[self.TIMEOUT_KEY] = min(client_args[self.TIMEOUT_KEY], self.__timeout)
        elif self.TIMEOUT_KEY in client_args:
            args[self.TIMEOUT_KEY] = client_args[self.TIMEOUT_KEY]
        elif self.__timeout is not None:
            args[self.TIMEOUT_KEY] = self.__timeout

        return args

    def _error_tls_not_enabled(self):
        detail = f"The resource [{self.__resource_name }] does not have TLS enabled"
        return _ex.ERuntimeValidation(f"Cannot create HTTPS connection: {detail}")

    def close_client(self, client: object):
        client.close()  # noqa


_plugins.PluginManager.register_plugin(
    _external.IExternalSystem, HttpPlugin,
    protocols=["http", "https"])
