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
    import urllib3 as _ul3
except ModuleNotFoundError:
    _ul3 = None

import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.external as _external
import tracdap.rt.ext.plugins as _plugins


class HttpPlugin(_external.IExternalSystem):

    __HC_CLIENT_ARGS = ["timeout"]
    __UL3_CLIENT_ARGS = ["timeout"]

    def __init__(self, properties: dict[str, str]):

        self.__properties = properties

        self.__resource_name = "http_resource"
        self.__host = "github.com"
        self.__port = None
        self.__tls = True

    def supported_types(self) -> list[type]:

        supported_types: list[type] = list()

        if _hc:
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

        if client_type == _hc.HTTPConnection:
            if self.__tls:
                return self._create_client_hc_https(**client_args)
            else:
                return self._create_client_hc_http(**client_args)

        if client_type == _hc.HTTPSConnection:
            if self.__tls:
                return self._create_client_hc_https(**client_args)
            else:
                raise self._error_tls_not_enabled()

        if _ul3 and client_type == _ul3.HTTPConnectionPool:
            if self.__tls:
                return self._create_client_ul3_https(**client_args)
            else:
                return self._create_client_ul3_http(**client_args)

        if _ul3 and client_type == _ul3.HTTPSConnectionPool:
            if self.__tls:
                return self._create_client_ul3_https(**client_args)
            else:
                raise self._error_tls_not_enabled()

        raise _ex.EPluginNotAvailable(f"Client type [{client_type.__qualname__}] is not available in {self.__class__.__name__}")

    def _create_client_hc_http(self, **client_args) -> _hc.HTTPConnection:
        hc_args = self._filter_args_hc(**client_args)
        return _hc.HTTPSConnection(self.__host, self.__port, **hc_args)

    def _create_client_hc_https(self, **client_args) -> _hc.HTTPSConnection:
        hc_args = self._filter_args_hc(**client_args)
        return _hc.HTTPSConnection(self.__host, self.__port, **hc_args)

    def _filter_args_hc(self, **client_args):
        return {k: v for k, v in client_args.items() if k in self.__HC_CLIENT_ARGS}

    def _create_client_ul3_http(self, **client_args) -> _ul3.HTTPConnectionPool:
        ul3_args = self._filter_args_ul3(**client_args)
        return _ul3.HTTPSConnectionPool(self.__host, self.__port, **ul3_args)

    def _create_client_ul3_https(self, **client_args) -> _ul3.HTTPSConnectionPool:
        ul3_args = self._filter_args_ul3(**client_args)
        return _ul3.HTTPSConnectionPool(self.__host, self.__port, **ul3_args)

    def _filter_args_ul3(self, **client_args):
        return {k: v for k, v in client_args.items() if k in self.__HC_CLIENT_ARGS}

    def _error_tls_not_enabled(self):
        detail = f"The resource [{self.__resource_name }] does not have TLS enabled"
        return _ex.ERuntimeValidation(f"Cannot create HTTPS connection: {detail}")

    def close_client(self, client: object):
        client.close()  # noqa


_plugins.PluginManager.register_plugin(
    _external.IExternalSystem, HttpPlugin,
    protocols=["http", "https"])
