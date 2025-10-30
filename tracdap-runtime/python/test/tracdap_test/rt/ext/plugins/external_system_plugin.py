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
import http.client as _http

import tracdap.rt.ext.plugins as plugins
import tracdap.rt.ext.external as trac_external
import tracdap.rt.ext.network as net


class ExtExternalSystemPlugin(trac_external.IExternalSystem):

    def __init__(self, properties: dict[str, str], network_manager: net.INetworkManager):

        self._properties = properties
        self._network_manager = network_manager

    def supported_types(self) -> list[type]:

        if "supported_types_none" in self._properties:
            return None  # noqa

        if "supported_types_empty" in self._properties:
            return []

        if "supported_types_bad_list" in self._properties:
            return _http.HTTPConnection  # noqa

        if "supported_types_bad_entry" in self._properties:
            return ["not_a_type"]  # noqa

        return [_http.HTTPConnection, _http.HTTPSConnection]

    def supported_args(self) -> _tp.Optional[_tp.Dict[str, type]]:

        if "supported_args_bad_dict" in self._properties:
            return []  # noqa

        if "supported_args_bad_key" in self._properties:
            return { 1: int }  # noqa

        if "supported_args_bad_value" in self._properties:
            return { "arg_1": 1 }  # noqa

        # Allow timeout to be set as a client arg
        return { "timeout": int }

    def create_client(self, client_type: type[_http.HTTPConnection], **client_args) -> _http.HTTPConnection:

        if "create_client_none" in self._properties:
            return None  # noqa

        if "create_client_wrong_type" in self._properties:
            return "not_a_valid_client"  # noqa

        scheme = self._properties.get("scheme")
        host = self._properties.get("host")
        port = int(self._properties.get("port"))
        tls = scheme == "https"

        return self._network_manager.create_http_client_connection(host, port, tls, **client_args)

    def close_client(self, client: _http.HTTPConnection):

        client.close()

        if "close_client_return_something" in self._properties:  # noqa
            return "unexpected_value"


plugins.PluginManager.register_plugin(trac_external.IExternalSystem, ExtExternalSystemPlugin, ["test-ext-external"])
