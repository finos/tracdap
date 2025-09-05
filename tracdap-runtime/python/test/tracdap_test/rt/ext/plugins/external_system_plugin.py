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

import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins
import tracdap.rt.ext.external as trac_external


class ExtExternalSystemPlugin(trac_external.IExternalSystem):

    def __init__(self, properties: dict[str, str]):

        self._properties = properties

    def supported_types(self) -> list[type]:

        if "client_types_none" in self._properties:
            return None  # noqa

        if "client_types_empty" in self._properties:
            return []

        if "client_types_bad_list" in self._properties:
            return _http.HTTPConnection  # noqa

        if "client_types_bad_entry" in self._properties:
            return ["not_a_type"]  # noqa

        return [_http.HTTPConnection, _http.HTTPSConnection]

    def supported_args(self) -> _tp.Optional[_tp.Dict[str, type]]:
        return None

    def create_client(self, client_type: type[_http.HTTPConnection], **kwargs) -> _http.HTTPConnection:

        scheme = self._properties.get("scheme")
        host = self._properties.get("host")
        port = self._properties.get("port")

        if scheme == "https":
            return _http.HTTPSConnection(host, int(port))

        if scheme == "http":
            return _http.HTTPConnection(host, int(port))

        raise ex.EConfig("Unsupported scheme: " + scheme)

    def close_client(self, client: _http.HTTPConnection):
        client.close()


plugins.PluginManager.register_plugin(trac_external.IExternalSystem, ExtExternalSystemPlugin, ["test-ext-external"])
