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

import abc as _abc
import typing as _tp

import tracdap.rt.config as _cfg
import tracdap.rt.metadata as _meta


if _tp.TYPE_CHECKING:

    import http.client as _hc

    try:
        import urllib3 as _ul3  # noqa
    except ModuleNotFoundError:
        _ul3 = None

    try:
        import requests as _rq  # noqa
    except ModuleNotFoundError:
        _rq = None

    try:
        import httpx as _hx  # noqa
    except ModuleNotFoundError:
        _hx = None


class INetworkManager(metaclass=_abc.ABCMeta):

    CONFIG_TYPE = _tp.Union[_cfg.PluginConfig, _meta.ResourceDefinition, None]

    @_abc.abstractmethod
    def create_http_client_connection(
            self, host: str, port: int, tls: bool,
            config: _tp.Optional[_cfg.PluginConfig] = None, **client_args) \
            -> "_hc.HTTPConnection":

        pass

    @_abc.abstractmethod
    def create_urllib3_connection_pool(
            self, host: str, port: int, tls: bool = True,
            config: CONFIG_TYPE = None, **client_args) \
            -> "_ul3.HTTPConnectionPool":

        pass

    @_abc.abstractmethod
    def create_urllib3_pool_manager(
            self, config: CONFIG_TYPE = None, **pool_args) \
            -> "_ul3.PoolManager":

        pass

    @_abc.abstractmethod
    def create_requests_session(self, config: CONFIG_TYPE = None) -> "_rq.Session":

        pass

    @_abc.abstractmethod
    def create_httpx_client(self, config: CONFIG_TYPE = None, **client_args) -> "_hx.Client":

        pass

    @_abc.abstractmethod
    def create_httpx_transport(self, config: CONFIG_TYPE = None, **transport_args) -> "_hx.HTTPTransport":

        pass
