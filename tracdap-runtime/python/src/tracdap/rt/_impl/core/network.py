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

import ssl as _ssl
import typing as _tp
import pathlib as _pathlib
import http.client as _hc

# Hard dependency on certifi for global SSL certificates
import certifi as _cf

try:
    import urllib3 as _ul3  # noqa
except ModuleNotFoundError:
    _ul3 = None

try:
    import requests as _rq  # noqa
    import requests.adapters as _rqa  # noqa
except ModuleNotFoundError:
    _rq = None

try:
    import httpx as _hx  # noqa
except ModuleNotFoundError:
    _hx = None

import tracdap.rt._impl.core.config_parser as _cfg
import tracdap.rt._impl.core.guard_rails as _guard
import tracdap.rt._impl.core.logging as _log
import tracdap.rt._impl.core.util as _util
import tracdap.rt._impl.core.validation as _val
import tracdap.rt.exceptions as _ex


class NetworkManager:

    NETWORK_PROFILE_KEY = "network.profile"
    NETWORK_SSL_CA_CERTIFICATES_KEY = "network.ssl.caCertificates"
    NETWORK_SSL_PUBLIC_CERTIFICATES_KEY = "network.ssl.publicCertificates"

    HTTP_CONNECTION_ARGS = ["timeout"]
    URLLIB3_CONNECTION_POOL_ARGS = ["timeout", "retries"]
    URLLIB3_POOL_MANAGER_ARGS = ["num_pools"]

    HTTPX_TRANSPORT_ARGS = ["retries", "limits", "htp1", "http2"]
    HTTPX_CLIENT_ARGS = ["base_url", "timeout", "follow_redirects", "max_redirects"] + HTTPX_TRANSPORT_ARGS

    CONFIG_TYPE = _cfg.PluginConfig | None

    __instance: "NetworkManager" = None

    # Utility class to allow using a regular SSL context with the requests library
    if _rq is not None:
        class _RequestsSslAdapter(_rqa.HTTPAdapter):

            def __init__(self, ssl_context):
                self._ssl_context = ssl_context
                super().__init__()

            def init_poolmanager(self, *args, **kwargs):
                kwargs['ssl_context'] = self._ssl_context
                return super().init_poolmanager(*args, **kwargs)

    @classmethod
    def initialize(
            cls, config_manager: _cfg.ConfigManager,
            system_config: _cfg.RuntimeConfig):

        _guard.run_model_guard("Initializing the network manager")
        _val.validate_signature(cls.initialize, config_manager, system_config)

        if cls.__instance is None:
            cls.__instance = NetworkManager(config_manager, system_config)
        else:
            # Allow re-initialization (for testing and embedded use cases)
            cls.__instance.__config_manager = config_manager
            cls.__instance.__sys_config = system_config

    @classmethod
    def instance(cls) -> "NetworkManager":

        _guard.run_model_guard("Accessing the network manager")

        if cls.__instance is None:
            raise _ex.ETracInternal("Network manager is not initialized")

        return cls.__instance

    def __init__(self, config_manager: _cfg.ConfigManager, system_config: _tp.Optional[_cfg.RuntimeConfig]):
        self.__config_manager: _cfg.ConfigManager = config_manager
        self.__sys_config: _cfg.RuntimeConfig = system_config
        self.__log = _log.logger_for_object(self)

    def create_http_client_connection(
            self, host: str, port: int, tls: bool = True,
            config: CONFIG_TYPE = None, **client_args) \
            -> "_hc.HTTPConnection":

        _guard.run_model_guard(allow_callback=True)
        _val.validate_signature(self.create_http_client_connection, host, port, tls, config, **client_args)
        self._check_args(self.create_http_client_connection, client_args, self.HTTP_CONNECTION_ARGS)

        if tls:
            ssl_context = self.create_ssl_context(config)
            return _hc.HTTPSConnection(host, port, context=ssl_context, **client_args)

        else:
            return _hc.HTTPConnection(host, port, **client_args)

    def create_urllib3_connection_pool(
            self, host: str, port: int, tls: bool = True,
            config: CONFIG_TYPE = None, **client_args) \
            -> "_ul3.HTTPConnectionPool":

        _guard.run_model_guard(allow_callback=True)
        _val.validate_signature(self.create_urllib3_connection_pool, host, port, tls, config, **client_args)
        self._check_args(self.create_urllib3_connection_pool, client_args, self.URLLIB3_CONNECTION_POOL_ARGS)

        if tls:
            ssl_context = self.create_ssl_context(config)
            return _ul3.HTTPSConnectionPool(host, port, ssl_context=ssl_context, **client_args)

        else:
            return _ul3.HTTPConnectionPool(host, port, **client_args)

    def create_urllib3_pool_manager(
            self, config: CONFIG_TYPE = None, **pool_args) \
            -> "_ul3.PoolManager":

        _guard.run_model_guard(allow_callback=True)
        _val.validate_signature(self.create_urllib3_pool_manager, config, **pool_args)
        self._check_args(self.create_urllib3_pool_manager, pool_args, self.URLLIB3_POOL_MANAGER_ARGS)

        ssl_context = self.create_ssl_context(config)
        return _ul3.PoolManager(ssl_context=ssl_context, **pool_args)

    def create_requests_session(self, config: CONFIG_TYPE = None) -> "_rq.Session":

        _guard.run_model_guard(allow_callback=True)
        _val.validate_signature(self.create_requests_session, config)

        ssl_context = self.create_ssl_context(config)

        session = _rq.Session()
        adapter = self._RequestsSslAdapter(ssl_context)
        session.mount("https://", adapter)

        return session

    def create_httpx_client(self, config: CONFIG_TYPE = None, **client_args) -> "_hx.Client":

        _guard.run_model_guard(allow_callback=True)
        _val.validate_signature(self.create_httpx_client, config, **client_args)
        self._check_args(self.create_httpx_client, client_args, self.HTTPX_CLIENT_ARGS)

        transport_args = self._filter_args(client_args, self.HTTPX_TRANSPORT_ARGS)
        transport = self.create_httpx_transport(config, **transport_args)

        return _hx.Client(transport=transport, **client_args)

    def create_httpx_transport(self, config: CONFIG_TYPE = None, **transport_args) -> "_hx.HTTPTransport":

        _guard.run_model_guard(allow_callback=True)
        _val.validate_signature(self.create_httpx_transport, config, **transport_args)
        self._check_args(self.create_httpx_transport, transport_args, self.HTTPX_TRANSPORT_ARGS)

        ssl_context = self.create_ssl_context(config)

        return _hx.HTTPTransport(verify=ssl_context, **transport_args)

    def create_ssl_context(self, config: CONFIG_TYPE = None) -> _ssl.SSLContext:

        _guard.run_model_guard(allow_callback=True)

        # Read the relevant SSL props
        properties = self._process_network_properties(config)
        ca_certs = _util.read_property(properties, self.NETWORK_SSL_CA_CERTIFICATES_KEY, optional=True)
        public_certs = _util.read_property(properties, self.NETWORK_SSL_PUBLIC_CERTIFICATES_KEY, convert=bool, default=False)

        # If caCertificates is specified, set up a custom context
        if ca_certs is not None:

            # Resolve the certs path using TRAC's configure manager path resolution
            # This allows relative paths for cert files kept in the TRAC config directory
            certs_url = self.__config_manager.resolve_config_url(ca_certs)

            ca_file = None
            ca_path = None
            ca_data = None

            # Determine how to interpret caCertificates (file, dir or data location)
            if certs_url.scheme == "file":
                certs_path = _pathlib.Path(certs_url.path)
                if certs_path.is_file():
                    ca_file = certs_path
                elif certs_path.is_dir():
                    ca_path = certs_path
            else:
                ca_data = self.__config_manager.load_config_file(ca_certs, "network certificates")

            # Set up context using just the configured caCertificates
            context = _ssl.create_default_context(cafile=ca_file, capath=ca_path, cadata=ca_data)

            # If publicCertificates is specified, add in the public certs as well
            if public_certs:
                context.load_verify_locations(cafile=_cf.where())

        # If caCertificates is not specified, always use the public certs
        else:
            context = _ssl.create_default_context(cafile=_cf.where())

        return context

    def _process_network_properties(self, config: CONFIG_TYPE= None) -> dict[str, str]:

        _guard.run_model_guard(allow_callback=True)

        if config is None or config.properties is None:
            return self.__sys_config.properties

        profile = _util.read_property(config.properties, self.NETWORK_PROFILE_KEY, default="system")

        if profile == "system":
            return self.__sys_config.properties
        elif profile == "direct":
            return dict()
        elif profile == "custom":
            return config.properties
        elif profile == "combined":
            combined_properties = dict(**self.__sys_config.properties)
            combined_properties.update(**config.properties)
            return combined_properties
        else:
            raise _ex.EConfig(f"Invalid network profile: [{profile}]")

    @classmethod
    def _check_args(cls, method, supplied_args, allowed_args):

        unknown_args = list(filter(lambda arg: arg not in allowed_args, supplied_args.keys()))

        if any(unknown_args):
            raise _ex.ETracInternal(f"Unknown args supplied to {method.__name__}(): {', '.join(unknown_args)}")

    @classmethod
    def _filter_args(cls, supplied_args, allowed_args):

        return dict(filter(lambda kv: kv[0] in allowed_args, supplied_args.items()))
