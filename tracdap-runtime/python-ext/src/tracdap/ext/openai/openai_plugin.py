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

import os
import threading
import concurrent.futures as fut
import queue
import logging


try:
    import openai  # noqa
except ModuleNotFoundError:
    openai = None

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.external as _external
import tracdap.rt.ext.plugins as _plugins
import tracdap.rt.ext.util as _util


class OpenAIPlugin(_external.IExternalSystem):

    API_KEY_KEY = "api_key"
    ORGANIZATION_KEY = "organization"
    PROJECT_KEY = "project"
    BASE_URL_KEY = "base_url"
    TIMEOUT_KEY = "timeout"
    MAX_RETRIES_KEY = "max_retries"

    AZURE_SUB_PROTOCOL = "azure"
    API_VERSION_KEY = "api_version"
    AZURE_ENDPOINT_KEY = "azure_endpoint"
    AZURE_DEPLOYMENT_KEY = "azure_deployment"
    AZURE_AD_TOKEN_KEY = "azure_ad_token"

    OPENAI_API_KEY = "OPENAI_API_KEY"
    AZURE_OPENAI_API_KEY = "AZURE_OPENAI_API_KEY"
    AZURE_OPENAI_AD_TOKEN = "AZURE_OPENAI_AD_TOKEN"

    def __init__(self, resource_name: str, config: _cfg.PluginConfig):

        log_name = f"{OpenAIPlugin.__module__}.{OpenAIPlugin.__name__}"
        self.__log = logging.getLogger(log_name)

        self.__resource_name = resource_name
        self.__protocol = config.protocol
        self.__sub_protocol = config.subProtocol

        self.__organization = _util.read_plugin_config(config, self.ORGANIZATION_KEY, optional=True)
        self.__project = _util.read_plugin_config(config, self.PROJECT_KEY, optional=True)
        self.__base_url = _util.read_plugin_config(config, self.BASE_URL_KEY, optional=True)
        self.__timeout = _util.read_plugin_config(config, self.TIMEOUT_KEY, default=openai.DEFAULT_TIMEOUT.read, convert=float)
        self.__max_retries = _util.read_plugin_config(config, self.MAX_RETRIES_KEY, default=openai.DEFAULT_MAX_RETRIES, convert=int)

        self.__api_version = _util.read_plugin_config(config, self.API_VERSION_KEY, optional=True)
        self.__azure_endpoint = _util.read_plugin_config(config, self.AZURE_ENDPOINT_KEY, optional=True)
        self.__azure_deployment = _util.read_plugin_config(config, self.AZURE_DEPLOYMENT_KEY, optional=True)

        if _util.has_plugin_config(config, self.API_KEY_KEY):
            api_key = _util.read_plugin_config(config, self.API_KEY_KEY)
            self.__api_key_func = lambda: api_key
        else:
            self.__api_key_func = None

        if _util.has_plugin_config(config, self.AZURE_AD_TOKEN_KEY):
            ad_token = _util.read_plugin_config(config, self.AZURE_AD_TOKEN_KEY)
            self.__azure_ad_token_func = lambda: ad_token
        else:
            self.__azure_ad_token_func = None

        self.__factory_queue = queue.Queue()
        self.__factory_thread = threading.Thread(name="openai-factory", target=self.__factory_main, daemon=True)
        self.__factory_thread.start()
        self.__warmed_up = False

        # Do not print info-level logs from the low-level frameworks
        # HTTPX logs every request by default
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("openai").setLevel(logging.WARNING)

    def supported_types(self) -> list[type]:

        supported_types = [openai.OpenAI]

        if openai.AzureOpenAI is not None:
            supported_types.append(openai.AzureOpenAI)

        return supported_types

    def supported_args(self) -> dict[str, type] | None:

        return {
            self.TIMEOUT_KEY: float,
            self.MAX_RETRIES_KEY: int
        }

    def create_client(self, client_type: type, **client_args) -> object:

        future = fut.Future()
        msg = (lambda: self._create_client_internal(client_type, **client_args), future)

        self.__factory_queue.put(msg)

        return future.result()

    def close_client(self, client: object):

        client.close()  # noqa

    def _create_client_internal(self, client_type: type, **client_args) -> object:

        if client_type == openai.OpenAI:
            if self.__sub_protocol:
                detail = f"The resource [{self.__resource_name }] is configured with sub protocol = [{self.__sub_protocol}]"
                raise _ex.ERuntimeValidation(f"Cannot create OpenAI client: {detail}")
            else:
                return self._create_client_std(**client_args)

        if openai.AzureOpenAI and client_type == openai.AzureOpenAI:
            if self.__sub_protocol != self.AZURE_SUB_PROTOCOL:
                detail = f"The resource [{self.__resource_name }] is configured with sub protocol = [{self.__sub_protocol}]"
                raise _ex.ERuntimeValidation(f"Cannot create Azure OpenAI client: {detail}")
            else:
                return self._create_client_azure(**client_args)

        raise _ex.EPluginNotAvailable(f"Client type [{client_type.__qualname__}] is not available in {self.__class__.__name__}")

    def _create_client_std(self, **client_args):

        std_args = self._build_std_args(**client_args)
        return openai.OpenAI(**std_args)

    def _create_client_azure(self, **client_args):

        azure_args = self._build_azure_args(**client_args)
        return openai.AzureOpenAI(**azure_args)

    def _build_std_args(self, **client_args):

        args = self._build_common_args(**client_args)

        if self.__api_key_func:
            api_key = self.__api_key_func()
        else:
            api_key = os.getenv(self.OPENAI_API_KEY)

        if api_key:
            args[self.API_KEY_KEY] = lambda: api_key

        return args

    def _build_azure_args(self, **client_args):

        args = self._build_common_args(**client_args)

        self._optional_arg(args, self.API_VERSION_KEY, self.__api_version)
        self._optional_arg(args, self.AZURE_ENDPOINT_KEY, self.__azure_endpoint)
        self._optional_arg(args, self.AZURE_DEPLOYMENT_KEY, self.__azure_deployment)

        if self.__api_key_func:
            api_key = self.__api_key_func()
        else:
            api_key = os.getenv(self.AZURE_OPENAI_API_KEY)

        if api_key:
            args[self.API_KEY_KEY] = api_key

        if self.__azure_ad_token_func:
            azure_ad_token = self.__azure_ad_token_func()
        else:
            azure_ad_token = os.getenv(self.AZURE_OPENAI_AD_TOKEN)

        if azure_ad_token is not None:
            args[self.AZURE_AD_TOKEN_KEY] = azure_ad_token

        return args

    def _build_common_args(self, **client_args):

        args = dict()

        self._optional_arg(args, self.ORGANIZATION_KEY, self.__organization)
        self._optional_arg(args, self.PROJECT_KEY, self.__project)
        self._optional_arg(args, self.BASE_URL_KEY, self.__base_url)

        if self.TIMEOUT_KEY in client_args:
            args[self.TIMEOUT_KEY] = min(client_args[self.TIMEOUT_KEY], self.__timeout)
        else:
            args[self.TIMEOUT_KEY] = self.__timeout

        if self.MAX_RETRIES_KEY in client_args:
            args[self.MAX_RETRIES_KEY] = min(client_args[self.MAX_RETRIES_KEY], self.__max_retries)
        else:
            args[self.MAX_RETRIES_KEY] = self.__max_retries

        return args

    @staticmethod
    def _optional_arg(args, key, value):

        if value is not None:
            args[key] = value

    def __factory_main(self):

        # OpenAI uses asyncio under the hood, even for synchronous clients
        # TRAC Actor threads have their own synchronization logic that interferes with AIO
        # These issues can be avoided by creating OpenAI clients on a dedicated factory thread
        # Initialization also happens on first use, so there has to be a warmup call as well

        while True:

            create_func, future = self.__factory_queue.get()
            client = None

            try:

                client = create_func()

                if not self.__warmed_up:
                    self.__warmup_client(client)
                    self.__warmed_up = True

                future.set_result(client)

            except Exception as e:

                if client is not None:
                    client.close()

                future.set_exception(e)

    def __warmup_client(self, client):

        self.__log.info("Warming up the OpenAI client...")

        model_list = client.models.list()
        model_count = len(model_list.data)

        self.__log.info(f"Warmup complete: Found {model_count} models (there may be more)")


if openai:
    _plugins.PluginManager.register_plugin(
        _external.IExternalSystem, OpenAIPlugin,
        protocols=["openai"])
