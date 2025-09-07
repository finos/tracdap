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

try:
    import openai
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

    AZURE_AD_TOKEN_PROVIDER = "azure_ad_token_provider"

    def __init__(self, resource_name: str, config: _cfg.PluginConfig):

        self.__resource_name = resource_name
        self.__protocol = config.protocol
        self.__sub_protocol = config.subProtocol

        self.__organization = _util.read_plugin_config(config, self.ORGANIZATION_KEY, optional=True)
        self.__project = _util.read_plugin_config(config, self.PROJECT_KEY, optional=True)
        self.__base_url = _util.read_plugin_config(config, self.BASE_URL_KEY, optional=True)
        self.__timeout = _util.read_plugin_config(config, self.TIMEOUT_KEY, default=openai.DEFAULT_TIMEOUT, convert=float)
        self.__max_retries = _util.read_plugin_config(config, self.MAX_RETRIES_KEY, default=openai.DEFAULT_MAX_RETRIES, convert=int)

        self.__api_version = _util.read_plugin_config(config, self.API_VERSION_KEY, optional=True)
        self.__azure_endpoint = _util.read_plugin_config(config, self.AZURE_ENDPOINT_KEY, optional=True)
        self.__azure_deployment = _util.read_plugin_config(config, self.AZURE_DEPLOYMENT_KEY, optional=True)

        if _util.has_plugin_config(config, self.API_KEY_KEY):
            self.__api_key_func = lambda: _util.read_plugin_config(config, self.ORGANIZATION_KEY)
        else:
            self.__api_key_func = None

        if _util.has_plugin_config(config, self.AZURE_AD_TOKEN_KEY):
            self.__azure_ad_token_func = lambda: _util.read_plugin_config(config, self.AZURE_AD_TOKEN_KEY)
        else:
            self.__azure_ad_token_func = None

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

        std_args = self._build_common_args(**client_args)
        return openai.OpenAI(**std_args)

    def _create_client_azure(self, **client_args):

        azure_args = self._build_azure_args(**client_args)
        return openai.AzureOpenAI(**azure_args)

    def _build_common_args(self, **client_args):

        args = dict()

        if self.__api_key_func is not None:
            args[self.API_KEY_KEY] = self.__api_key_func
        else:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key is not None:
                args[self.API_KEY_KEY] = lambda: api_key

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

    def _build_azure_args(self, **client_args):

        args = self._build_common_args(**client_args)

        self._optional_arg(args, self.API_VERSION_KEY, self.__api_version)
        self._optional_arg(args, self.AZURE_ENDPOINT_KEY, self.__azure_endpoint)
        self._optional_arg(args, self.AZURE_DEPLOYMENT_KEY, self.__azure_deployment)
        self._optional_arg(args, self.AZURE_AD_TOKEN_PROVIDER, self.__azure_ad_token_func)

        return args

    @staticmethod
    def _optional_arg(args, key, value):

        if value is not None:
            args[key] = value

    def close_client(self, client: object):

        client.close()  # noqa


if openai:
    _plugins.PluginManager.register_plugin(
        _external.IExternalSystem, OpenAIPlugin,
        protocols=["openai"])
