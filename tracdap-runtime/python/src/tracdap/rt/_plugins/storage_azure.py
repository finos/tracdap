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

import typing as tp
import datetime as dt

import tracdap.rt.exceptions as ex

# Import storage interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

from pyarrow import fs as afs

try:
    import adlfs
    azure_available = True
except ImportError:
    adlfs = None
    azure_available = False

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class AzureBlobStorageProvider(IStorageProvider):

    CONTAINER_PROPERTY = "container"
    PREFIX_PROPERTY = "prefix"
    REGION_PROPERTY = "region"
    ENDPOINT_PROPERTY = "endpoint"

    CREDENTIALS_PROPERTY = "credentials"

    def __init__(self, properties: tp.Dict[str, str]):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

    def has_arrow_native(self) -> bool:
        return True

    def get_arrow_native(self) -> afs.SubTreeFileSystem:

        azure_args = self.setup_client_args()
        azure_fs = adlfs.AzureBlobFileSystem(**azure_args)

        container = _helpers.get_plugin_property(self._properties, self.CONTAINER_PROPERTY)

        if container is None or len(container.strip()) == 0:
            message = f"Missing required config property [{self.CONTAINER_PROPERTY}] for Azure blob storage"
            self._log.error(message)
            raise ex.EConfigParse(message)

        protocol = "az"
        prefix = _helpers.get_plugin_property(self._properties, self.PREFIX_PROPERTY)

        root_path = f"{protocol}://{container}/{prefix}" if prefix else container

        return afs.SubTreeFileSystem(root_path, azure_fs)

    def setup_client_args(self) -> tp.Dict[str, tp.Any]:

        client_args = dict()

        region = _helpers.get_plugin_property(self._properties, self.REGION_PROPERTY)
        endpoint = _helpers.get_plugin_property(self._properties, self.ENDPOINT_PROPERTY)

        if region is not None:
            client_args["default_bucket_location"] = region

        if endpoint is not None:
            client_args["endpoint_override"] = endpoint

        credentials = self.setup_credentials()
        client_args.update(credentials)

        return client_args

    def setup_credentials(self):

        # Only default (Google ADC) mechanism is supported
        # Arrow GCP FS does also support access tokens, but ADC is probably all we ever need

        mechanism = _helpers.get_plugin_property(self._properties, self.CREDENTIALS_PROPERTY)

        if mechanism is None or len(mechanism) == 0 or mechanism.lower() == self.CREDENTIALS_ADC:
            self._log.info(f"Using [{self.CREDENTIALS_ADC}] credentials mechanism")
            return dict()

        if mechanism == self.CREDENTIALS_ACCESS_TOKEN:

            access_token = _helpers.get_plugin_property(self._properties, self.ACCESS_TOKEN)
            access_token_expiry = _helpers.get_plugin_property(self._properties, self.ACCESS_TOKEN_EXPIRY)

            if access_token is None or len(access_token.strip()) == 0:
                message = f"Missing required config property [{self.ACCESS_TOKEN}] for GCP storage"
                raise ex.EConfigParse(message)

            if access_token_expiry is None:
                access_token_expiry = self.ACCESS_TOKEN_EXPIRY_DEFAULT

            expiry_timestamp = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=float(access_token_expiry))

            return {"access_token": access_token, "credential_token_expiration": expiry_timestamp}

        message = f"Unrecognised credentials mechanism: [{mechanism}]"
        self._log.error(message)
        raise ex.EStartup(message)


if azure_available:
    plugins.PluginManager.register_plugin(IStorageProvider, AzureBlobStorageProvider, ["BLOB"])
