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

import logging
import typing as tp

# TRAC interfaces
import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

import pyarrow.fs as afs

try:
    # These dependencies are provided by the optional [azure] feature
    # For local development, pip install -r requirements_plugins.txt
    import azure.storage.blob as az_blob  # noqa
    import adlfs  # noqa
    __azure_available = True
except ImportError:
    adlfs = None
    __azure_available = False

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class AzureBlobStorageProvider(IStorageProvider):

    # This client depends on the Azure fsspec implementation, since there is no native implementation from Arrow
    # To enable it, the tracdap package must be installed with the optional [azure] feature

    # Current supported authentication mechanisms are "default" and "access_key"
    # Client always uses location mode = primary, version aware = False

    STORAGE_ACCOUNT_PROPERTY = "storageAccount"
    CONTAINER_PROPERTY = "container"
    PREFIX_PROPERTY = "prefix"

    CREDENTIALS_PROPERTY = "credentials"
    CREDENTIALS_DEFAULT = "default"
    CREDENTIALS_ACCESS_KEY = "access_key"

    ACCESS_KEY_PROPERTY = "accessKey"

    RUNTIME_FS_PROPERTY = "runtimeFs"
    RUNTIME_FS_AUTO = "auto"
    RUNTIME_FS_FSSPEC = "fsspec"
    RUNTIME_FS_DEFAULT = RUNTIME_FS_AUTO

    def __init__(self, properties: tp.Dict[str, str]):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

        self._runtime_fs = _helpers.get_plugin_property(
            properties, self.RUNTIME_FS_PROPERTY) \
            or self.RUNTIME_FS_DEFAULT

        # The Azure SDK is very verbose with logging
        # Avoid log noise by raising the log level for the Azure namespace
        azure_log = _helpers.logger_for_namespace("azure.core")
        azure_log.level = logging.WARNING

    def has_arrow_native(self) -> bool:
        return True

    def get_arrow_native(self) -> afs.SubTreeFileSystem:

        if self._runtime_fs == self.RUNTIME_FS_AUTO or self._runtime_fs == self.RUNTIME_FS_FSSPEC:
            azure_fs = self.create_fsspec()
        else:
            message = f"Requested runtime FS [{self._runtime_fs}] is not available for Azure storage"
            self._log.error(message)
            raise ex.EStartup(message)

        container = _helpers.get_plugin_property(self._properties, self.CONTAINER_PROPERTY)
        prefix = _helpers.get_plugin_property(self._properties, self.PREFIX_PROPERTY)

        if container is None or container.strip() == "":
            message = f"Missing required config property [{self.CONTAINER_PROPERTY}] for Azure blob storage"
            self._log.error(message)
            raise ex.EConfigParse(message)

        root_path = f"{container}/{prefix}" if prefix else container

        return afs.SubTreeFileSystem(root_path, azure_fs)

    def create_fsspec(self) -> afs.FileSystem:

        azure_fsspec_args = self.setup_client_args()
        azure_fsspec = adlfs.AzureBlobFileSystem(**azure_fsspec_args)

        return afs.PyFileSystem(afs.FSSpecHandler(azure_fsspec))

    def setup_client_args(self) -> tp.Dict[str, tp.Any]:

        client_args = dict()

        storage_account = _helpers.get_plugin_property(self._properties, self.STORAGE_ACCOUNT_PROPERTY)

        if storage_account is None or len(storage_account.strip()) == 0:
            message = f"Missing required config property [{self.STORAGE_ACCOUNT_PROPERTY}] for Azure blob storage"
            self._log.error(message)
            raise ex.EConfigParse(message)

        client_args["account_name"] = storage_account

        credentials = self.setup_credentials()
        client_args.update(credentials)

        return client_args

    def setup_credentials(self):

        # Only default (Google ADC) mechanism is supported
        # Arrow GCP FS does also support access tokens, but ADC is probably all we ever need

        mechanism = _helpers.get_plugin_property(self._properties, self.CREDENTIALS_PROPERTY)

        if mechanism is None or len(mechanism) == 0 or mechanism.lower() == self.CREDENTIALS_DEFAULT:
            self._log.info(f"Using [{self.CREDENTIALS_DEFAULT}] credentials mechanism")
            return {"anon": False}

        if mechanism == self.CREDENTIALS_ACCESS_KEY:

            self._log.info(f"Using [{self.CREDENTIALS_ACCESS_KEY}] credentials mechanism")

            access_key = _helpers.get_plugin_property(self._properties, self.ACCESS_KEY_PROPERTY)

            if access_key is None or len(access_key.strip()) == 0:
                message = f"Missing required config property [{self.ACCESS_KEY_PROPERTY}] for Azure blob storage"
                raise ex.EConfigParse(message)

            return {"account_key": access_key}

        message = f"Unrecognised credentials mechanism: [{mechanism}]"
        self._log.error(message)
        raise ex.EStartup(message)


# Only register the plugin if the [azure] feature is available
if __azure_available:
    plugins.PluginManager.register_plugin(IStorageProvider, AzureBlobStorageProvider, ["BLOB"])
