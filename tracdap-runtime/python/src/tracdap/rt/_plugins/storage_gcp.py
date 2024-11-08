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

import typing as tp
import datetime as dt

import tracdap.rt.exceptions as ex

# Import storage interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

from pyarrow import fs as pa_fs

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


try:
    # These dependencies are provided by the optional [gcp] feature
    # For local development, pip install -r requirements_plugins.txt
    import google.cloud.storage as gcs  # noqa
    import gcsfs  # noqa
    __gcp_available = True
except ImportError:
    gcs = None
    gcsfs = None
    __gcp_available = False


class GcpStorageProvider(IStorageProvider):

    BUCKET_PROPERTY = "bucket"
    PREFIX_PROPERTY = "prefix"
    REGION_PROPERTY = "region"
    ENDPOINT_PROPERTY = "endpoint"

    CREDENTIALS_PROPERTY = "credentials"
    CREDENTIALS_ADC = "adc"
    CREDENTIALS_ACCESS_TOKEN = "access_token"

    ACCESS_TOKEN = "accessToken"
    ACCESS_TOKEN_EXPIRY = "accessTokenExpiry"
    ACCESS_TOKEN_EXPIRY_DEFAULT = 3600

    RUNTIME_FS_PROPERTY = "runtimeFs"
    RUNTIME_FS_AUTO = "auto"
    RUNTIME_FS_ARROW = "arrow"
    RUNTIME_FS_FSSPEC = "fsspec"
    RUNTIME_FS_DEFAULT = RUNTIME_FS_AUTO

    ARROW_CLIENT_ARGS = {
        REGION_PROPERTY: "default_bucket_location",
        ENDPOINT_PROPERTY: "endpoint_override"
    }

    FSSPEC_CLIENT_ARGS = {
        REGION_PROPERTY: "default_location",
        ENDPOINT_PROPERTY: "endpoint_url"
    }

    try:
        __arrow_available = pa_fs.GcsFileSystem is not None
    except ImportError:
        __arrow_available = False

    def __init__(self, properties: tp.Dict[str, str]):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

        self._runtime_fs = _helpers.get_plugin_property(
            properties, self.RUNTIME_FS_PROPERTY) \
            or self.RUNTIME_FS_DEFAULT

    def has_arrow_native(self) -> bool:
        return True

    def get_arrow_native(self) -> pa_fs.SubTreeFileSystem:

        if self._runtime_fs == self.RUNTIME_FS_AUTO:
            gcs_fs = self.create_arrow() if self.__arrow_available else self.create_fsspec()
        elif self._runtime_fs == self.RUNTIME_FS_ARROW:
            gcs_fs = self.create_arrow()
        elif self._runtime_fs == self.RUNTIME_FS_FSSPEC:
            gcs_fs = self.create_fsspec()
        else:
            message = f"Requested runtime FS [{self._runtime_fs}] is not available for GCP storage"
            self._log.error(message)
            raise ex.EStartup(message)

        bucket = _helpers.get_plugin_property(self._properties, self.BUCKET_PROPERTY)
        prefix = _helpers.get_plugin_property(self._properties, self.PREFIX_PROPERTY)

        if bucket is None or len(bucket.strip()) == 0:
            message = f"Missing required config property [{self.BUCKET_PROPERTY}] for GCP storage"
            self._log.error(message)
            raise ex.EConfigParse(message)

        root_path = f"{bucket}/{prefix}" if prefix else bucket

        return pa_fs.SubTreeFileSystem(root_path, gcs_fs)

    def create_arrow(self) -> pa_fs.FileSystem:

        gcs_arrow_args = self.setup_client_args(self.ARROW_CLIENT_ARGS)

        return pa_fs.GcsFileSystem(**gcs_arrow_args)

    def create_fsspec(self) -> pa_fs.FileSystem:

        gcs_fsspec_args = self.setup_client_args(self.FSSPEC_CLIENT_ARGS)
        gcs_fsspec = gcsfs.GCSFileSystem(**gcs_fsspec_args)

        return pa_fs.PyFileSystem(pa_fs.FSSpecHandler(gcs_fsspec))

    def setup_client_args(self, arg_mapping: tp.Dict[str, str]) -> tp.Dict[str, tp.Any]:

        client_args = dict()

        region = _helpers.get_plugin_property(self._properties, self.REGION_PROPERTY)
        endpoint = _helpers.get_plugin_property(self._properties, self.ENDPOINT_PROPERTY)

        if region is not None:
            region_key = arg_mapping[self.REGION_PROPERTY]
            client_args[region_key] = region

        if endpoint is not None:
            endpoint_key = arg_mapping[self.ENDPOINT_PROPERTY]
            client_args[endpoint_key] = endpoint

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

            self._log.info(f"Using [{self.CREDENTIALS_ACCESS_TOKEN}] credentials mechanism")

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


if __gcp_available:
    plugins.PluginManager.register_plugin(IStorageProvider, GcpStorageProvider, ["GCS"])
