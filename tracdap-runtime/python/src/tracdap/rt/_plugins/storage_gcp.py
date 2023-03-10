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

import tracdap.rt.exceptions as ex

# Import storage interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

from pyarrow import fs as afs

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class GcpStorageProvider(IStorageProvider):

    ARROW_NATIVE_FS_PROPERTY = "arrowNativeFs"
    ARROW_NATIVE_FS_DEFAULT = False

    BUCKET_PROPERTY = "bucket"
    PREFIX_PROPERTY = "prefix"
    REGION_PROPERTY = "region"
    ENDPOINT_PROPERTY = "endpoint"

    CREDENTIALS_PROPERTY = "credentials"
    CREDENTIALS_DEFAULT = "default"
    CREDENTIALS_STATIC = "static"

    def __init__(self, properties: tp.Dict[str, str]):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

        self._arrow_native = _helpers.get_plugin_property_boolean(
            properties, self.ARROW_NATIVE_FS_PROPERTY, self.ARROW_NATIVE_FS_DEFAULT)

    def has_arrow_native(self) -> bool:
        return True

    def get_arrow_native(self) -> afs.SubTreeFileSystem:

        gcs_args = self.setup_client_args()
        gcs_fs = afs.GcsFileSystem(**gcs_args)

        bucket = _helpers.get_plugin_property(self._properties, self.BUCKET_PROPERTY)
        prefix = _helpers.get_plugin_property(self._properties, self.PREFIX_PROPERTY) or ""
        root_path = f"{bucket}/{prefix}"

        return afs.SubTreeFileSystem(root_path, gcs_fs)

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

        mechanism = _helpers.get_plugin_property(self._properties, self.CREDENTIALS_PROPERTY)

        if mechanism is None or len(mechanism) == 0 or mechanism.lower() == self.CREDENTIALS_DEFAULT:
            self._log.info(f"Using [{self.CREDENTIALS_DEFAULT}] credentials mechanism")
            return dict()

        if mechanism.lower() == self.CREDENTIALS_STATIC:

            raise NotImplementedError("GCP static credentials not implemented yet")

        message = f"Unrecognised credentials mechanism: [{mechanism}]"
        self._log.error(message)
        raise ex.EStartup(message)


plugins.PluginManager.register_plugin(IStorageProvider, GcpStorageProvider, ["GCS"])
