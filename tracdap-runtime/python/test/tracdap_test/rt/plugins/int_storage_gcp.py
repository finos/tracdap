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

import os
import uuid

from tracdap_test.rt.suites.file_storage_suite import *

import tracdap.rt.config as cfg
import tracdap.rt.ext.plugins as plugins
import tracdap.rt._impl.util as util  # noqa
import tracdap.rt._impl.storage as storage  # noqa
import tracdap.rt._plugins.storage_aws as storage_aws  # noqa

import pyarrow.fs as pa_fs

plugins.PluginManager.register_core_plugins()

try:
    __arrow_available = pa_fs.GcsFileSystem is not None
except ImportError:
    __arrow_available = False


@unittest.skipIf(not __arrow_available, "Arrow GCS file system is not available on this platform")
class GcsArrowStorageTest(unittest.TestCase, FileOperationsTestSuite, FileReadWriteTestSuite):

    suite_storage_prefix = f"runtime_storage_test_suite_{uuid.uuid4()}"
    suite_storage: storage.IFileStorage

    @classmethod
    def setUpClass(cls) -> None:

        suite_properties = cls._properties_from_env()
        suite_config = cfg.PluginConfig(protocol="GCS", properties=suite_properties)

        cls.suite_storage = cls._storage_from_config(suite_config, "tracdap_ci_storage_setup")
        cls.suite_storage.mkdir(cls.suite_storage_prefix)

        test_properties = cls._properties_from_env()
        test_properties["prefix"] = cls.suite_storage_prefix
        test_config = cfg.PluginConfig(protocol="GCS", properties=test_properties)

        cls.storage = cls._storage_from_config(test_config, "tracdap_ci_storage")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.suite_storage.rmdir(cls.suite_storage_prefix)

    @staticmethod
    def _properties_from_env():

        properties = dict()
        properties["runtimeFs"] = "arrow"
        properties["bucket"] = os.getenv("TRAC_GCP_BUCKET")

        credentials = os.getenv("TRAC_GCP_CREDENTIALS")

        if credentials is not None:
            properties["credentials"] = credentials

        if credentials == "access_token":
            properties["accessToken"] = os.getenv("TRAC_GCP_ACCESS_TOKEN")
            properties["accessTokenExpiry"] = os.getenv("TRAC_GCP_ACCESS_TOKEN_EXPIRY")

        return properties

    @staticmethod
    def _storage_from_config(storage_config: cfg.PluginConfig, storage_key: str):

        sys_config = cfg.RuntimeConfig()
        sys_config.storage = cfg.StorageConfig()
        sys_config.storage.buckets[storage_key] = storage_config

        manager = storage.StorageManager(sys_config)

        return manager.get_file_storage(storage_key)


class GcsFsspecStorageTest(unittest.TestCase, FileOperationsTestSuite, FileReadWriteTestSuite):

    suite_storage_prefix = f"runtime_storage_test_suite_{uuid.uuid4()}"
    suite_storage: storage.IFileStorage

    @classmethod
    def setUpClass(cls) -> None:

        suite_properties = cls._properties_from_env()
        suite_config = cfg.PluginConfig(protocol="GCS", properties=suite_properties)

        cls.suite_storage = cls._storage_from_config(suite_config, "tracdap_ci_storage_setup")
        cls.suite_storage.mkdir(cls.suite_storage_prefix)

        test_properties = cls._properties_from_env()
        test_properties["prefix"] = cls.suite_storage_prefix
        test_config = cfg.PluginConfig(protocol="GCS", properties=test_properties)

        cls.storage = cls._storage_from_config(test_config, "tracdap_ci_storage")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.suite_storage.rmdir(cls.suite_storage_prefix)

    @staticmethod
    def _properties_from_env():

        properties = dict()
        properties["runtimeFs"] = "fsspec"
        properties["bucket"] = os.getenv("TRAC_GCP_BUCKET")

        credentials = os.getenv("TRAC_GCP_CREDENTIALS")

        if credentials is not None:
            properties["credentials"] = credentials

        if credentials == "access_token":
            properties["accessToken"] = os.getenv("TRAC_GCP_ACCESS_TOKEN")
            properties["accessTokenExpiry"] = os.getenv("TRAC_GCP_ACCESS_TOKEN_EXPIRY")

        return properties

    @staticmethod
    def _storage_from_config(storage_config: cfg.PluginConfig, storage_key: str):

        sys_config = cfg.RuntimeConfig()
        sys_config.storage = cfg.StorageConfig()
        sys_config.storage.buckets[storage_key] = storage_config

        manager = storage.StorageManager(sys_config)

        return manager.get_file_storage(storage_key)
