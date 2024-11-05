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

util.configure_logging()
plugins.PluginManager.register_core_plugins()


class BlobFsspecStorageTest(unittest.TestCase, FileOperationsTestSuite, FileReadWriteTestSuite):

    suite_storage_prefix = f"runtime_storage_test_suite_{uuid.uuid4()}"
    suite_storage: storage.IFileStorage

    @classmethod
    def setUpClass(cls) -> None:

        suite_properties = cls._properties_from_env()
        suite_storage_config = cfg.PluginConfig(protocol="BLOB", properties=suite_properties)

        cls.suite_storage = cls._storage_from_config(suite_storage_config, "tracdap_ci_storage_setup")
        cls.suite_storage.mkdir(cls.suite_storage_prefix)

        test_properties = cls._properties_from_env()
        test_properties["prefix"] = cls.suite_storage_prefix
        test_storage_config = cfg.PluginConfig(protocol="BLOB", properties=test_properties)

        cls.storage = cls._storage_from_config(test_storage_config, "tracdap_ci_storage")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.suite_storage.rmdir(cls.suite_storage_prefix)

    @staticmethod
    def _properties_from_env():

        properties = dict()
        properties["storageAccount"] = os.getenv("TRAC_AZURE_STORAGE_ACCOUNT")
        properties["container"] = os.getenv("TRAC_AZURE_CONTAINER")

        credentials = os.getenv("TRAC_AZURE_CREDENTIALS")

        if credentials:
            properties["credentials"] = credentials

        if credentials == "access_key":
            properties["accessKey"] = os.getenv("TRAC_AZURE_ACCESS_KEY")

        return properties

    @staticmethod
    def _storage_from_config(storage_config: cfg.PluginConfig, storage_key: str):

        sys_config = cfg.RuntimeConfig()
        sys_config.storage = cfg.StorageConfig()
        sys_config.storage.buckets[storage_key] = storage_config

        manager = storage.StorageManager(sys_config)

        return manager.get_file_storage(storage_key)
