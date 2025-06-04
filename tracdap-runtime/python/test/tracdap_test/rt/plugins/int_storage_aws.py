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
import uuid

from tracdap_test.rt.suites.file_storage_suite import *

import tracdap.rt.config as cfg
import tracdap.rt.ext.plugins as plugins
import tracdap.rt._impl.core.logging as log  # noqa
import tracdap.rt._impl.core.storage as storage  # noqa
import tracdap.rt._plugins.storage_aws as storage_aws  # noqa

log.configure_logging()
plugins.PluginManager.register_core_plugins()


class S3ArrowStorageTest(unittest.TestCase, FileOperationsTestSuite, FileReadWriteTestSuite):

    suite_storage_prefix = f"runtime_storage_test_suite_{uuid.uuid4()}"
    suite_storage: storage.IFileStorage

    @classmethod
    def setUpClass(cls) -> None:

        suite_properties = cls._properties_from_env()
        suite_storage_config = cfg.PluginConfig(protocol="S3", properties=suite_properties)

        cls.suite_storage = cls._storage_from_config(suite_storage_config, "tracdap_ci_storage_setup")
        cls.suite_storage.mkdir(cls.suite_storage_prefix)

        test_properties = cls._properties_from_env()
        test_properties["arrowNativeFs"] = "true"
        test_properties["prefix"] = cls.suite_storage_prefix
        test_storage_config = cfg.PluginConfig(protocol="S3", properties=test_properties)

        cls.storage = cls._storage_from_config(test_storage_config, "tracdap_ci_storage")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.suite_storage.rmdir(cls.suite_storage_prefix)

    @staticmethod
    def _properties_from_env():

        properties = dict()
        properties["runtimeFs"] = "arrow"
        properties["region"] = os.getenv("TRAC_AWS_REGION")
        properties["bucket"] = os.getenv("TRAC_AWS_BUCKET")

        credentials = os.getenv("TRAC_AWS_CREDENTIALS")

        if credentials:
            properties["credentials"] = credentials

        if credentials == "static":
            properties["accessKeyId"] = os.getenv("TRAC_AWS_ACCESS_KEY_ID")
            properties["secretAccessKey"] = os.getenv("TRAC_AWS_SECRET_ACCESS_KEY")

        return properties

    @staticmethod
    def _storage_from_config(storage_config: cfg.PluginConfig, storage_key: str):

        sys_config = cfg.RuntimeConfig()
        sys_config.properties["storage.default.location"] = storage_key
        sys_config.resources[storage_key] = storage_config

        manager = storage.StorageManager(sys_config)

        return manager.get_file_storage(storage_key)
