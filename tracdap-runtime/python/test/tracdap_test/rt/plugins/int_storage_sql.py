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
import os
import unittest

import tracdap.rt.config as _cfg
import tracdap.rt.ext.plugins as _plugins
import tracdap.rt._impl.config_parser as _cfg_p  # noqa
import tracdap.rt._impl.core.logging as _log  # noqa
import tracdap.rt._impl.storage as _storage  # noqa

from tracdap_test.rt.suites.data_storage_suite_2 import DataStorageSuite2


class SqlDataStorageTest(unittest.TestCase, DataStorageSuite2):

    log: logging.Logger

    @classmethod
    def setUpClass(cls) -> None:

        _log.configure_logging()
        _plugins.PluginManager.register_core_plugins()

        cls.log = _log.logger_for_class(cls)

        sys_config_path = os.getenv("TRAC_RT_SYS_CONFIG")
        config_manager = _cfg_p.ConfigManager.for_root_config(sys_config_path)

        sys_config: _cfg.RuntimeConfig = config_manager.load_root_object(
            _cfg.RuntimeConfig, config_file_name="system")

        manager = _storage.StorageManager(sys_config)

        config_key = next(iter(sys_config.storage.external.keys()))
        config_block = sys_config.storage.external[config_key]
        dialect = config_block.properties.get("dialect")

        cls.storage = manager.get_data_storage(config_key, external=True)
        cls.backend = dialect.lower() if dialect is not None else None

    @classmethod
    def tearDownClass(cls) -> None:

        pass
