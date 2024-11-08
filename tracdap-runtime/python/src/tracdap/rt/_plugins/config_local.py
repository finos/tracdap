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

import pathlib as pathlib
import typing as tp

import tracdap.rt.ext.plugins as plugins
import tracdap.rt.exceptions as ex

from tracdap.rt.ext.config import *


class LocalConfigLoader(IConfigLoader):

    # Properties dict will be empty for config plugins
    def __init__(self, properties: tp.Dict[str, str]):  # noqa
        pass

    def has_config_file(self, config_url: str) -> bool:
        if config_url.startswith("file:"):
            config_url = config_url[5:]
        config_path = pathlib.Path(config_url).resolve()
        return config_path.exists() and config_path.is_file()

    def load_config_file(self, config_url: str) -> bytes:
        if config_url.startswith("file:"):
            config_url = config_url[5:]
        config_path = pathlib.Path(config_url).resolve()
        return config_path.read_bytes()

    def has_config_dict(self, config_url: str) -> bool:
        return False

    def load_config_dict(self, config_url: str) -> dict:
        raise ex.ETracInternal("Local config loader does not support loading objects")


plugins.PluginManager.register_plugin(IConfigLoader, LocalConfigLoader, ["LOCAL", "file"])
