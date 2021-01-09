#  Copyright 2021 Accenture Global Solutions Limited
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


class StorageConfig:

    def __init__(self,
                 storageType: str,  # noqa
                 storageConfig: tp.Dict[str, str] = {}):  # noqa

        self.storageType = storageType
        self.storageConfig = storageConfig


class StorageSettings:

    def __init__(self,
                 defaultStorage: str,  # noqa
                 defaultFormat: str):  # noqa

        self.defaultStorage = defaultStorage
        self.defaultFormat = defaultFormat


class SparkSettings:

    def __init__(self,
                 sparkConfig: tp.Dict[str, str]):  # noqa

        self.sparkConfig = sparkConfig


class RuntimeConfig:

    def __init__(self,
                 storage: tp.Dict[str, StorageConfig],
                 storageSettings: StorageSettings,  # noqa
                 sparkSettings: SparkSettings):  # noqa

        self.storage = storage
        self.storageSettings = storageSettings
        self.sparkSettings = sparkSettings


class JobConfig:

    def __init__(self,
                 parameters: tp.Dict[str, str],
                 inputs: tp.Dict[str, tp.Dict[str, str]],
                 outputs: tp.Dict[str, str]):
        self.parameters = parameters
        self.inputs = inputs
        self.outputs = outputs
