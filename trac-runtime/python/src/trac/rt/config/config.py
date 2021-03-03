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

from __future__ import annotations

import typing as tp
import dataclasses as dc

import trac.rt.metadata as meta


def _empty(factory: tp.Callable):
    return dc.field(default_factory=factory)


@dc.dataclass
class RepositoryConfig:

    repoType: str
    repoUrl: str = None
    repoSettings: tp.Dict[str, str] = _empty(dict)


@dc.dataclass
class StorageConfig:

    storageType: str
    storageConfig: tp.Dict[str, str] = _empty(dict)


@dc.dataclass
class StorageSettings:

    defaultStorage: str
    defaultFormat: str


@dc.dataclass
class SparkSettings:

    sparkConfig: tp.Dict[str, str] = _empty(dict)


@dc.dataclass
class SystemConfig:

    repositories: tp.Dict[str, RepositoryConfig] = _empty(dict)
    storage: tp.Dict[str, StorageConfig] = _empty(dict)
    storageSettings: tp.Optional[StorageSettings] = None
    sparkSettings: tp.Optional[SparkSettings] = None


@dc.dataclass
class JobConfig:

    job_id: tp.Optional[str] = None

    target: tp.Optional[str] = None
    parameters: tp.Dict[str, tp.Any] = _empty(dict)
    inputs: tp.Dict[str, str] = _empty(dict)
    outputs: tp.Dict[str, str] = _empty(dict)

    objects: tp.Dict[str, meta.ObjectDefinition] = _empty(dict)

    job: tp.Optional[meta.JobDefinition] = None
