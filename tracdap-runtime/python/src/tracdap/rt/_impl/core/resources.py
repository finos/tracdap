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

import tracdap.rt.config as _config
import tracdap.rt._impl.core.models as _models
import tracdap.rt._impl.core.repos as _repos
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.logging as _log


class ResourceManager:

    # Convenience class to hold all the resource managers and external systems
    # These are process level resources, used together in the engine layer to set up jobs
    # Job-level resources (e.g. log provider) are handled separately

    def __init__(
            self, sys_config: _config.RuntimeConfig,
            storage_manager: _storage.StorageManager,
            repository_manager: _repos.RepositoryManager,
            model_loader: _models.ModelLoader):

        self.__log = _log.logger_for_object(self)
        self.__storage = storage_manager
        self.__repositories = repository_manager
        self.__models = model_loader

    def get_storage(self) -> _storage.StorageManager:
        return self.__storage

    def get_repositories(self) -> _repos.RepositoryManager:
        return self.__repositories

    def get_models(self):
        return self.__models
