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

import typing as _tp

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _config
import tracdap.rt.exceptions as _ex
import tracdap.rt.ext.plugins as _plugins
import tracdap.rt.ext.external as _external
import tracdap.rt._impl.core.models as _models
import tracdap.rt._impl.core.repos as _repos
import tracdap.rt._impl.core.storage as _storage
import tracdap.rt._impl.core.logging as _log
import tracdap.rt._impl.core.validation as _val


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
        self.__sys_config = sys_config
        self.__storage = storage_manager
        self.__repositories = repository_manager
        self.__models = model_loader
        self.__external_systems = self._load_external_systems(sys_config)

    @classmethod
    def _load_external_systems(cls, sys_config: _config.RuntimeConfig) -> _tp.Dict[str, _external.IExternalSystem]:

        external_systems = dict()

        for resource_name, resource_def in sys_config.resources.items():
            if resource_def.resourceType == _meta.ResourceType.EXTERNAL_SYSTEM:

                config = _config.PluginConfig(
                    protocol=resource_def.protocol,
                    publicProperties=resource_def.publicProperties,
                    properties=resource_def.properties,
                    secrets=resource_def.secrets)

                system = _plugins.PluginManager.load_plugin(_external.IExternalSystem, config)
                external_systems[resource_name] = _val.plugin_validation_wrapper(_external.IExternalSystem, system)

        return external_systems

    def get_resource_definition(self, resource_key: str) -> _tp.Optional[_meta.ResourceDefinition]:
        return self.__sys_config.resources.get(resource_key)

    def get_storage(self) -> _storage.StorageManager:
        return self.__storage

    def get_repositories(self) -> _repos.RepositoryManager:
        return self.__repositories

    def get_models(self):
        return self.__models

    def get_external_system(self, system_name: str) -> _external.IExternalSystem:

        system = self.__external_systems.get(system_name)

        if system is None:
            raise _ex.ETracInternal(f"External system [{system_name}] is not available")

        return system
