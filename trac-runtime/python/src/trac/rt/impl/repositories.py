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

import abc
import importlib
import typing as tp

import trac.rt.api as api
import trac.rt.metadata as meta
import trac.rt.config as config


class IModelLoader:

    @abc.abstractmethod
    def load_model(self, model_def: meta.ModelDefinition) -> api.TracModel.__class__:
        pass


class IntegratedModelLoader(IModelLoader):

    def __init__(self, repo_config: config.StorageConfig):
        self._repo_config = repo_config

    def load_model(self, model_def: meta.ModelDefinition) -> api.TracModel.__class__:

        entry_point_parts = model_def.entryPoint.rsplit(".", 1)
        module_name = entry_point_parts[0]
        class_name = entry_point_parts[1]

        model_module = importlib.import_module(module_name)
        model_class = model_module.__dict__[class_name]

        return model_class


class LocalModelLoader(IModelLoader):

    def __init__(self, repo_config: config.StorageConfig):
        self._repo_config = repo_config

    def load_model(self, model_def: meta.ModelDefinition) -> api.TracModel.__class__:
        raise NotImplementedError()


class GitModelLoader(IModelLoader):

    def __init__(self, repo_config: config.StorageConfig):
        self._repo_config = repo_config

    def load_model(self, model_def: meta.ModelDefinition) -> api.TracModel.__class__:
        raise NotImplementedError()


class Repositories:

    __repo_types: tp.Dict[str, tp.Callable[[config.RepositoryConfig], IModelLoader]] = {
        "integrated": IntegratedModelLoader,
        "local": LocalModelLoader
    }

    @classmethod
    def register_repo_type(cls, repo_type: str, loader_class: tp.Callable[[config.RepositoryConfig], IModelLoader]):
        cls.__repo_types[repo_type] = loader_class

    def __init__(self, sys_config: config.RuntimeConfig):

        self._loaders: tp.Dict[str, IModelLoader] = dict()

        for repo_name, repo_config in sys_config.repositories.items():

            if repo_config.repoType not in self.__repo_types:
                # TODO: Exception type
                raise RuntimeError(f"No model loader plugin is available for repository type '{repo_config.repoType}'")

            loader_class = self.__repo_types[repo_config.repoType]
            loader = loader_class(repo_config)
            self._loaders[repo_name] = loader

    def get_model_loader(self, repo_name: str) -> IModelLoader:

        loader = self._loaders.get(repo_name)

        if loader is None:
            # TODO: Exception type
            raise RuntimeError(f"Repository '{repo_name}' is unknown or not configured, no model loader available")

        return loader
