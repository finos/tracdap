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
import typing as tp
import pathlib

import trac.rt.metadata as _meta
import trac.rt.config as _cfg
import trac.rt.exceptions as _ex
import trac.rt.impl.util as _util


class IModelRepository:

    @abc.abstractmethod
    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_path: tp.Union[str, pathlib.Path]) \
            -> tp.Union[pathlib.Path, str, None]:

        pass


class IntegratedModelRepo(IModelRepository):

    def __init__(self, repo_config: _cfg.RepositoryConfig):
        self._repo_config = repo_config

    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_path: tp.Union[str, pathlib.Path]) \
            -> None:

        # For the integrated repo there is nothing to check out

        return None


class LocalModelLoader(IModelRepository):

    def __init__(self, repo_config: _cfg.RepositoryConfig):
        self._repo_config = repo_config

    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_path: tp.Union[str, pathlib.Path]) \
            -> pathlib.Path:

        # For local repos, checkout is a no-op since the model is already local
        # Return the existing full path to the model package as the checkout dir

        checkout_path = pathlib.Path(self._repo_config.repoUrl).joinpath(model_def.path)

        return checkout_path


class GitModelLoader(IModelRepository):

    def __init__(self, repo_config: _cfg.RepositoryConfig):
        self._repo_config = repo_config

    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_path: tp.Union[str, pathlib.Path]) \
            -> pathlib.Path:

        raise NotImplementedError()


class RepositoryManager:

    __repo_types: tp.Dict[str, tp.Callable[[_cfg.RepositoryConfig], IModelRepository]] = {
        "integrated": IntegratedModelRepo,
        "local": LocalModelLoader
    }

    @classmethod
    def register_repo_type(cls, repo_type: str, loader_class: tp.Callable[[_cfg.RepositoryConfig], IModelRepository]):
        cls.__repo_types[repo_type] = loader_class

    def __init__(self, sys_config: _cfg.RuntimeConfig):

        self._log = _util.logger_for_object(self)
        self._loaders: tp.Dict[str, IModelRepository] = dict()

        for repo_name, repo_config in sys_config.repositories.items():

            if repo_config.repoType not in self.__repo_types:

                msg = f"Model repository type [{repo_config.repoType}] is not recognised" \
                    + " (this could indicate a missing model loader plugin)"

                self._log.error(msg)
                raise _ex.EModelRepoConfig(msg)

            loader_class = self.__repo_types[repo_config.repoType]
            loader = loader_class(repo_config)
            self._loaders[repo_name] = loader

    def get_repository(self, repo_name: str) -> IModelRepository:

        loader = self._loaders.get(repo_name)

        if loader is None:

            msg = f"Model repository [{repo_name}] is unknown or not configured" \
                + " (this could indicate a missing repository entry in the system config)"

            self._log.error(msg)
            raise _ex.EModelRepoConfig(msg)

        return loader
