#  Copyright 2022 Accenture Global Solutions Limited
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
import subprocess as sp
import time

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.util as _util


class IModelRepository:

    @abc.abstractmethod
    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: tp.Union[str, pathlib.Path]) \
            -> tp.Union[pathlib.Path, str, None]:

        pass


class IntegratedSource(IModelRepository):

    def __init__(self, repo_config: _cfg.RepositoryConfig):
        self._repo_config = repo_config

    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: tp.Union[str, pathlib.Path]) \
            -> None:

        # For the integrated repo there is nothing to check out

        return None


class LocalRepository(IModelRepository):

    def __init__(self, repo_config: _cfg.RepositoryConfig):
        self._repo_config = repo_config

    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: tp.Union[str, pathlib.Path]) \
            -> pathlib.Path:

        # For local repos, checkout is a no-op since the model is already local
        # Return the existing full path to the model package as the checkout dir

        checkout_path = pathlib.Path(self._repo_config.repoUrl).joinpath(model_def.path)

        return checkout_path


class GitRepository(IModelRepository):

    GIT_TIMEOUT_SECONDS = 30

    def __init__(self, repo_config: _cfg.RepositoryConfig):
        self._repo_config = repo_config
        self._log = _util.logger_for_object(self)

    def checkout_model(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: tp.Union[str, pathlib.Path]) \
            -> pathlib.Path:

        repo_dir = pathlib.Path(checkout_dir) \
                .joinpath(model_def.repository) \
                .joinpath(model_def.version) \
                .resolve()

        self._log.info(f"Git checkout {model_def.repository} {model_def.version} -> {repo_dir}")

        repo_dir.mkdir(mode=750, parents=True, exist_ok=False)

        git_cli = ["git", "-C", str(repo_dir)]

        git_cmds = [
            ["init"],
            ["remote", "add", "origin", self._repo_config.repoUrl],
            ["fetch", "--depth=1", "origin", model_def.version],
            ["reset", "--hard", "FETCH_HEAD"]]

        for git_cmd in git_cmds:

            self._log.info(f"git {' '.join(git_cmd)}")

            cmd = [*git_cli, *git_cmd]
            cmd_result = sp.run(cmd, cwd=repo_dir, stdout=sp.PIPE, stderr=sp.PIPE, timeout=self.GIT_TIMEOUT_SECONDS)

            if cmd_result.returncode != 0:
                time.sleep(1)
                self._log.warning(f"git {' '.join(git_cmd)} (retrying)")
                cmd_result = sp.run(cmd, cwd=repo_dir, stdout=sp.PIPE, stderr=sp.PIPE, timeout=self.GIT_TIMEOUT_SECONDS)

            cmd_out = str(cmd_result.stdout, 'utf-8').splitlines()
            cmd_err = str(cmd_result.stderr, 'utf-8').splitlines()

            for line in cmd_out:
                self._log.info(line)

            if cmd_result.returncode == 0:
                for line in cmd_err:
                    self._log.info(line)

            else:
                for line in cmd_err:
                    self._log.error(line)

                error_msg = f"Git checkout failed for {model_def.repository} {model_def.version}"
                self._log.error(error_msg)
                raise _ex.EModelRepo(error_msg)

        self._log.info(f"Git checkout succeeded for {model_def.repository} {model_def.version}")

        return repo_dir.joinpath(model_def.path)


class RepositoryManager:

    __repo_types: tp.Dict[str, tp.Callable[[_cfg.RepositoryConfig], IModelRepository]] = {
        "integrated": IntegratedSource,
        "local": LocalRepository,
        "git": GitRepository
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
                    + " (this could indicate a missing model repository plugin)"

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
