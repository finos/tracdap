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

from __future__ import annotations

import subprocess
import subprocess as sp
import time

import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.util as _util

# Import repo interfaces
from tracdap.rt.ext.repos import *


class IntegratedSource(IModelRepository):

    def __init__(self, repo_config: _cfg.PluginConfig):
        self._repo_config = repo_config

    def checkout_key(self, model_def: _meta.ModelDefinition):
        return "trac_integrated"

    def package_path(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> tp.Optional[pathlib.Path]:

        return None

    def do_checkout(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: tp.Union[str, pathlib.Path]) \
            -> None:

        # For the integrated repo there is nothing to check out

        return self.package_path(model_def, checkout_dir)


class LocalRepository(IModelRepository):

    REPO_URL_KEY = "repoUrl"

    def __init__(self, repo_config: _cfg.PluginConfig):
        self._repo_config = repo_config
        self._repo_url = self._repo_config.properties[self.REPO_URL_KEY]

        if not self._repo_url:
            raise _ex.EConfigParse(f"Missing required property [{self.REPO_URL_KEY}] in local repository config")

    def checkout_key(self, model_def: _meta.ModelDefinition):
        return "trac_local"

    def package_path(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> tp.Optional[pathlib.Path]:

        checkout_path = pathlib.Path(self._repo_url).joinpath(model_def.path)

        return checkout_path

    def do_checkout(self, model_def: _meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        # For local repos, checkout is a no-op since the model is already local
        # Just return the existing package path

        return self.package_path(model_def, checkout_dir)


class GitRepository(IModelRepository):

    REPO_URL_KEY = "repoUrl"
    GIT_TIMEOUT_SECONDS = 30

    def __init__(self, repo_config: _cfg.PluginConfig):
        self._repo_config = repo_config
        self._log = _util.logger_for_object(self)
        self._repo_url = self._repo_config.properties.get(self.REPO_URL_KEY)

        if not self._repo_url:
            raise _ex.EConfigParse(f"Missing required property [{self.REPO_URL_KEY}] in Git repository config")

    def checkout_key(self, model_def: _meta.ModelDefinition):
        return model_def.version

    def package_path(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> tp.Optional[pathlib.Path]:

        return checkout_dir.joinpath(model_def.path)

    def do_checkout(self, model_def: _meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(f"Git checkout {model_def.repository} {model_def.version} -> {checkout_dir}")

        git_cli = ["git", "-C", str(checkout_dir)]

        git_cmds = [
            ["init"],
            ["remote", "add", "origin", self._repo_url],
            ["fetch", "--depth=1", "origin", model_def.version],
            ["reset", "--hard", "FETCH_HEAD"]]

        # Work around Windows issues
        if _util.is_windows():

            # Some machines may still be setup without long path support in Windows and/or the Git client
            # Workaround: Enable the core.longpaths flag for each individual Git command (do not rely on system config)
            git_cli += ["-c", "core.longpaths=true"]

            # On some systems, directories created by the TRAC runtime process may not be owned by the process owner
            # This will cause Git to report an unsafe repo directory
            # Finding the current owner requires either batch scripting or using the win32_api package
            # Workaround: Explicitly take ownership of the repo directory, always, before starting the checkout
            try:
                self._log.info(f"Fixing filesystem permissions for [{checkout_dir}]")
                subprocess.run(f"takeown /f \"{checkout_dir}\"")
            except Exception:  # noqa
                self._log.info(f"Failed to fix filesystem permissions, this might prevent checkout from succeeding")

        for git_cmd in git_cmds:

            self._log.info(f"git {' '.join(git_cmd)}")

            cmd = [*git_cli, *git_cmd]
            cmd_result = sp.run(cmd, cwd=checkout_dir, stdout=sp.PIPE, stderr=sp.PIPE, timeout=self.GIT_TIMEOUT_SECONDS)

            if cmd_result.returncode != 0:
                time.sleep(1)
                self._log.warning(f"git {' '.join(git_cmd)} (retrying)")
                cmd_result = sp.run(cmd, cwd=checkout_dir, stdout=sp.PIPE, stderr=sp.PIPE, timeout=self.GIT_TIMEOUT_SECONDS)  # noqa

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

        return self.package_path(model_def, checkout_dir)


class RepositoryManager:

    __repo_types: tp.Dict[str, tp.Callable[[_cfg.PluginConfig], IModelRepository]] = {
        "integrated": IntegratedSource,
        "local": LocalRepository,
        "git": GitRepository
    }

    @classmethod
    def register_repo_type(cls, repo_type: str, loader_class: tp.Callable[[_cfg.PluginConfig], IModelRepository]):
        cls.__repo_types[repo_type] = loader_class

    def __init__(self, sys_config: _cfg.RuntimeConfig):

        self._log = _util.logger_for_object(self)
        self._loaders: tp.Dict[str, IModelRepository] = dict()

        for repo_name, repo_config in sys_config.repositories.items():

            if repo_config.protocol not in self.__repo_types:

                msg = f"Model repository type [{repo_config.protocol}] is not recognised" \
                    + " (this could indicate a missing model repository plugin)"

                self._log.error(msg)
                raise _ex.EModelRepoConfig(msg)

            loader_class = self.__repo_types[repo_config.protocol]
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
