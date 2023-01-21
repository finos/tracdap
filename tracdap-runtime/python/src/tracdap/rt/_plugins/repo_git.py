#  Copyright 2023 Accenture Global Solutions Limited
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

import pathlib
import subprocess as sp
import typing as tp
import urllib.parse
import time

import tracdap.rt.metadata as meta
import tracdap.rt.exceptions as ex

# Import repo interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.repos import *

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class GitRepository(IModelRepository):

    REPO_URL_KEY = "repoUrl"
    GIT_TIMEOUT_SECONDS = 30

    def __init__(self, properties: tp.Dict[str, str]):

        self._properties = properties
        self._log = _helpers.logger_for_object(self)

        repo_url_prop = _helpers.get_plugin_property(self._properties, self.REPO_URL_KEY)

        if not repo_url_prop:
            raise ex.EConfigParse(f"Missing required property [{self.REPO_URL_KEY}] in Git repository config")

        repo_url = urllib.parse.urlparse(repo_url_prop)
        credentials = _helpers.get_http_credentials(repo_url, self._properties)

        self._repo_url = _helpers.apply_http_credentials(repo_url, credentials)

    def package_path(
            self, model_def: meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> pathlib.Path:

        return checkout_dir.joinpath(model_def.path)

    def do_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(
            f"Git checkout: repo = [{model_def.repository}], " +
            f"group = [{model_def.packageGroup}], package = [{model_def.package}], version = [{model_def.version}]")

        self._log.info(f"Checkout location: [{checkout_dir}]")

        git_cli = ["git", "-C", str(checkout_dir)]

        git_cmds = [
            ["init"],
            ["remote", "add", "origin", self._repo_url.geturl()],
            ["fetch", "--depth=1", "origin", model_def.version],
            ["reset", "--hard", "FETCH_HEAD"]]

        # Work around Windows issues
        if _helpers.is_windows():

            # Some machines may still be setup without long path support in Windows and/or the Git client
            # Workaround: Enable the core.longpaths flag for each individual Git command (do not rely on system config)
            git_cli += ["-c", "core.longpaths=true"]

            # On some systems, directories created by the TRAC runtime process may not be owned by the process owner
            # This will cause Git to report an unsafe repo directory
            # Finding the current owner requires either batch scripting or using the win32_api package
            # Workaround: Explicitly take ownership of the repo directory, always, before starting the checkout
            try:
                self._log.info(f"Fixing filesystem permissions for [{checkout_dir}]")
                sp.run(f"takeown /f \"{checkout_dir}\"")
            except Exception:  # noqa
                self._log.info(f"Failed to fix filesystem permissions, this might prevent checkout from succeeding")

        for git_cmd in git_cmds:

            safe_cmd = map(_helpers.log_safe, git_cmd)
            self._log.info(f"git {' '.join(safe_cmd)}")

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

                error_msg = f"Git checkout failed for {model_def.package} {model_def.version}"
                self._log.error(error_msg)
                raise ex.EModelRepo(error_msg)

        self._log.info(f"Git checkout succeeded for {model_def.package} {model_def.version}")

        return self.package_path(model_def, checkout_dir)


# Register plugin
plugins.PluginManager.register_plugin(IModelRepository, GitRepository, ["git"])
