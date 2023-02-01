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
import re
import subprocess as sp
import typing as tp
import urllib.parse
import time

import dulwich.repo as git_repo
import dulwich.client as git_client
import dulwich.index as git_index

import tracdap.rt.metadata as meta
import tracdap.rt.exceptions as ex

# Import repo interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.repos import *

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class GitRepository(IModelRepository):

    REPO_URL_KEY = "repoUrl"
    NATIVE_GIT_KEY = "nativeGit"
    NATIVE_GIT_DEFAULT = True

    GIT_TIMEOUT_SECONDS = 30

    GIT_CONFIG_PATTERN = re.compile("^git\\.([^.]+)\\.(.+)")
    SHA1_PATTERN = re.compile("^[0-9a-f]{40}$")

    def __init__(self, properties: tp.Dict[str, str]):

        self._properties = properties
        self._log = _helpers.logger_for_object(self)

        repo_url_prop = _helpers.get_plugin_property(self._properties, self.REPO_URL_KEY)
        native_git_prop = _helpers.get_plugin_property(self._properties, self.NATIVE_GIT_KEY)

        if not repo_url_prop:
            raise ex.EConfigParse(f"Missing required property [{self.REPO_URL_KEY}] in Git repository config")

        repo_url = urllib.parse.urlparse(repo_url_prop)
        credentials = _helpers.get_http_credentials(repo_url, self._properties)

        self._repo_url = _helpers.apply_http_credentials(repo_url, credentials)

        if native_git_prop is not None:
            self._native_git = native_git_prop.strip().lower() == "true"
        else:
            self._native_git = self.NATIVE_GIT_DEFAULT

    def package_path(
            self, model_def: meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> pathlib.Path:

        return checkout_dir.joinpath(model_def.path)

    def do_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(
            f"Git checkout: repo = [{model_def.repository}], " +
            f"group = [{model_def.packageGroup}], package = [{model_def.package}], version = [{model_def.version}]")

        self._log.info(f"Checkout location: [{checkout_dir}]")

        if self._native_git:
            package_path = self._do_native_checkout(model_def, checkout_dir)
        else:
            package_path = self._do_python_checkout(model_def, checkout_dir)

        self._log.info(f"Git checkout succeeded for {model_def.package} {model_def.version}")

        return package_path

    def _do_native_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(f"Checkout mechanism: [native]")

        git_cli = ["git", "-C", str(checkout_dir)]

        git_cmds = [
            ["init"],
            ["remote", "add", "origin", self._repo_url.geturl()],
            ["fetch", "--depth=1", "origin", model_def.version],
            ["reset", "--hard", "FETCH_HEAD"]]

        # Flag to track when git config is written to the repo
        config_written = False

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
            self._log.info(f"=> git {' '.join(safe_cmd)}")

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

            # After the init command, use dulwich to write config into the repo folder
            # This is a regular .gitconfig file, so it will be understood by the native commands
            if not config_written:
                repo = git_repo.Repo(str(checkout_dir))
                self._apply_config_from_properties(repo)
                config_written = True

        return self.package_path(model_def, checkout_dir)

    def _do_python_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(f"Checkout mechanism: [python]")

        # Create a new repo

        self._log.info("=> git init")

        repo = git_repo.Repo.init(str(checkout_dir))
        self._apply_config_from_properties(repo)

        # Set up origin

        self._log.info(f"=> git remote add origin {_helpers.log_safe(self._repo_url)}")

        self._add_remote(repo, "origin", self._repo_url.geturl())

        # Set up the ref keys to look for in the fetch response
        commit_hash = self._ref_key(model_def.version) if self.SHA1_PATTERN.match(model_def.version) else None
        tag_key = self._ref_key(f"refs/tags/{model_def.version}")
        branch_key = self._ref_key(f"refs/heads/{model_def.version}")

        # This is how dulwich filters what comes back from a fetch
        # Commit hash is first preference, then tag, then branch
        # We are only ever going to request a single commit
        def select_commit_hash(refs, depth):  # noqa
            if commit_hash is not None:
                return [commit_hash]
            if tag_key in refs:
                return [refs[tag_key]]
            if branch_key in refs:
                return [refs[branch_key]]
            raise ex.EModelRepo(f"Model version not found: [{model_def.version}]")

        # Run the Git fetch command

        self._log.info(f"=> git fetch --depth=1 origin {model_def.version}")

        credentials = _helpers.get_http_credentials(self._repo_url, self._properties)
        username, password = _helpers.split_http_credentials(credentials)

        client = git_client.HttpGitClient(
            self._repo_url.geturl(),
            config=repo.get_config(),
            username=username,
            password=password)

        fetch = client.fetch(self._repo_url.path, repo, select_commit_hash,  depth=1)

        # Look for the ref that came back from the fetch command and set it as HEAD

        self._log.info("=> git reset --hard FETCH_HEAD")

        if commit_hash is not None:
            repo[b"HEAD"] = commit_hash
        elif tag_key in fetch.refs:
            repo[b"HEAD"] = fetch.refs[tag_key]
        elif branch_key in fetch.refs:
            repo[b"HEAD"] = fetch.refs[branch_key]
        else:
            raise ex.EModelRepo(f"Model version not found: [{model_def.version}]")

        # This checks out HEAD into the repo folder
        index_file = repo.index_path()
        tree = repo[b"HEAD"].tree
        git_index.build_index_from_tree(repo.path, index_file, repo.object_store, tree)

        return self.package_path(model_def, checkout_dir)

    def _apply_config_from_properties(self, repo: git_repo.Repo):

        config = repo.get_config()

        for key, value in self._properties.items():

            match = self.GIT_CONFIG_PATTERN.match(key)

            if match:
                section = match.group(1)
                name = match.group(2)
                config.set(section, name, value)

        config.write_to_path()

    @staticmethod
    def _add_remote(repo: git_repo.Repo, remote_name: str, remote_location):

        config = repo.get_config()

        config.set(f"remote \"{remote_name}\"", "url", remote_location)

        config.write_to_path()

    @staticmethod
    def _ref_key(key):
        return bytes(key, "ascii")


# Register plugin
plugins.PluginManager.register_plugin(IModelRepository, GitRepository, ["git"])
