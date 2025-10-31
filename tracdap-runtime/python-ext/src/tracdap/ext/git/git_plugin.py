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

import pathlib
import re
import subprocess as sp
import urllib.parse
import time

import dulwich.repo as git_repo
import dulwich.client as git_client
import dulwich.index as git_index
import urllib3.exceptions  # noqa

import tracdap.rt.config as cfg
import tracdap.rt.metadata as meta
import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins
import tracdap.rt.ext.util as util

# Import repo interfaces
from tracdap.rt.ext.repos import *
from tracdap.rt.ext.util import hide_http_credentials


class GitRepository(IModelRepository):

    REPO_URL_KEY = "repoUrl"
    NATIVE_GIT_KEY = "nativeGit"
    USERNAME_KEY = "username"
    PASSWORD_KEY = "password"
    TOKEN_KEY = "token"

    NATIVE_GIT_DEFAULT = False

    GIT_TIMEOUT_SECONDS = 30

    GIT_CONFIG_PATTERN = re.compile("^git\\.([^.]+)\\.(.+)")
    SHA1_PATTERN = re.compile("^[0-9a-f]{40}$")

    def __init__(
            self, config: cfg.PluginConfig,
            network_manager: plugins.INetworkManager,
            log_provider: plugins.ILogProvider):

        self._config = config
        self._pool_manager = network_manager.create_urllib3_pool_manager(config)
        self._log = log_provider.logger_for_object(self)

        repo_url_str = util.read_plugin_config(self._config, self.REPO_URL_KEY)
        repo_url = urllib.parse.urlparse(repo_url_str)
        credentials = self._get_http_credentials(repo_url)

        self._repo_url = util.apply_http_credentials(repo_url, credentials)

        # Whether to use the system native Git, instead of Python Dulwich
        self._native_git = util.read_plugin_config(
            self._config, self.NATIVE_GIT_KEY,
            convert=bool, default=self.NATIVE_GIT_DEFAULT)

    def package_path(
            self, model_def: meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> pathlib.Path:

        return checkout_dir.joinpath(model_def.path)

    def do_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        try:

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

        except Exception as e:

            error = e

            # For retry failures, try to find the original cause
            while e.__cause__ is not None:
                if isinstance(e, urllib3.exceptions.MaxRetryError):
                    error = e.__cause__
                    break
                else:
                    e = e.__cause__

            # Try to sanitize error messages from urllib3
            if isinstance(error, urllib3.exceptions.HTTPError):
                detail = self._clean_urllib3_error(error)
            else:
                detail = str(error)

            message = f"Failed to check out [{model_def.repository}]: {detail}"

            self._log.error(message)
            raise ex.EModelRepo(message) from error

    def _do_native_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(f"Checkout mechanism: [native]")

        # Using windows_safe_path() to create UNC paths does not always work with Windows native Git
        # So, use the regular checkout_dir, and set core.longpaths = true once the repo is created
        # This will fail if the path for the repo config file exceeds the Windows MAX_PATH length
        # I.e. checkout_dir/.git/config

        git_cli = ["git", "-C", str(checkout_dir)]

        git_cmds = [
            ["init"],
            ["remote", "add", "origin", self._repo_url.geturl()],
            ["fetch", "--depth=1", "origin", model_def.version],
            ["reset", "--hard", "FETCH_HEAD"]]

        # Flag to track when git config is written to the repo
        config_written = False

        # Work around Windows issues
        if util.is_windows():

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

            safe_cmd = map(self._log_safe, git_cmd)
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

                # After the init command, use dulwich to write config into the repo folder
                # This is a regular .gitconfig file, so it will be understood by the native commands
                if not config_written:
                    repo = git_repo.Repo(str(checkout_dir))
                    self._apply_config_from_properties(repo)
                    config_written = True

            elif cmd_err:

                for line in cmd_err:
                    self._log.error(line)

                raise ex.EModelRepo(cmd_err[-1])

            else:

                error_msg = f"Git checkout failed for {model_def.package} {model_def.version}"
                self._log.error(error_msg)
                raise ex.EModelRepo(error_msg)

        return self.package_path(model_def, checkout_dir)

    def _do_python_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(f"Checkout mechanism: [python]")

        # Create a new repo

        self._log.info("=> git init")

        repo = git_repo.Repo.init(str(checkout_dir))
        self._apply_config_from_properties(repo)

        # Set up origin

        self._log.info(f"=> git remote add origin {util.hide_http_credentials(self._repo_url)}")

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

        credentials = self._get_http_credentials(self._repo_url)
        username, password = self._split_http_credentials(credentials)

        client = git_client.HttpGitClient(
            self._repo_url.geturl(),
            config=repo.get_config(),
            pool_manager=self._pool_manager,
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

    def _get_http_credentials(self, url: urllib.parse.ParseResult) -> str | None:

        token = util.read_plugin_config(self._config, self.TOKEN_KEY, optional=True)
        username = util.read_plugin_config(self._config, self.USERNAME_KEY, optional=True)
        password = util.read_plugin_config(self._config, self.PASSWORD_KEY, optional=True)

        if token is not None:
            return token

        if username is not None and password is not None:
            return f"{username}:{password}"

        # If credentials are not explicit in the config, try looking in the URL
        return util.extract_http_credentials(url)

    @classmethod
    def _split_http_credentials(cls, credentials: str) -> (str | None, str | None):

        if credentials is None:
            return None, None

        elif ":" in credentials:
            sep = credentials.index(":")
            username = credentials[:sep]
            password = credentials[sep + 1:]
            return username, password

        else:
            return credentials, None

    def _apply_config_from_properties(self, repo: git_repo.Repo):

        config = repo.get_config()

        for key, value in self._config.properties.items():

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

    @classmethod
    def _clean_urllib3_error(cls, error: urllib3.exceptions.HTTPError):

        match = cls._URLLIB3_ERROR_PATTERN.match(str(error))

        # Best efforts to clean up the message, fall back on str(error)
        if match:
            return match.group(1)
        else:
            return str(error)

    # Error message format is like this:
    # <pkg.ClassName object at 0xXXXXXXX>: Message
    _URLLIB3_ERROR_PATTERN = re.compile(r"<[^>]*>: (.*)")

    @classmethod
    def _log_safe(cls, param):

        if isinstance(param, urllib.parse.ParseResult) or isinstance(param, urllib.parse.ParseResultBytes):
            return util.hide_http_credentials(param)

        if isinstance(param, str):
            try:
                url = urllib.parse.urlparse(param)
                return hide_http_credentials(url)
            except ValueError:
                return param

        if isinstance(param, list):
            return list(map(cls._log_safe, param))

        return param


# Register plugin
plugins.PluginManager.register_plugin(IModelRepository, GitRepository, ["git"])
