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
import typing as tp

import tracdap.rt.metadata as meta
import tracdap.rt.exceptions as ex

# Import repo interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.repos import *

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


class IntegratedSource(IModelRepository):

    def __init__(self, properties: tp.Dict[str, str]):
        self._properties = properties

    def package_path(
            self, model_def: meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> tp.Optional[pathlib.Path]:

        # Integrated repo does not have a package path, since it is loading from the Python path

        return None

    def do_checkout(
            self, model_def: meta.ModelDefinition,
            checkout_dir: tp.Union[str, pathlib.Path]) \
            -> tp.Optional[pathlib.Path]:

        # For the integrated repo there is nothing to check out

        return self.package_path(model_def, checkout_dir)


class LocalRepository(IModelRepository):

    REPO_URL_KEY = "repoUrl"

    def __init__(self, properties: tp.Dict[str, str]):
        self._properties = properties
        self._repo_url = _helpers.get_plugin_property(self._properties, self.REPO_URL_KEY)

        if not self._repo_url:
            raise ex.EConfigParse(f"Missing required property [{self.REPO_URL_KEY}] in local repository config")

    def package_path(
            self, model_def: meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> pathlib.Path:

        checkout_path = pathlib.Path(self._repo_url).joinpath(model_def.path)

        return checkout_path

    def do_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        # For local repos, checkout is a no-op since the model is already local
        # Just return the existing package path

        return self.package_path(model_def, checkout_dir)


plugins.PluginManager.register_plugin(IModelRepository, IntegratedSource, ["integrated"])
plugins.PluginManager.register_plugin(IModelRepository, LocalRepository, ["local"])
