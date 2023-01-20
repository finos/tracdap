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

import tracdap.rt.ext.plugins as plugins
import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex
import tracdap.rt._impl.util as util

# Import repo interfaces
from tracdap.rt.ext.repos import *


class RepositoryManager:

    def __init__(self, sys_config: cfg.RuntimeConfig):

        self._log = util.logger_for_object(self)
        self._repos: tp.Dict[str, IModelRepository] = dict()

        # Initialize all repos in the system config
        # Any errors for missing repo types (plugins) will be raised during startup

        for repo_name, repo_config in sys_config.repositories.items():

            try:

                self._repos[repo_name] = plugins.PluginManager.load_plugin(IModelRepository, repo_config)

            except ex.EPluginNotAvailable as e:

                msg = f"Model repository type [{repo_config.protocol}] is not recognised" \
                      + " (this could indicate a missing model repository plugin)"

                self._log.error(msg)
                raise ex.EStartup(msg) from e

    def get_repository(self, repo_name: str) -> IModelRepository:

        repo = self._repos.get(repo_name)

        if repo is None:

            msg = f"Model repository [{repo_name}] is unknown or not configured" \
                  + " (this could indicate a missing repository entry in the system config)"

            self._log.error(msg)
            raise ex.EModelRepoConfig(msg)

        return repo
