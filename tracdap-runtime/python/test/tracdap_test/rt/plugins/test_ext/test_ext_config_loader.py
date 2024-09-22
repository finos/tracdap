#  Copyright 2024 Accenture Global Solutions Limited
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

import typing as _tp

import tracdap.rt.api as trac
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.api import TracContext

from tracdap.rt.ext.config import *
from tracdap.rt_gen.domain.tracdap.metadata import ModelOutputSchema, ModelInputSchema, ModelParameter

_TEST_EXT_PROTOCOL = "test-ext"


class TestExtModel(trac.TracModel):

    def define_parameters(self) -> _tp.Dict[str, ModelParameter]:

        return trac.define_parameters(
            trac.P("param_1", trac.INTEGER, label="Parameter 1"))

    def define_inputs(self) -> _tp.Dict[str, ModelInputSchema]:
        return dict()

    def define_outputs(self) -> _tp.Dict[str, ModelOutputSchema]:
        return dict()

    def run_model(self, ctx: TracContext):
        pass


class TestExtConfigLoader(IConfigLoader):

    STATIC_SYS_KEY = "sys_config_HuX-7"
    STATIC_SYS_OBJ = {
        "repositories": {
            "TEST_EXT_REPO": {
                "protocol": "local",
                "properties": {"repoUrl": "/tmp/wherever"}
            }
        }
    }

    STATIC_JOB_KEY = "job_config_A1-6"
    STATIC_JOB_OBJ = {
        "job": {
            "runModel": {
                "model": f'{__name__}.TestExtModel',
                "parameters": { "param_1": 42 }
            }
        }
    }

    # Properties dict will be empty for config plugins
    def __init__(self, _: _tp.Dict[str, str]):
        pass

    def has_config_file(self, config_url: str) -> bool:
        return False

    def load_config_file(self, config_url: str) -> bytes:
        raise RuntimeError("The TEST EXT config loader only supports loading objects")

    def has_config_dict(self, config_url: str) -> bool:

        if not config_url.startswith(_TEST_EXT_PROTOCOL + ":"):
            return False

        config_key = config_url[len(_TEST_EXT_PROTOCOL) + 1:]

        return config_key in [self.STATIC_SYS_KEY, self.STATIC_JOB_KEY]

    def load_config_dict(self, config_url: str) -> dict:

        if not config_url.startswith(_TEST_EXT_PROTOCOL + ":"):
            raise RuntimeError("Invalid protocol for loading from the TEST EXT config loader")

        config_key = config_url[len(_TEST_EXT_PROTOCOL) + 1:]

        if config_key == self.STATIC_JOB_KEY:
            return self.STATIC_JOB_OBJ

        if config_key == self.STATIC_SYS_KEY:
            return self.STATIC_SYS_OBJ

        raise RuntimeError(f"Config key not found: [{config_key}")


plugins.PluginManager.register_plugin(IConfigLoader, TestExtConfigLoader, ["test-ext"])
