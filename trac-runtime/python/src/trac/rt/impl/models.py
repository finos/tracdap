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

import typing as _tp
import types as _tps
import pathlib
import tempfile

import trac.rt.api as _api
import trac.rt.metadata as _meta
import trac.rt.config as _cfg

import trac.rt.impl.repos as _repos
import trac.rt.impl.model_shim as _shim
import trac.rt.impl.util as _util


class _ModelLoaderState:

    def __init__(self, scratch_dir: _tp.Union[pathlib.Path, str]):
        self.scratch_dir = scratch_dir
        self.cache: _tp.Dict[str, _api.TracModel.__class__] = dict()


class ModelLoader:

    def __init__(self, sys_config: _cfg.RuntimeConfig):
        self.__repos = _repos.RepositoryManager(sys_config)
        self.__scopes: _tp.Dict[str, _ModelLoaderState] = dict()
        self.__log = _util.logger_for_object(self)

    def create_scope(self, scope: str, model_scratch_dir: _tp.Union[str, pathlib.Path, _tps.NoneType] = None):

        if model_scratch_dir is None:
            model_scratch_dir = tempfile.mkdtemp()

        self.__scopes[scope] = _ModelLoaderState(model_scratch_dir)

    def destroy_scope(self, scope: str):
        del self.__scopes[scope]

    def load_model_class(self, scope: str, model_def: _meta.ModelDefinition) -> _api.TracModel.__class__:

        state = self.__scopes[scope]

        model_key = f"{model_def.repository}#{model_def.path}#{model_def.version}#{model_def.entryPoint}"
        model_class = state.cache.get(model_key)

        if model_class is not None:
            return model_class

        self.__log.info(f"Loading model [{model_def.entryPoint}] (version=[{model_def.version}], scope=[{scope}])...")

        repo = self.__repos.get_repository(model_def.repository)
        checkout_dir = pathlib.Path(state.scratch_dir).joinpath(model_def.repository)
        checkout = repo.checkout_model(model_def, checkout_dir)

        with _shim.ModelShim.use_checkout(checkout):

            model_class = _shim.ModelShim.load_model_class(model_def.entryPoint)

            state.cache[model_key] = model_class
            return model_class

    def scan_model(self, model_class: _api.TracModel.__class__) -> _meta.ModelDefinition:

        model: _api.TracModel = object.__new__(model_class)
        model_class.__init__(model)

        try:
            params = model.define_parameters()
            inputs = model.define_inputs()
            outputs = model.define_outputs()

        # TODO: Error handling
        except Exception as e:
            print(e)

        return _meta.ModelDefinition()
