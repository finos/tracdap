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

import types
import typing as tp
import tempfile
import pathlib

import tracdap.rt.api as _api
import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex

import tracdap.rt.impl.util as _util
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.repos as _repos
import tracdap.rt.impl.shim as _shim


class ModelLoader:

    class _ScopeState:
        def __init__(self, scratch_dir: tp.Union[pathlib.Path, str]):
            self.scratch_dir = scratch_dir
            self.cache: tp.Dict[str, _api.TracModel.__class__] = dict()

    def __init__(self, sys_config: _cfg.RuntimeConfig):
        self.__repos = _repos.RepositoryManager(sys_config)
        self.__scopes: tp.Dict[str, ModelLoader._ScopeState] = dict()
        self.__log = _util.logger_for_object(self)

    def create_scope(self, scope: str, model_scratch_dir: tp.Union[str, pathlib.Path, types.NoneType] = None):

        # TODO: Use a per-job location for model checkouts, that can be cleaned up?

        if model_scratch_dir is None:
            model_scratch_dir = tempfile.mkdtemp()

        self.__scopes[scope] = ModelLoader._ScopeState(model_scratch_dir)

    def destroy_scope(self, scope: str):

        # TODO: Delete model checkout location

        del self.__scopes[scope]

    def load_model_class(self, scope: str, model_def: _meta.ModelDefinition) -> _api.TracModel.__class__:

        state = self.__scopes[scope]

        model_key = f"{model_def.repository}#{model_def.path}#{model_def.version}#{model_def.entryPoint}"
        model_class = state.cache.get(model_key)

        if model_class is not None:
            return model_class

        self.__log.info(f"Loading model [{model_def.entryPoint}] (version=[{model_def.version}], scope=[{scope}])...")

        # TODO: Prevent duplicate checkout per scope

        repo = self.__repos.get_repository(model_def.repository)
        checkout_dir = pathlib.Path(state.scratch_dir)
        checkout = repo.checkout_model(model_def, checkout_dir)

        with _shim.ShimLoader.use_checkout(checkout):

            module_name = model_def.entryPoint.rsplit(".", maxsplit=1)[0]
            class_name = model_def.entryPoint.rsplit(".", maxsplit=1)[1]

            model_class = _shim.ShimLoader.load_class(module_name, class_name, _api.TracModel)

            state.cache[model_key] = model_class
            return model_class

    def scan_model(self, model_class: _api.TracModel.__class__) -> _meta.ModelDefinition:

        model: _api.TracModel = object.__new__(model_class)
        model_class.__init__(model)

        try:

            parameters = model.define_parameters()
            inputs = model.define_inputs()
            outputs = model.define_outputs()

            for parameter in parameters.values():
                if parameter.defaultValue is not None:
                    parameter.defaultValue = _types.MetadataCodec.encode_value(parameter.defaultValue)

            # TODO: Model validation

            model_def = _meta.ModelDefinition()
            model_def.parameters.update(parameters)
            model_def.inputs.update(inputs)
            model_def.outputs.update(outputs)

            for name, param in model_def.parameters.items():
                self.__log.info(f"Parameter [{name}] - {param.paramType.basicType.name}")

            for name, schema in model_def.inputs.items():
                self.__log.info(f"Input [{name}] - {schema.schema.schemaType.name}")

            for name, schema in model_def.outputs.items():
                self.__log.info(f"Output [{name}] - {schema.schema.schemaType.name}")

            return model_def

        except Exception as e:

            model_class_name = f"{model_class.__module__}.{model_class.__name__}"
            msg = f"An error occurred while scanning model class [{model_class_name}]: {str(e)}"

            self.__log.error(msg, exc_info=True)
            raise _ex.EModelValidation(msg) from e
