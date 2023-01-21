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

import typing as tp
import pathlib
import copy

import tracdap.rt.api as _api
import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex

import tracdap.rt._impl.type_system as _types
import tracdap.rt._impl.repos as _repos
import tracdap.rt._impl.shim as _shim
import tracdap.rt._impl.util as _util
import tracdap.rt._impl.validation as _val


class ModelLoader:

    class _ScopeState:
        def __init__(self, scratch_dir: pathlib.Path):
            self.scratch_dir = scratch_dir
            self.model_cache: tp.Dict[str, _api.TracModel.__class__] = dict()
            self.code_cache: tp.Dict[str, pathlib.Path] = dict()
            self.shims: tp.Dict[pathlib.Path, str] = dict()

    def __init__(self, sys_config: _cfg.RuntimeConfig, scratch_dir: pathlib.Path):

        self.__log = _util.logger_for_object(self)

        self.__scratch_dir = scratch_dir.joinpath("models")
        self.__scratch_dir.mkdir(exist_ok=True, parents=False, mode=0o750)

        self.__repos = _repos.RepositoryManager(sys_config)
        self.__scopes: tp.Dict[str, ModelLoader._ScopeState] = dict()

    def create_scope(self, scope: str):

        try:

            self.__log.info(f"Creating model scope [{scope}]")

            scope_scratch_dir = self.__scratch_dir.joinpath(scope)
            scope_scratch_dir.mkdir(exist_ok=False, parents=False, mode=0o750)

            scope_state = ModelLoader._ScopeState(scope_scratch_dir)
            self.__scopes[scope] = scope_state

        except FileExistsError as e:

            msg = f"Model scope [{scope}] already exists"
            self.__log.error(msg)
            self.__log.exception(e)
            raise _ex.EStartup(msg) from e

    def destroy_scope(self, scope: str):

        self.__log.info(f"Destroying model scope [{scope}]")

        del self.__scopes[scope]

        # Do not delete scope scratch dir here
        # The top level scratch dir is cleaned up on exit, depending on the --scratch-dir-persist flag
        # If the flag is set, all scratch content should be left behind
        # This can be for debugging, or because the scratch data is written to an ephemeral volume

    @staticmethod
    def model_checkout_key(model_def: _meta.ModelDefinition):

        # The "packageGroup" field is optional and may not be present for some repo types
        # The "package" field remains optional until the 0.6 metadata upgrade

        group = model_def.packageGroup or "-"
        package = model_def.package or "-"
        version = model_def.version

        return f"{group}/{package}/{version}"

    def load_model_class(self, scope: str, model_def: _meta.ModelDefinition) -> _api.TracModel.__class__:

        scope_state = self.__scopes[scope]

        model_key = f"{model_def.repository}#{model_def.path}#{model_def.version}#{model_def.entryPoint}"
        model_class = scope_state.model_cache.get(model_key)

        if model_class is not None:
            return model_class

        self.__log.info(f"Loading model [{model_def.entryPoint}] (version=[{model_def.version}], scope=[{scope}])...")

        repo = self.__repos.get_repository(model_def.repository)
        checkout_key = self.model_checkout_key(model_def)
        checkout_subdir = pathlib.Path(checkout_key)

        # Make sure the checkout key is safe to use as a directory name

        if checkout_subdir.is_absolute() or checkout_subdir.is_reserved():

            msg = f"Checkout failed: Invalid checkout key [{checkout_key}] in repo [{model_def.repository}]" + \
                  f" (type: {type(repo).__name__})"

            self.__log.error(msg)
            raise _ex.EUnexpected(msg)

        # If the repo/checkout already exists in the code cache, we can use the existing checkout and package dir

        code_cache_key = f"{model_def.repository}#{checkout_key}"

        if code_cache_key in scope_state.code_cache:
            checkout_dir = scope_state.code_cache[code_cache_key]
            package_dir = repo.package_path(model_def, checkout_dir)

        # Otherwise, we need to run the checkout, and store the checkout dir into the code cache
        # What gets cached is the checkout, which may contain multiple packages depending on the repo type

        else:
            scope_dir = scope_state.scratch_dir
            checkout_dir = scope_dir.joinpath(model_def.repository, checkout_subdir)
            checkout_dir.mkdir(mode=0o750, parents=True, exist_ok=False)
            package_dir = repo.do_checkout(model_def, checkout_dir)

            scope_state.code_cache[code_cache_key] = checkout_dir

        # For the integrated repo (i.e. model code in PYTHONPATH), do not use a shim
        if package_dir is None:
            shim = None

        # For all other repo types, use a shim to load external model code
        # Only create one shim per package root in each model scope
        # This allows models from the same repo / version to be loaded in the same namespace
        else:
            package_dir = package_dir.absolute().resolve()
            if package_dir in scope_state.shims:
                shim = scope_state.shims[package_dir]
            else:
                shim = _shim.ShimLoader.create_shim(package_dir)
                scope_state.shims[package_dir] = shim

        # Now we have the required shim, we can use it with the shim loader to load a model

        with _shim.ShimLoader.use_shim(shim):

            module_name = model_def.entryPoint.rsplit(".", maxsplit=1)[0]
            class_name = model_def.entryPoint.rsplit(".", maxsplit=1)[1]

            model_class = _shim.ShimLoader.load_class(module_name, class_name, _api.TracModel)

            scope_state.model_cache[model_key] = model_class
            return model_class

    def scan_model(self, model_stub: _meta.ModelDefinition, model_class: _api.TracModel.__class__) \
            -> _meta.ModelDefinition:

        try:

            model: _api.TracModel = object.__new__(model_class)
            model_class.__init__(model)

            attributes = model.define_attributes()
            parameters = model.define_parameters()
            inputs = model.define_inputs()
            outputs = model.define_outputs()

            model_def = copy.copy(model_stub)
            model_def.staticAttributes = attributes
            model_def.parameters = parameters
            model_def.inputs = inputs
            model_def.outputs = outputs

            _val.quick_validate_model_def(model_def)

            for attr_name, attr_value in attributes.items():
                self.__log.info(f"Attribute [{attr_name}] - {_types.MetadataCodec.decode_value(attr_value)}")

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
