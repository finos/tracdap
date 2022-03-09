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
import itertools
import tempfile
import logging
import pathlib
import sys
import contextlib

import importlib as _il
import importlib.util as _ilu
import importlib.machinery as _ilm

import tracdap.rt.api as _api
import tracdap.rt.metadata as _meta
import tracdap.rt.config as _cfg
import tracdap.rt.impl.repos as _repos
import tracdap.rt.impl.type_system as _types
import tracdap.rt.impl.util as _util
import tracdap.rt.exceptions as _ex


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

        with ModelShim.use_checkout(checkout):

            model_class = ModelShim.load_model_class(model_def.entryPoint)

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
                    parameter.defaultValue = _types.encode_value(parameter.defaultValue)

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


class ModelShim:

    SHIM_NAMESPACE = "tracdap.shim"

    _log: tp.Optional[logging.Logger] = None

    __shim_id_seq = itertools.count()
    __shim_map: tp.Dict[str, pathlib.Path] = dict()
    __shim: tp.Optional[str] = None

    @classmethod
    def _init(cls):
        sys.meta_path.append(ModelShim.ModelShimFinder(cls.__shim_map))

    @classmethod
    def create_shim(cls, model_import_root: tp.Union[str, pathlib.Path]) -> str:

        shim_id = next(cls.__shim_id_seq)
        shim_namespace = f"{cls.SHIM_NAMESPACE}._{shim_id}"

        model_import_root = pathlib.Path(model_import_root).resolve()

        cls._log.info(f"Creating model shim [{shim_id}] for path [{model_import_root}]")

        cls.__shim_map[shim_namespace] = model_import_root

        return shim_namespace

    @classmethod
    def activate_shim(cls, shim: str):
        cls.__shim = shim
        sys.meta_path.append(ModelShim.ActiveShimFinder(shim))

    @classmethod
    def deactivate_shim(cls):
        cls.__shim = None
        sys.meta_path.remove(sys.meta_path[-1])

    @classmethod
    @contextlib.contextmanager
    def use_checkout(cls, model_checkout: tp.Union[str, pathlib.Path]):

        if model_checkout:
            shim = cls.create_shim(model_checkout)
            cls.activate_shim(shim)

        yield

        if model_checkout:
            cls.deactivate_shim()

    @classmethod
    def load_model_class(cls, entry_point: str) -> _api.TracModel.__class__:

        entry_point_parts = entry_point.rsplit(".", 1)
        module_name = entry_point_parts[0]
        class_name = entry_point_parts[1]

        model_module = _il.import_module(module_name)
        model_class = model_module.__dict__.get(class_name)

        if model_class is None:
            error_msg = f"Model class [{class_name}] was not found in module [{module_name}]"
            cls._log.error(error_msg)
            raise _ex.EModelRepoRequest(error_msg)

        if not isinstance(model_class, _api.TracModel.__class__):
            error_msg = f"Model class [{class_name}] is not a TRAC model"
            cls._log.error(error_msg)
            raise _ex.EModelRepoRequest(error_msg)

        return model_class

    class ActiveShimFinder(_ilm.PathFinder):

        def __init__(self, shim_namespace: str):
            self._shin_namespace = shim_namespace

        def find_spec(
                self,
                fullname: str,
                path: tp.Optional[tp.Sequence[tp.Union[bytes, str]]] = None,
                target: tp.Optional[types.ModuleType] = None) \
                -> tp.Optional[_ilm.ModuleSpec]:

            if fullname.startswith(ModelShim.SHIM_NAMESPACE):
                return None

            shim_module = f"{self._shin_namespace}.{fullname}"

            ModelShim._log.info(f"Shim module map [{fullname}] -> [{shim_module}]")

            return _ilu.find_spec(shim_module)

            # return ModelShim.ModelShimFinder.find_spec(shim_module, path, target)

    class ModelShimFinder(_ilm.PathFinder):

        def __init__(self, shim_map: tp.Dict[str, pathlib.Path]):
            self._shim_map = shim_map

        def find_spec(
                self,
                fullname: str,
                path: tp.Optional[tp.Sequence[tp.Union[bytes, str]]] = None,
                target: tp.Optional[types.ModuleType] = None) \
                -> tp.Optional[_ilm.ModuleSpec]:

            module_parts = fullname.split(".")

            if ".".join(module_parts[:2]) != ModelShim.SHIM_NAMESPACE:
                return None

            if fullname in sys.modules:
                return sys.modules[fullname].__spec__

            ModelShim._log.info(f"Shim module load [{fullname}]")

            # Create namespace pkgs for trac.shim and trac.shim._x
            if len(module_parts) == 2 or len(module_parts) == 3:
                spec = _ilm.ModuleSpec(fullname, origin=None, is_package=True, loader=None)
                return spec

            shim_name = ".".join(module_parts[:3])
            shim_path = pathlib.Path(self._shim_map[shim_name])

            shim_module_path = shim_path.joinpath(module_parts[3] + ".py")

            if shim_module_path.exists() and shim_module_path.is_file():

                shim_module = ".".join(module_parts[:4])
                shim_module_loader = ModelShim.ModelSourceLoader(shim_module, str(shim_module_path))
                spec = _ilm.ModuleSpec(shim_module, origin=str(shim_module_path), loader=shim_module_loader)
                return spec

            return None

    class ModelSourceLoader(_ilm.SourceFileLoader):

        def __init__(self, fullname, path):
            super(ModelShim.ModelSourceLoader, self).__init__(fullname, path)
            self._exec_done = False

        def create_module(self, spec: _ilm.ModuleSpec) -> tp.Optional[types.ModuleType]:

            if spec.name in sys.modules:
                return sys.modules[spec.name]

            ModelShim._log.info(f"Loading model shim module [{spec.name}]")

            return super().create_module(spec)

        def exec_module(self, module: types.ModuleType) -> None:

            if not self._exec_done:
                self._exec_done = True
                super().exec_module(module)


ModelShim._log = _util.logger_for_class(ModelShim)
ModelShim._init()  # noqa
