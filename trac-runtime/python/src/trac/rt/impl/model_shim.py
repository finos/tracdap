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

import importlib as _il
import importlib.util as _ilu
import importlib.machinery as _ilm
import logging
import pathlib
import sys
import contextlib

import types
import typing as _tp
import pathlib as _path
import itertools as _iter

import trac.rt.api as _api
import trac.rt.impl.util as _util


class ModelShim:

    SHIM_NAMESPACE = "trac.shim"

    _log: _tp.Optional[logging.Logger] = None

    __shim_id_seq = _iter.count()
    __shim_map: _tp.Dict[str, pathlib.Path] = dict()
    __shim: _tp.Optional[str] = None

    @classmethod
    def _init(cls):
        sys.meta_path.append(ModelShim.ModelShimFinder(cls.__shim_map))

    @classmethod
    def create_shim(cls, model_import_root: _tp.Union[str, _path.Path]) -> str:

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
    def use_checkout(cls, model_checkout: _tp.Union[str, _path.Path]):

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
        model_class = model_module.__dict__[class_name]

        return model_class

    class ActiveShimFinder(_ilm.PathFinder):

        def __init__(self, shim_namespace: str):
            self._shin_namespace = shim_namespace

        def find_spec(
                self,
                fullname: str,
                path: _tp.Optional[_tp.Sequence[_tp.Union[bytes, str]]] = None,
                target: _tp.Optional[types.ModuleType] = None) \
                -> _tp.Optional[_ilm.ModuleSpec]:

            if fullname.startswith(ModelShim.SHIM_NAMESPACE):
                return None

            shim_module = f"{self._shin_namespace}.{fullname}"

            ModelShim._log.info(f"Shim module map [{fullname}] -> [{shim_module}]")

            return _ilu.find_spec(shim_module)

            # return ModelShim.ModelShimFinder.find_spec(shim_module, path, target)

    class ModelShimFinder(_ilm.PathFinder):

        def __init__(self, shim_map: _tp.Dict[str, pathlib.Path]):
            self._shim_map = shim_map

        def find_spec(
                self,
                fullname: str,
                path: _tp.Optional[_tp.Sequence[_tp.Union[bytes, str]]] = None,
                target: _tp.Optional[types.ModuleType] = None) \
                -> _tp.Optional[_ilm.ModuleSpec]:

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

        def create_module(self, spec: _ilm.ModuleSpec) -> _tp.Optional[types.ModuleType]:

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
