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
import types
import itertools
import logging
import pathlib
import sys
import contextlib

import importlib as _il
import importlib.machinery as _ilm

import tracdap.rt.exceptions as _ex
import tracdap.rt.impl.util as _util


class _NamespaceShimFinder(_il.abc.MetaPathFinder):

    def __init__(self, shim_map: tp.Dict[str, pathlib.Path]):
        self.__shim_map = shim_map
        self._log = _util.logger_for_object(self)

    def find_spec(
            self, fullname: str,
            path: tp.Optional[tp.Sequence[tp.Union[bytes, str]]] = None,
            target: tp.Optional[types.ModuleType] = None) \
            -> tp.Optional[_ilm.ModuleSpec]:

        module_parts = fullname.split(".")  # TODO: Relative import

        if ".".join(module_parts[:2]) != ShimLoader.SHIM_NAMESPACE:
            return None

        if len(module_parts) == 2:
            spec = _ilm.ModuleSpec(fullname, origin=None, is_package=True, loader=None)
            return spec

        shim_namespace = ".".join(module_parts[:3])
        shim_path = self.__shim_map.get(shim_namespace)

        if shim_path is None:

            # TODO: More helpful error message
            self._log.warning(f"Attempt to load module [{fullname}] from unregistered shim [{shim_namespace}]")

            return None

        if len(module_parts) == 3:

            self._log.info(f"Created module spec for shim namespace [{shim_namespace}]")

            spec = _ilm.ModuleSpec(fullname, origin=None, is_package=True, loader=None)
            spec.submodule_search_locations = str(shim_path)

            return spec

        self._log.warning(f"Attempt to load submodule [{fullname}] in shim [{shim_namespace}]")

        return None

    def invalidate_caches(self) -> None:
        pass


class _ActiveShimFinder(_il.abc.MetaPathFinder):

    def __init__(self, shim_map: tp.Dict[str, pathlib.Path], active_shim: tp.Optional[str] = None):
        self.__shim_map = shim_map
        self.__activate_shim = active_shim
        self._log = _util.logger_for_object(self)

    def set_shim_map(self, shim_map: tp.Dict[str, pathlib.Path]):
        self.__shim_map = shim_map

    def set_active_shim(self, active_shim: tp.Optional[str]):
        self.__activate_shim = active_shim

    def find_spec(
            self, fullname: str,
            path: tp.Optional[tp.Sequence[tp.Union[bytes, str]]] = None,
            target: tp.Optional[types.ModuleType] = None) \
            -> tp.Optional[_ilm.ModuleSpec]:

        # If the module name is already qualified with a shim, don't use the active shim finder
        # Qualified modules can be looked up statically by the namespace shim finder
        if fullname.startswith(ShimLoader.SHIM_NAMESPACE):
            return None

        shim_module = f"{self.__activate_shim}.{fullname}"
        shim_path = self.__shim_map[self.__activate_shim]

        self._log.info(f"Looking for [{fullname}] in shim [{self.__activate_shim}]")

        module_parts = fullname.split(".")  # TODO: Handle relative imports?
        module_package_path = shim_path

        for i in range(len(module_parts) - 1):
            module_package_path = module_package_path.joinpath(module_parts[i])

        module_path = module_package_path.joinpath(module_parts[-1] + ".py")
        package_path = module_package_path.joinpath(module_parts[-1], "__init__.py")

        if module_path.exists() and module_path.is_file():

            shim_module_loader = _ActiveShimLoader(fullname, shim_module, str(module_path))
            spec = _ilm.ModuleSpec(shim_module, origin=str(module_path), loader=shim_module_loader)
            return spec

        if package_path.exists() and package_path.is_file():

            shim_module_loader = _ActiveShimLoader(fullname, shim_module, str(package_path))
            spec = _ilm.ModuleSpec(shim_module, origin=str(package_path), is_package=True, loader=shim_module_loader)
            spec.submodule_search_locations = str(package_path.parent)
            return spec

        return None

    def invalidate_caches(self) -> None:
        pass


class _ActiveShimLoader(_ilm.SourceFileLoader):

    def __init__(self, raw_module_name, shim_module_name, path):
        super().__init__(shim_module_name, path)
        self._raw_module_name = raw_module_name
        self._exec_done = False

    def create_module(self, spec: _ilm.ModuleSpec) -> tp.Optional[types.ModuleType]:

        return super().create_module(spec)

    def exec_module(self, module: types.ModuleType) -> None:

        # When loading in an active shim, put an additional entry in sys.modules for the raw module name
        # Submodule loads will expect the parent module to be present with the raw module name
        # This behavior is built into the base implementation of Pythons module loading mechanism

        # Raw module names are removed from sys.modules when the shim is deactivated
        # At that point all module members are available in the shim namespace and raw names are no longer needed
        # Logic code around import statements will work, so long as all that logic executes during the initial import
        # Very strange behaviors, like attempting dynamic imports at runtime, are likely to fail or behave erratically

        # It is important to add the entry for the raw module name before the module is executed
        # This is because the module may import from submodules,
        # which would otherwise cause either a lookup error or infinite recursion

        if self._raw_module_name not in sys.modules:
            sys.modules[self._raw_module_name] = module

        super().exec_module(module)


class ShimLoader:

    SHIM_NAMESPACE = "tracdap.shim"

    _T = tp.TypeVar("_T")

    _log: tp.Optional[logging.Logger] = None

    __shim_id_seq = itertools.count()
    __shim_map: tp.Dict[str, pathlib.Path] = dict()

    __shim: tp.Optional[str] = None
    __finder: tp.Optional[_ActiveShimFinder] = None

    @classmethod
    def _init(cls):

        sys.meta_path.append(_NamespaceShimFinder(cls.__shim_map))

    @classmethod
    def create_shim(cls, model_import_root: tp.Union[str, pathlib.Path]) -> str:

        shim_id = next(cls.__shim_id_seq)
        shim_namespace = f"{cls.SHIM_NAMESPACE}._{shim_id}"
        model_import_root = pathlib.Path(model_import_root).resolve()

        cls.__shim_map[shim_namespace] = model_import_root

        cls._log.info(f"Creating shim [{shim_namespace}] for path [{model_import_root}]")

        _il.import_module(shim_namespace)

        return shim_namespace

    @classmethod
    def activate_shim(cls, shim: str):

        cls.__shim = shim
        cls.__finder = _ActiveShimFinder(cls.__shim_map, cls.__shim)

        sys.meta_path.append(cls.__finder)

        # It may be useful to put raw name entries for modules in the active shim back into sys.modules
        # This will help if a shim is reactivated at a later point in time, e.g. to load additional resources

    @classmethod
    def deactivate_shim(cls):

        sys.meta_path.remove(cls.__finder)

        shim_namespace = cls.__shim
        shim_modules = []

        cls.__finder = None
        cls.__shim = None

        for module_name, module in sys.modules.items():
            shim_module_name = f"{shim_namespace}.{module_name}"
            if module.__name__ == shim_module_name:
                shim_modules.append(module_name)

        for module_name in shim_modules:
            del sys.modules[module_name]

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
    def load_class(
            cls, module: tp.Union[types.ModuleType, str],
            class_name: str, class_type: tp.Type[_T]) -> tp.Type[_T]:

        if isinstance(module, str):
            module_name = module
            module = _il.import_module(module_name)
        else:
            module_name = module.__name__

        class_ = module.__dict__.get(class_name)

        if class_ is None:
            error_msg = f"Class [{class_name}] was not found in module [{module_name}]"
            cls._log.error(error_msg)
            raise _ex.EModelRepoRequest(error_msg)

        if not isinstance(class_, class_type.__class__):
            error_msg = f"Class [{class_name}] is the wrong type (expected [{class_type.__name__}], got [{type(class_)}]"
            cls._log.error(error_msg)
            raise _ex.EModelRepoRequest(error_msg)

        return class_


ShimLoader._log = _util.logger_for_class(ShimLoader)
ShimLoader._init()  # noqa