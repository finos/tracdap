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

import typing as tp
import types
import itertools
import logging
import pathlib
import sys
import contextlib
import functools as fn

import inspect
import importlib as _il
import importlib.util as _ilu
import importlib.abc as _ila
import importlib.machinery as _ilm
import importlib.resources as _ilr

import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.core.guard_rails as _guard
import tracdap.rt._impl.core.logging as _log
import tracdap.rt._impl.core.util as _util


class _Shim:

    def __init__(self, shim_id: int, namespace: str, search_locations: tp.List[str]):
        self.shim_id = shim_id
        self.namespace = namespace
        self.search_locations = search_locations


class _ActiveShim:

    # Wrapper class to hold a reference to the active shim
    # This reference needs to be shared across the various pieces of machinery for shim loading

    def __init__(self):
        self.shim: tp.Optional[str] = None
        self.main_module: tp.Optional[str] = None


class _ActiveShimFinder(_ila.MetaPathFinder):

    # The active shim finder trys to resolve global module imports in the active shim
    # For imports of the form: import acme_global.module
    # The module name is mapped: acme_global.module -> tracdap.shim._XXX.acme_global.module
    # The shimmed module name is passed back to importlib for resolution

    # This finder will handle loading the root model packages, after which
    # absolute and qualified imports are handled by the normal mechanism

    # Unqualified (i.e. relative) imports will always come to this loader
    # This is because the names are qualified before adding to the module cache
    # So, unqualified relative imports only work when the shim is active, i.e. during initial load
    # Relative imports used dynamically will not work (this is very bad practice anyway)!

    def __init__(self, shim_map: tp.Dict[str, _Shim], active_shim: _ActiveShim):
        self.__shim_map = shim_map
        self.__active_shim = active_shim

    def find_spec(
            self, fullname: str,
            path: tp.Optional[tp.Sequence[tp.Union[bytes, str]]] = None,
            target: tp.Optional[types.ModuleType] = None) \
            -> tp.Optional[_ilm.ModuleSpec]:

        # The active shim finder works by translating the supplied module name
        # E.g. acme.rockets -> import tracdap.shim._0.acme.rockets
        # The loader issue a new call to find_module for the translated name
        # Theis new call will hit the namespace shim finder

        # If the module name is already qualified with a shim, don't try to re-qualify it
        if fullname.startswith(ShimLoader.SHIM_NAMESPACE):
            return None

        # If there is no active shim set, don't return a module
        if self.__active_shim.shim is None:
            return None

        # Python adds the directory containing __main__ to PYTHONPATH ahead of other source dirs
        # This has the effect that relative imports are given priority in the loading order
        # To replicate this behavior, we can attempt a relative import before looking for absolute names

        # Different models have different main modules, so relative names will resolve differently
        # Several models can be loaded in the same shim, so we can't put relative imports in the root namespace
        # Instead we qualify relative imports, using the model main module as the relative root

        if self.__active_shim.main_module is not None:

            # Only try relative import if the relative root is already loaded
            shim_relative_root = f"{self.__active_shim.shim}.{self.__active_shim.main_module}"
            if shim_relative_root in sys.modules:

                # Relative root for packages is the root name
                # Relative root for modules is the parent of the root name
                shim_relative_module = sys.modules[shim_relative_root]
                if not shim_relative_module.__spec__.origin.endswith("__init__.py"):
                    shim_relative_root = shim_relative_root[:shim_relative_root.rfind(".")]

                # We need to check down the tree for the relative import
                # find_spec() will raise an error f we look for a module before the parent is loaded
                # We want to return None in this case, and fall back to the absolute import
                shim_relative_name = shim_relative_root
                shim_relative_spec = None
                for level in fullname.split("."):
                    shim_relative_name += f".{level}"
                    shim_relative_spec = _ilu.find_spec(shim_relative_name)
                    if shim_relative_spec is None:
                        break

                # If the relative module is found then we can return that spec
                if shim_relative_spec is not None:
                    return shim_relative_spec

        # No relative module available, so now try to load using the absolute module name
        shim_absolute_name = f"{self.__active_shim.shim}.{fullname}"
        return _ilu.find_spec(shim_absolute_name)

    def invalidate_caches(self) -> None:
        pass


class _NamespaceShimFinder(_ila.MetaPathFinder):

    def __init__(self, shim_map: tp.Dict[str, _Shim], active_shim: _ActiveShim):
        self.__shim_map = shim_map
        self.__active_shim = active_shim
        self._log = _log.logger_for_class(ShimLoader)

    def find_spec(
            self, fullname: str,
            path: tp.Optional[tp.Sequence[tp.Union[bytes, str]]] = None,
            target: tp.Optional[types.ModuleType] = None) \
            -> tp.Optional[_ilm.ModuleSpec]:

        module_parts = fullname.split(".")

        # Ignore requests for anything that is not a shim module
        if ".".join(module_parts[:2]) != ShimLoader.SHIM_NAMESPACE:
            return None

        # If this request is for the shim root itself, create a namespace package
        # This will be requested / created before any other shim modules
        if len(module_parts) == 2:
            spec = _ilm.ModuleSpec(fullname, origin=None, is_package=True, loader=None)
            return spec

        # The shim namespace is the first three parts of the module name, tracdap.shim._XXX
        # This should have been registered in the shim map by ShimLoader.create_shim
        shim_namespace = ".".join(module_parts[:3])
        shim = self.__shim_map.get(shim_namespace)

        # The relative module name is everything after the shim namespace
        # This is what would be written in the original model code, and will look familiar to modellers
        relative_parts = module_parts[3:]
        relative_name = ".".join(relative_parts)

        # If the shim is not registered, this is probably a bug
        # The loader mechanism should not generate requests to load from unregistered shims
        # The alternative is an explicit reference to a shim in model code, which should definitely not be allowed!
        if shim is None:

            self._log.error("There was an error in the model loading mechanism (this is a bug)")
            self._log.error(f"Attempt to load module [{relative_name}] from unregistered shim [{shim_namespace}]")
            raise _ex.ETracInternal("There was an error in the model loading mechanism (this is a bug)")

        # If this request is for a shim namespace package, create a namespace module spec
        # This should only happen after the shim has been created in ShimLoader
        # Attach the shim path to the spec, this will be the location to search for modules in the shim
        if len(module_parts) == 3:

            self._log.debug(f"Creating namespace package for shim [{shim_namespace}]")
            spec = _ilm.ModuleSpec(fullname, origin=None, is_package=True, loader=None)

            # Do not set search locations on the shim package
            # We want to force root packages back through the shim loader mechanism
            # Setting search locations will let them use the regular file-based finder / loader

            spec.submodule_search_locations = []

            return spec

        for source_path in shim.search_locations:

            shim_path = pathlib.Path(source_path)

            module_spec = self._try_load_module(
                fullname, relative_name, relative_parts,
                shim_namespace, shim_path)

            if module_spec is not None:
                return module_spec

        # Module not found, return None

        self._log.debug(f"Module [{relative_name}] not found in shim [{shim_namespace}]")

        return None

    def _try_load_module(
            self, fullname: str,
            relative_name: str, relative_parts: tp.List[str],
            shim_namespace, shim_path: pathlib.Path):

        # The requested module can be either a normal module or a package
        # The expected path is different for modules and packages, we need to check for both
        parent_path = fn.reduce(lambda p, q: p.joinpath(q), relative_parts[:-1], shim_path)
        package_path = parent_path.joinpath(relative_parts[-1], "__init__.py")
        module_path = parent_path.joinpath(relative_parts[-1] + ".py")

        # Windows still has a path length limit of 260 characters! Above that you have to use UNC-style paths
        # Model checkout paths can easily exceed the limit, to be safe always use UNC paths on Windows
        if _util.is_windows():
            package_path = pathlib.Path(f"\\\\?\\{str(package_path)}")
            module_path = pathlib.Path(f"\\\\?\\{str(module_path)}")

        # A module can exist as both a module and a package in the same source path
        # This is a very bad thing to do and should always be avoided
        # However, it is allowed when using the regular Python loader mechanisms
        # We want to replicate the same behaviour, to avoid unexpected breaks when loading to the platform
        # The Python behaviour is to give precedence to packages, so the shim loader should do the same

        if package_path.exists() and module_path.exists():
            self._log.warning(f"Module [{relative_name}] is both a regular module and a package (this causes problems)")

        # Packages have submodules which can be imported using global or relative import statements
        # To import global submodules the package needs to be in the global namespace
        # This can only happen when the shim is active, in which case we use the active shim loader
        # When the active shim is deactivated, shim modules will be removed from the global namespace
        # If the shim is not active the only option is a regular source loader in the shim namespace
        # This does not normally happen unless model code uses dynamic imports (highly unrecommended)!
        if package_path.exists() and package_path.is_file():

            self._log.debug(f"Loading package [{relative_name}] in shim [{shim_namespace}]")

            loader = _NamespaceShimLoader(
                self.__active_shim, shim_namespace, relative_name,
                fullname, str(package_path))

            spec = _ilm.ModuleSpec(fullname, origin=str(package_path), is_package=True, loader=loader)

            # Do not set search locations on the shim package
            # We want to force shim imports back through the shim loader mechanism
            # Setting search locations will let them use the regular file-based finder / loader

            spec.submodule_search_locations = []

            return spec

        # If the module path is found, return a spec for a regular module
        # The module exists in the shim namespace only
        # Since modules have no children, there will be no issues resolving submodules with absolute imports
        elif module_path.exists() and module_path.is_file():

            self._log.debug(f"Loading module [{relative_name}] in shim [{shim_namespace}]")

            loader = _ilm.SourceFileLoader(fullname, str(module_path))
            spec = _ilm.ModuleSpec(fullname, origin=str(module_path), loader=loader)

            return spec

    def invalidate_caches(self) -> None:
        pass


class _NamespaceShimLoader(_ilm.SourceFileLoader):

    # When loading in an active shim, put a global entry in sys.modules for the relative module name
    # Submodule loads will expect the parent module to be present with the relative module name
    # This behavior is built into the base implementation of Pythons module loading mechanism
    # It is important to add the global entry before the module is executed, which is when submodule imports happen

    # Global module names are removed from sys.modules when the shim is deactivated
    # At that point all module members are available in the shim namespace and global names are no longer needed

    # Logic code around import statements will work, so long as all that logic executes during the initial import
    # Very strange behaviors, like attempting dynamic imports at runtime, are likely to fail or behave erratically

    def __init__(self, active_shim, shim_namespace, relative_name, fullname, path):
        super().__init__(fullname, path)
        self.__active_shim = active_shim
        self._shim_namespace = shim_namespace
        self._relative_name = relative_name

    def create_module(self, spec: _ilm.ModuleSpec) -> tp.Optional[types.ModuleType]:

        return super().create_module(spec)

    def exec_module(self, module: types.ModuleType) -> None:

        if self.__active_shim.shim == self._shim_namespace:
            if self._relative_name not in sys.modules:
                sys.modules[self._relative_name] = module

        super().exec_module(module)


class ShimLoader:

    SHIM_NAMESPACE = "tracdap.shim"

    _T = tp.TypeVar("_T")

    _log: tp.Optional[logging.Logger] = None

    __shim_id_seq = itertools.count()
    __shim_map: tp.Dict[str, _Shim] = dict()
    __active_shim = _ActiveShim()

    @classmethod
    def _init(cls):

        sys.meta_path.append(_NamespaceShimFinder(cls.__shim_map, cls.__active_shim))
        sys.meta_path.append(_ActiveShimFinder(cls.__shim_map, cls.__active_shim))

    @classmethod
    def create_shim(cls, shim_root_path: tp.Union[str, pathlib.Path], source_paths: tp.List[str] = None) -> str:

        # If source paths not specified, preserve the old behavior and look in the shim root
        if not source_paths:
            source_paths = ["."]

        shim_id = next(cls.__shim_id_seq)
        shim_namespace = f"{cls.SHIM_NAMESPACE}._{shim_id}"
        shim_root = pathlib.Path(shim_root_path).resolve()

        search_locations = list(map(lambda p: str(shim_root.joinpath(p).resolve()), source_paths))

        cls.__shim_map[shim_namespace] = _Shim(shim_id, shim_namespace, search_locations)

        cls._log.debug(f"Creating shim [{shim_namespace}] for root path [{shim_root}]")

        _il.import_module(shim_namespace)

        return shim_namespace

    @classmethod
    def activate_shim(cls, shim_namespace: str):

        if shim_namespace not in cls.__shim_map:
            msg = f"Cannot activate module loading shim, shim is not registered for [{shim_namespace}]"
            cls._log.error(msg)
            raise _ex.ETracInternal(msg)

        if cls.__active_shim.shim is not None:
            msg = f"Cannot activate module loading shim, another shim is already active"
            cls._log.error(msg)
            raise _ex.ETracInternal(msg)

        cls.__active_shim.shim = shim_namespace
        shim_modules = {}

        # Put shim modules for the active shim back into the global namespace
        # For absolute imports, Python expects the module to exist in the global namespace
        # If the same shim is used multiple times, this prevents existing modules from being reloaded

        for module_name, module in sys.modules.items():
            module_parts = module_name.split(".")
            if len(module_parts) > 3 and ".".join(module_parts[:3]) == shim_namespace:
                relative_name = ".".join(module_parts[3:])
                shim_modules[relative_name] = module

        for relative_name, module in shim_modules.items():
            if relative_name not in sys.modules:
                sys.modules[relative_name] = module

    @classmethod
    def deactivate_shim(cls):

        if cls.__active_shim.shim is None:
            msg = f"Cannot deactivate module loading shim, no shim is active"
            cls._log.error(msg)
            raise _ex.ETracInternal(msg)

        shim_namespace = cls.__active_shim.shim
        shim_modules = []

        cls.__active_shim.shim = None

        # Remove shim modules from the global namespace
        # (prevents contamination across different shims)

        for module_name, module in sys.modules.items():
            shim_module_name = f"{shim_namespace}.{module_name}"
            if module.__name__ == shim_module_name:
                shim_modules.append(module_name)

        for module_name in shim_modules:
            del sys.modules[module_name]

    @classmethod
    @contextlib.contextmanager
    def use_shim(cls, shim_namespace: str):

        try:

            if shim_namespace:
                cls.activate_shim(shim_namespace)

            yield

        finally:

            if shim_namespace:
                cls.deactivate_shim()

    @classmethod
    def load_class(
            cls, module: tp.Union[types.ModuleType, str],
            class_name: str, class_type: tp.Type[_T]) -> tp.Type[_T]:

        cls._run_model_guard()

        if isinstance(module, types.ModuleType):
            module_name = module.__name__
        else:
            module_name = module

        try:

            cls._log.debug(f"Loading class [{class_name}] from [{module_name}]")

            if isinstance(module, str):
                try:
                    # Set the root module for class loading
                    # Relative imports will be resolved relative to this root
                    cls.__active_shim.main_module = module_name
                    module = cls.trac_model_code_import(module_name)
                finally:
                    cls.__active_shim.main_module = None

            class_ = module.__dict__.get(class_name)

            if class_ is None:
                err = f"Loading classes failed in module [{module_name}]: Class [{class_name}] not found"
                cls._log.error(err)
                raise _ex.EModelLoad(err)

            if not isinstance(class_, type):
                err = f"Loading classes failed in module [{module_name}]: [{class_name}] is not a class"
                cls._log.error(err)
                raise _ex.EModelLoad(err)

            if not issubclass(class_, class_type):
                err = f"Loading classes failed in module [{module_name}]: " \
                    + f"Class [{class_name}] does not extend [{class_type.__name__}]"
                cls._log.error(err)
                raise _ex.EModelLoad(err)

            return class_

        except _ex.EModelLoad:
            raise

        except (ModuleNotFoundError, NameError) as e:
            details = _util.error_details_from_exception(e)
            err = f"Loading classes failed in module [{module_name}]: {str(e)}{details}"
            cls._log.error(err)
            raise _ex.EModelLoad(err) from e

        except Exception as e:
            err = f"Loading classes failed in module [{module_name}]: {str(e)}"
            cls._log.error(err)
            raise _ex.EModelLoad(err) from e

    @classmethod
    def trac_model_code_import(cls, module_name):

        # Guard rails can interfere with import
        # This method turns off some of the rails that interfere with importing
        # This method name is used as a hook in PythonGuardRails, to detect model code imports

        with _guard.PythonGuardRails.enable_import_functions():
            return _il.import_module(module_name)

    @classmethod
    def load_resource(
            cls, module: tp.Union[types.ModuleType, str],
            resource_name: str) -> bytes:

        return cls._load_or_open_resource(module, resource_name, _ilr.read_binary)

    @classmethod
    def open_resource(
            cls, module: tp.Union[types.ModuleType, str],
            resource_name: str) -> tp.BinaryIO:

        return cls._load_or_open_resource(module, resource_name, _ilr.open_binary)

    @classmethod
    def _load_or_open_resource(
            cls, module: tp.Union[types.ModuleType, str], resource_name: str,
            load_func: tp.Callable[[types.ModuleType, str], tp.Union[bytes, tp.BinaryIO]]) \
            -> tp.Union[bytes, tp.BinaryIO]:

        cls._run_model_guard()

        if isinstance(module, types.ModuleType):
            module_name = module.__name__
        else:
            module_name = module

        try:

            cls._log.debug(f"Loading resource [{resource_name}] from [{module_name}]")

            if isinstance(module, str):
                module = _il.import_module(module_name)

            return load_func(module, resource_name)

        except _ex.EModelLoad:
            raise

        except (ModuleNotFoundError, NameError) as e:
            details = _util.error_details_from_exception(e)
            err = f"Loading resources failed in module [{module_name}]: {str(e)}{details}"
            cls._log.error(err)
            raise _ex.EModelLoad(err) from e

        except FileNotFoundError as e:
            err = f"Loading resources failed in module [{module_name}]: File not found for [{resource_name}]"
            cls._log.error(err)
            raise _ex.EModelLoad(err) from e

        except Exception as e:
            err = f"Loading resources failed in module [{module_name}]: Unexpected error"
            cls._log.error(err)
            raise _ex.EModelLoad(err) from e

    @classmethod
    def _run_model_guard(cls):

        # Loading resources from inside run_model is an invalid use of the runtime API
        # If a model attempts this, throw back a runtime validation error

        stack = inspect.stack()
        frame = stack[-1]

        for frame_index in range(len(stack) - 2, 0, -1):

            parent_frame = frame
            frame = stack[frame_index]

            if frame.function == "run_model" and parent_frame.function == "_execute":
                err = f"Loading resources is not allowed inside run_model()"
                cls._log.error(err)
                raise _ex.ERuntimeValidation(err)


ShimLoader._log = _log.logger_for_class(ShimLoader)
ShimLoader._init()  # noqa
