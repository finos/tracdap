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

import typing as tp
import types
import itertools
import logging
import pathlib
import sys
import contextlib
import functools as fn
import traceback as tb

import inspect
import importlib as _il
import importlib.util as _ilu
import importlib.abc as _ila
import importlib.machinery as _ilm
import importlib.resources as _ilr

import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.util as _util


class _ActiveShim:

    # Wrapper class to hold a reference to the active shim
    # This reference needs to be shared across the various pieces of machinery for shim loading

    def __init__(self):
        self.shim: tp.Optional[str] = None
        self.root_module: tp.Optional[str] = None


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

    def __init__(self, shim_map: tp.Dict[str, pathlib.Path], active_shim: _ActiveShim):
        self.__shim_map = shim_map
        self.__active_shim = active_shim

    def set_shim_map(self, shim_map: tp.Dict[str, pathlib.Path]):
        self.__shim_map = shim_map

    def set_active_shim(self, active_shim: tp.Optional[str]):
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

        shim_path = self.__shim_map.get(self.__active_shim.shim)
        module_spec = None

        # Python adds the directory containing __main__ to PYTHONPATH ahead of other source dirs
        # This has the effect that relative imports are given priority in the loading order
        # To replicate this behavior, we can attempt a relative import before looking for qualified names

        # The root package has to be loaded before relative imports are attempted
        # I.e. a.b is the root, do not look for a.b.a (relative import) before loading a.b

        if self._root_exists_and_is_loaded():

            # Relative imports are different for packages and modules
            # If the root is a package, relative imports are at the same level as the package
            # IF the root is a module, relative imports are at the parent level

            root_module = self.__active_shim.root_module
            root_module_file = shim_path.joinpath(root_module.replace(".", "/") + ".py")
            root_package_file = shim_path.joinpath(root_module.replace(".", "/")).joinpath("__init__.py")

            # Attempt relative import if the root module is a package

            if root_package_file.exists():
                relative_module = f"{self.__active_shim.shim}.{root_module}.{fullname}"
                module_spec = _ilu.find_spec(relative_module, root_package_file.parent)

            # Attempt relative import if the root module is a module
            # Not needed if the root module is in the root of the shim,
            # in which case the relative and absolute module paths will be the same

            elif "." in root_module:
                parent_package = root_module[:root_module.rfind(".")]
                relative_module = f"{self.__active_shim.shim}.{parent_package}.{fullname}"
                module_spec = _ilu.find_spec(relative_module, root_module_file.parent)

        # The last option is to look for the module using an absolute import in the root of the shim
        # This is in line with Python's own behavior, which gives priority to relative imports

        if module_spec is None:

            shim_module = f"{self.__active_shim.shim}.{fullname}"
            module_spec = _ilu.find_spec(shim_module, str(shim_path))

        return module_spec

    def invalidate_caches(self) -> None:
        pass

    def _root_exists_and_is_loaded(self):

        if self.__active_shim.root_module is None:
            return False

        root_module = self.__active_shim.root_module
        root_package = root_module[:root_module.find('.')]
        shim_root_package = f"{self.__active_shim.shim}.{root_package}"

        return shim_root_package in sys.modules


class _ActiveShimLoader(_ilm.SourceFileLoader):

    # When loading in an active shim, put a global entry in sys.modules for the relative module name
    # Submodule loads will expect the parent module to be present with the relative module name
    # This behavior is built into the base implementation of Pythons module loading mechanism
    # It is important to add the global entry before the module is executed, which is when submodule imports happen

    # Global module names are removed from sys.modules when the shim is deactivated
    # At that point all module members are available in the shim namespace and global names are no longer needed

    # Logic code around import statements will work, so long as all that logic executes during the initial import
    # Very strange behaviors, like attempting dynamic imports at runtime, are likely to fail or behave erratically

    def __init__(self, relative_name, fullname, path):
        super().__init__(fullname, path)
        self._relative_name = relative_name

    def create_module(self, spec: _ilm.ModuleSpec) -> tp.Optional[types.ModuleType]:

        return super().create_module(spec)

    def exec_module(self, module: types.ModuleType) -> None:

        if self._relative_name not in sys.modules:
            sys.modules[self._relative_name] = module

        super().exec_module(module)


class _NamespaceShimFinder(_ila.MetaPathFinder):

    def __init__(self, shim_map: tp.Dict[str, pathlib.Path], active_shim: _ActiveShim):
        self.__shim_map = shim_map
        self.__active_shim = active_shim
        self._log = _util.logger_for_class(ShimLoader)

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
        shim_path = self.__shim_map.get(shim_namespace)

        # The relative module name is everything after the shim namespace
        # This is what would be written in the original model code, and will look familiar to modellers
        relative_parts = module_parts[3:]
        relative_name = ".".join(relative_parts)

        # If the shim is not registered, this is probably a bug
        # The loader mechanism should not generate requests to load from unregistered shims
        # The alternative is an explicit reference to a shim in model code, which should definitely not be allowed!
        if shim_path is None:

            self._log.error("There was an error in the model loading mechanism (this is a bug)")
            self._log.error(f"Attempt to load module [{relative_name}] from unregistered shim [{shim_namespace}]")
            raise _ex.ETracInternal("There was an error in the model loading mechanism (this is a bug)")

        # If this request is for a shim namespace package, create a namespace module spec
        # This should only happen after the shim has been created in ShimLoader
        # Attach the shim path to the spec, this will be the location to search for modules in the shim
        if len(module_parts) == 3:

            self._log.debug(f"Creating namespace package for shim [{shim_namespace}]")

            spec = _ilm.ModuleSpec(fullname, origin=None, is_package=True, loader=None)
            spec.submodule_search_locations = str(shim_path)

            return spec

        # Once the code falls through to here, the request is for a module inside a recognized shim

        # The requested module can be either a normal module or a package
        # The expected path is different for modules and packages, we need to check for both
        parent_path = fn.reduce(lambda p, q: p.joinpath(q), relative_parts[:-1], shim_path)
        package_path = parent_path.joinpath(relative_parts[-1], "__init__.py")
        module_path = parent_path.joinpath(relative_parts[-1] + ".py")

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

            if shim_namespace == self.__active_shim.shim:
                shim_loader = _ActiveShimLoader(relative_name, fullname, str(package_path))
            else:
                shim_loader = _ilm.SourceFileLoader(fullname, str(package_path))

            spec = _ilm.ModuleSpec(fullname, origin=str(package_path), is_package=True, loader=shim_loader)
            spec.submodule_search_locations = [str(package_path.parent)]
            return spec

        # If the module path is found, return a spec for a regular module
        # The module exists in the shim namespace only
        # Since modules have no children, there will be no issues resolving submodules with absolute imports
        elif module_path.exists() and module_path.is_file():

            self._log.debug(f"Loading module [{relative_name}] in shim [{shim_namespace}]")

            shim_loader = _ilm.SourceFileLoader(fullname, str(module_path))
            spec = _ilm.ModuleSpec(fullname, origin=str(module_path), loader=shim_loader)
            return spec

        # Module not found, return None
        else:
            self._log.debug(f"Module [{relative_name}] not found in shim [{shim_namespace}]")
            return None

    def invalidate_caches(self) -> None:
        pass


class ShimLoader:

    SHIM_NAMESPACE = "tracdap.shim"

    _T = tp.TypeVar("_T")

    _log: tp.Optional[logging.Logger] = None

    __shim_id_seq = itertools.count()
    __shim_map: tp.Dict[str, pathlib.Path] = dict()
    __active_shim = _ActiveShim()

    @classmethod
    def _init(cls):

        sys.meta_path.append(_NamespaceShimFinder(cls.__shim_map, cls.__active_shim))
        sys.meta_path.append(_ActiveShimFinder(cls.__shim_map, cls.__active_shim))

    @classmethod
    def create_shim(cls, package_root: tp.Union[str, pathlib.Path]) -> str:

        shim_id = next(cls.__shim_id_seq)
        shim_namespace = f"{cls.SHIM_NAMESPACE}._{shim_id}"
        package_root = pathlib.Path(package_root).resolve()

        cls.__shim_map[shim_namespace] = package_root

        cls._log.debug(f"Creating shim [{shim_namespace}] for path [{package_root}]")

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
                    cls.__active_shim.root_module = module_name
                    module = _il.import_module(module_name)
                finally:
                    cls.__active_shim.root_module = None

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
            details = cls._error_details(e)
            err = f"Loading classes failed in module [{module_name}]: {str(e)}{details}"
            cls._log.error(err)
            raise _ex.EModelLoad(err) from e

        except Exception as e:
            err = f"Loading classes failed in module [{module_name}]: Unexpected error"
            cls._log.error(err)
            raise _ex.EModelLoad(err) from e

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
            details = cls._error_details(e)
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

    @classmethod
    def _error_details(cls, error: Exception):

        trace = tb.extract_tb(error.__traceback__)
        last_frame = trace[len(trace) - 1]
        filename = pathlib.PurePath(last_frame.filename).name

        # Do not report errors from inside C modules,
        # they will not be meaningful to users
        if filename.startswith("<"):
            return ""
        else:
            return f" ({filename} line {last_frame.lineno}, {last_frame.line})"


ShimLoader._log = _util.logger_for_class(ShimLoader)
ShimLoader._init()  # noqa
