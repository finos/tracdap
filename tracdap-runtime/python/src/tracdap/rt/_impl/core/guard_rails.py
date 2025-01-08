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

import inspect
import pathlib
import importlib
import sys
import traceback
import contextlib

import tracdap.rt.api as api
import tracdap.rt.exceptions as ex


def _get_model_entry_points():

    entry_points = []

    for member_name, member in api.TracModel.__dict__.items():
        if callable(member):
            entry_points.append(member_name)

    return entry_points


def _get_package_path(module_name):

    importlib.import_module(module_name)

    module = sys.modules[module_name]
    module_path = pathlib.Path(module.__spec__.origin)

    depth = module_name.count(".") + 1

    return module_path.parents[depth]


def run_model_guard(operation: str = None):

    # A simple guard method to block model code from accessing parts of the TRAC runtime framework
    # To blocks calls to the Python stdlib or 3rd party libs, use PythonGuardRails instead

    stack = inspect.stack()
    frame = stack[-1]

    if operation is None:
        operation = f"Calling {frame.function}()"

    for frame_index in range(len(stack) - 2, 0, -1):

        parent_frame = frame
        frame = stack[frame_index]

        if frame.function == "run_model" and parent_frame.function == "_execute":
            err = f"{operation} is not allowed inside run_model()"
            raise ex.ERuntimeValidation(err)


class PythonGuardRails:

    DANGEROUS_BUILTIN_FUNCTIONS = ["exec", "eval", "compile", "open", "input", "memoryview"]

    DANGEROUS_STDLIB_FUNCTIONS = [
        ("sys", "exit")
    ]

    MODEL_IMPORT_ENTRY_POINT = "trac_model_code_import"
    MODEL_ENTRY_POINTS = _get_model_entry_points()
    TRAC_PACKAGE_PATH: pathlib.Path = _get_package_path("tracdap.rt")
    SITE_PACKAGE_PATH: pathlib.Path = _get_package_path("pyarrow")

    PROTECTED_FUNC_STACK_DEPTH = 2

    REQUIRED_DEBUG_FUNCTIONS = ["exec", "eval", "compile"]

    REQUIRED_IMPORT_FUNCTIONS = {
        "exec": exec,
        "eval": eval,
        "compile": compile,
        "memoryview": memoryview
    }

    @classmethod
    @contextlib.contextmanager
    def enable_import_functions(cls):

        # Guard rails can interfere with module importing
        # This method turns off some of the rails that interfere with importing

        protected_builtins = {}

        try:

            for func_name, real_func in cls.REQUIRED_IMPORT_FUNCTIONS.items():
                protected_builtins[func_name] = __builtins__[func_name]  # noqa
                __builtins__[func_name] = real_func  # noqa

            yield

        finally:

            for func_name, real_func in cls.REQUIRED_IMPORT_FUNCTIONS.items():
                __builtins__[func_name] = protected_builtins[func_name]  # noqa

    @classmethod
    def protect_dangerous_functions(cls):

        for func_name in cls.DANGEROUS_BUILTIN_FUNCTIONS:
            raw_func = __builtins__[func_name]  # noqa
            __builtins__[func_name] = cls.protect_function(func_name, raw_func)  # noqa

        for module_name, func_name in cls.DANGEROUS_STDLIB_FUNCTIONS:

            # Make sure the module is loaded
            importlib.import_module(module_name)

            qualified_name = f"{module_name}.{func_name}"
            module = sys.modules[module_name]

            raw_func = module.__dict__[func_name]
            module.__dict__[func_name] = cls.protect_function(qualified_name, raw_func)

    @classmethod
    def protect_function(cls, func_name, func):

        # Do not re-wrap functions that are already wrapped if the runtime is started multiple times
        # E.g. running in embedded mode or during testing
        if hasattr(func, "__trac_protection__") and func.__trac_protection__:
            return func

        def protected_function(*args, **kwargs):

            model_code_details = cls.check_for_model_code(func_name)

            # Allow libraries and platform code to call the dangerous functions
            # Trying to restrict libraries or even Python itself causes too many errors to be practical
            if model_code_details is None:
                return func(*args, **kwargs)

            # Some dangerous functions are required by the debugger, e.g. exec, eval, compile
            # We definitely want the debugger to work for model code, so these have to be allowed
            if cls.is_debug() and func_name in cls.REQUIRED_DEBUG_FUNCTIONS:
                return func(*args, **kwargs)

            # ----------------------------------------------------------------------------------------------------------
            # Do not guard model code imports for now
            # More work is required to guard imports without breaking third-party libraries
            #
            # if cls.is_import() and func_name in cls.REQUIRED_IMPORT_FUNCTIONS:
            #    return func(*args, **kwargs)
            # ----------------------------------------------------------------------------------------------------------

            raise ex.EModelValidation(f"Calling {func_name}() is not allowed in model code: {model_code_details}")

        setattr(protected_function,  "__trac_protection__", True)

        return protected_function

    @classmethod
    def check_for_model_code(cls, func_name):

        # Traceback is a lot faster than inspect.stack()
        # If there is no model entry point in the traceback,
        # then we are not in model code and no further checks are needed

        # Do not guard model code imports for now
        # More work is required to guard imports without breaking third-party libraries

        trace = traceback.extract_stack()

        if not any(map(lambda f: f.name in cls.MODEL_ENTRY_POINTS, trace)):
            return None

        # If model entry points exist in the traceback we need to inspect the full stack
        # Calling inspect.stack() gives the stack with the top frame at index 0

        stack = inspect.stack()
        model_entry_depth = None

        # Check down from the top of the stack to find the depth of the model entry point

        for depth, frame in enumerate(stack):

            # We need to check the model entry point functions, to make sure they are really model entry points
            # E.g. run_model is a very generic name that could be used in other places
            # The test is whether one of the parameters is a TracModel instance
            # We do not look for the special name "self", that would give a really easy way to defeat the check

            if frame.function in cls.MODEL_ENTRY_POINTS:
                frame_locals = frame.frame.f_locals
                if any(map(lambda param: isinstance(param, api.TracModel), frame_locals.values())):
                    # If we have hit a model entry point, record the frame depth
                    # More checks are needed to see if control has left model code
                    model_entry_depth = depth
                    break

            # ----------------------------------------------------------------------------------------------------------
            # Do not guard model code imports for now
            # More work is required to guard imports without breaking third-party libraries
            #
            # As well as the model API entry points, we need to look for model code imports
            # Dangerous functions can execute in model scripts at global scope
            # if frame.function == cls.MODEL_IMPORT_ENTRY_POINT:
            #     if frame.frame.f_globals["__name__"] == "tracdap.rt._impl.shim":
            #         model_entry_depth = depth
            #         break
            # ----------------------------------------------------------------------------------------------------------

        # If we didn't find a really model entry point, then there is no need to check further

        if model_entry_depth is None:
            return None

        # Model entry point has been found

        model_stack = stack[cls.PROTECTED_FUNC_STACK_DEPTH:model_entry_depth + 1]
        first_model_frame = stack[model_entry_depth]
        last_model_frame = first_model_frame

        # --------------------------------------------------------------------------------------------------------------
        # Do not guard model code imports for now
        # More work is required to guard imports without breaking third-party libraries
        #
        # Check the entry point to see if it is a model code import
        # If it is the stack will be full of module import machinery
        # There's no easy way to get the file / line / statement where the call occurred
        # This may cause problems if model code imports libraries that use dangerous functions on init
        # if first_model_frame.function == cls.MODEL_IMPORT_ENTRY_POINT:
        #
        #     module_name = first_model_frame.frame.f_locals.get("module_name")
        #     return f"{func_name}() called during import of module [{module_name}]"
        # --------------------------------------------------------------------------------------------------------------

        # Now we know the entry point was an API call on the TracModel class
        # Look back up the stack from the entry point, to see if there is a call to a library
        # We need to ignore the protected func depth, which is the frames for this checking code
        # For this check, anything in site-packages or the path where TRAC is installed is considered a library
        # The python built-in modules are not considered an external call (this may need to change)

        for frame in reversed(model_stack):

            frame_path = pathlib.Path(frame.filename)

            if cls.path_is_in_dir(frame_path, cls.SITE_PACKAGE_PATH):
                return None

            if cls.path_is_in_dir(frame_path, cls.TRAC_PACKAGE_PATH):
                return None

            last_model_frame = frame

        # The call came from model code
        # Report details of the last model code frame

        filename = pathlib.Path(last_model_frame.filename).name
        snippet = trace[-(cls.PROTECTED_FUNC_STACK_DEPTH + 1)].line

        return f"{filename} line {last_model_frame.lineno}, {snippet}"

    @classmethod
    def path_is_in_dir(cls, path: pathlib.Path, directory: pathlib.Path):

        # path.is_relative_to() only appears in Python 3.9
        # We are still supporting 3.7 +

        return directory in path.parents

    @classmethod
    def is_debug(cls):

        has_trace = hasattr(sys, 'gettrace') and sys.gettrace() is not None
        has_breakpoint = sys.breakpointhook.__module__ != "sys"

        if has_trace or has_breakpoint:
            return True

        return False

    @classmethod
    def is_import(cls):

        trace = traceback.extract_stack()
        return any(map(lambda f: f.name == cls.MODEL_IMPORT_ENTRY_POINT, trace))
