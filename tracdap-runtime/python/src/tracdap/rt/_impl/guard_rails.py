#  Copyright 2023 Accenture Global Solutions Limited
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

import inspect
import pathlib
import importlib
import sys

import tracdap.rt.api as api
import tracdap.rt.exceptions as ex


class PythonGuardRails:

    DANGEROUS_BUILTIN_FUNCTIONS = ["exec", "eval", "compile", "open", "input", "memoryview"]

    DANGEROUS_STDLIB_FUNCTIONS = [
        ("sys", "exit")
    ]

    REQUIRED_DEBUG_FUNCTIONS = ["exec", "eval", "compile"]

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

            model_code_details = cls.check_for_model_code()

            # Allow libraries and platform code to call the dangerous functions
            # Trying to restrict libraries or even Python itself causes too many errors to be practical
            if model_code_details is None:
                return func(*args, **kwargs)

            # Some dangerous functions are required by the debugger, e.g. exec, eval, compile
            # We definitely want the debugger to work for model code, so these have to be allowed
            if cls.is_debug() and func_name in cls.REQUIRED_DEBUG_FUNCTIONS:
                return func(*args, **kwargs)

            raise ex.EModelValidation(f"Calling {func_name}() is not allowed in model code ({model_code_details})")

        setattr(protected_function,  "__trac_protection__", True)

        return protected_function

    @classmethod
    def check_for_model_code(cls):

        stack = inspect.stack()

        for frame in stack:
            if frame.function is not None and frame.function == "run_model":
                for param_name, param in frame.frame.f_locals.items():
                    if isinstance(param, api.TracModel):
                        filename = pathlib.Path(frame.filename).name
                        return f"{filename} line {frame.lineno}"

        return None

    @classmethod
    def is_debug(cls):

        get_trace_func = getattr(sys, 'gettrace', None)

        if get_trace_func is None:
            return False

        else:
            trace = get_trace_func()
            return trace is not None
