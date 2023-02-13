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

import tracdap.rt.api as api
import tracdap.rt.exceptions as ex


class PythonGuardRails:

    # TODO: Do not block these calls running in the debugger

    # breakpoint, globals
    DANGEROUS_BUILTIN_FUNCTIONS = ["exec", "eval", "compile", "open", "input", "memoryview"]

    @classmethod
    def protect_dangerous_functions(cls):

        for func_name in cls.DANGEROUS_BUILTIN_FUNCTIONS:
            raw_func = __builtins__[func_name]  # noqa
            __builtins__[func_name] = cls.protect_function(func_name, raw_func)  # noqa

    @classmethod
    def protect_function(cls, func_name, func):

        if hasattr(func, "__trac_protection__") and func.__trac_protection__:
            return func

        def protected_function(*args, **kwargs):
            model_code_details = cls.check_for_model_code()
            if model_code_details is not None:
                raise ex.EModelValidation(f"Calling {func_name}() is not allowed in model code ({model_code_details})")
            else:
                return func(*args, **kwargs)

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
