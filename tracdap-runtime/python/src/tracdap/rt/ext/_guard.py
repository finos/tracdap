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
import tracdap.rt.exceptions as ex


def run_model_guard(operation: str = None):

    # Loading resources from inside run_model is an invalid use of the runtime API
    # If a model attempts this, throw back a runtime validation error

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
