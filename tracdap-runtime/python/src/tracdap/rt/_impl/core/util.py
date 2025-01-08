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

import datetime as dt
import pathlib
import platform

import typing as tp
import uuid

import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as meta
import tracdap.rt.config as cfg

import traceback as tb


__IS_WINDOWS = platform.system() == "Windows"
__FIRST_MODEL_FRAME_NAME = "run_model"
__FIRST_MODEL_FRAME_TEST_NAME = "_callTestMethod"


def is_windows():
    return __IS_WINDOWS


def format_file_size(size: int) -> str:

    if size < 1024:
        if size == 0:
            return "0 bytes"
        elif size == 1:
            return "1 byte"
        else:
            return f"{size} bytes"

    if size < 1024 ** 2:
        kb = size / 1024
        return f"{kb:.1f} KB"

    if size < 1024 ** 3:
        mb = size / (1024 ** 2)
        return f"{mb:.1f} MB"

    gb = size / (1024 ** 3)
    return f"{gb:.1f} GB"


def new_object_id(object_type: meta.ObjectType) -> meta.TagHeader:

    timestamp = dt.datetime.utcnow()

    return meta.TagHeader(
        objectType=object_type,
        objectId=str(uuid.uuid4()),
        objectVersion=1,
        objectTimestamp=meta.DatetimeValue(timestamp.isoformat()),
        tagVersion=1,
        tagTimestamp=meta.DatetimeValue(timestamp.isoformat()))


def object_key(object_id: tp.Union[meta.TagHeader, meta.TagSelector]) -> str:

    if isinstance(object_id, meta.TagHeader):
        return f"{object_id.objectType.name}-{object_id.objectId}-v{object_id.objectVersion}"

    if object_id.objectVersion is not None:
        return f"{object_id.objectType.name}-{object_id.objectId}-v{object_id.objectVersion}"

    if object_id.objectAsOf is not None:
        return f"{object_id.objectType.name}-{object_id.objectId}-asof-{object_id.objectAsOf.isoDatetime}"

    if object_id.latestObject:
        return f"{object_id.objectType.name}-{object_id.objectId}-latest"

    raise ex.EUnexpected()


def selector_for(object_id: meta.TagHeader) -> meta.TagSelector:

    return meta.TagSelector(
        objectType=object_id.objectType,
        objectId=object_id.objectId,
        objectVersion=object_id.objectVersion,
        tagVersion=object_id.tagVersion)


def selector_for_latest(object_id: meta.TagHeader) -> meta.TagSelector:

    return meta.TagSelector(
        objectType=object_id.objectType,
        objectId=object_id.objectId,
        latestObject=True,
        latestTag=True)


def get_job_resource(
        selector: tp.Union[meta.TagHeader, meta.TagSelector],
        job_config: cfg.JobConfig,
        optional: bool = False):

    resource_key = object_key(selector)
    resource_id = job_config.resourceMapping.get(resource_key)

    if resource_id is not None:
        resource_key = object_key(resource_id)

    resource = job_config.resources.get(resource_key)

    if resource is not None:
        return resource

    if optional:
        return None

    err = f"Missing required {selector.objectType.name} resource [{object_key(selector)}]"
    raise ex.ERuntimeValidation(err)


def get_origin(metaclass: type):

    # Minimum supported Python is 3.7, which does not provide get_origin and get_args

    if "get_origin" in tp.__dict__:
        return tp.get_origin(metaclass)
    elif "__origin__" in metaclass.__dict__:
        return metaclass.__origin__  # noqa
    else:
        return None


def get_args(metaclass: type):

    # Minimum supported Python is 3.7, which does not provide get_origin and get_args

    if "get_args" in tp.__dict__:
        return tp.get_args(metaclass)
    elif "__args__" in metaclass.__dict__:
        return metaclass.__args__  # noqa
    else:
        return None


def get_constraints(metaclass: tp.TypeVar):

    return metaclass.__constraints__ or None


def get_bound(metaclass: tp.TypeVar):

    return metaclass.__bound__ or None


def try_clean_dir(dir_path: pathlib.Path, remove: bool = False) -> bool:

    normalized_path = windows_unc_path(dir_path)

    return __try_clean_dir(normalized_path, remove)


def __try_clean_dir(normalized_path, remove):

    clean_ok = True

    for item in normalized_path.iterdir():

        if item.is_dir():
            clean_ok &= __try_clean_dir(item, remove=True)

        else:
            try:
                item.unlink()
            except Exception as e:  # noqa
                clean_ok = False

    if remove:
        try:
            normalized_path.rmdir()
            return clean_ok
        except Exception:  # noqa
            return False


def windows_unc_path(path: pathlib.Path) -> pathlib.Path:

    # Convert a path to its UNC form on Windows

    if is_windows() and not str(path).startswith("\\\\?\\"):
        return pathlib.Path("\\\\?\\" + str(path.resolve()))
    else:
        return path


def error_details_from_trace(trace: tb.StackSummary):
    last_frame = trace[len(trace) - 1]
    filename = pathlib.PurePath(last_frame.filename).name
    # Do not report errors from inside C modules,
    # they will not be meaningful to users
    if filename.startswith("<"):
        return ""
    else:
        return f" ({filename} line {last_frame.lineno}, {last_frame.line})"


def error_details_from_model_exception(error: Exception, checkout_directory: pathlib.Path):
    trace = tb.extract_tb(error.__traceback__)
    filtered_trace = filter_model_stack_trace(trace, checkout_directory)
    return error_details_from_trace(filtered_trace)


def error_details_from_exception(error: Exception):
    trace = tb.extract_tb(error.__traceback__)
    return error_details_from_trace(trace)


def filter_model_stack_trace(full_stack: tb.StackSummary, checkout_directory: pathlib.Path):

    frame_names = list(map(lambda frame_: frame_.name, full_stack))

    if __FIRST_MODEL_FRAME_NAME in frame_names:
        first_model_frame = frame_names.index(__FIRST_MODEL_FRAME_NAME)
    elif __FIRST_MODEL_FRAME_TEST_NAME in frame_names:
        first_model_frame = frame_names.index(__FIRST_MODEL_FRAME_TEST_NAME)
    else:
        first_model_frame = 0

    last_model_frame = first_model_frame

    for frame_index, frame in enumerate(full_stack[first_model_frame:]):
        module_path = pathlib.Path(frame.filename)
        if "tracdap" in module_path.parts:
            tracdap_index = len(module_path.parts) - 1 - list(reversed(module_path.parts)).index("tracdap")
            if tracdap_index < len(module_path.parts)-1:
                if module_path.parts[tracdap_index+1] == "rt":
                    break
        if ("site-packages" in module_path.parts) or ("venv" in module_path.parts):
            break
        # is_relative_to only supported in Python 3.9+, we need to support 3.7
        if (checkout_directory is not None) and (checkout_directory not in module_path.parents):
            break
        last_model_frame = first_model_frame + frame_index

    return full_stack[first_model_frame:last_model_frame+1]
