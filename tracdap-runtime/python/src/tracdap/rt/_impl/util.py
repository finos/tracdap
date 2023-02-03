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

import datetime as dt
import logging
import pathlib
import platform

import sys
import typing as tp
import uuid

import tracdap.rt.exceptions as ex
import tracdap.rt.metadata as meta
import tracdap.rt.config as cfg


__IS_WINDOWS = platform.system() == "Windows"


def is_windows():
    return __IS_WINDOWS


class ColorFormatter(logging.Formatter):

    _BLACK, _RED, _GREEN, _YELLOW, _BLUE, _MAGENTA, _CYAN, _WHITE, _DEFAULT_WHITE = range(9)
    _DARK_BASE = 30
    _LIGHT_BASE = 90

    # DARK_BASE + WHITE = light grey
    # DARK_BASE + DEFAULT_WHITE = regular console white
    # LIGHT_BASE + WHITE = bright white (0xffffff), very bright!

    def __init__(self, is_bright: bool):

        super().__init__(self._base_fmt(is_bright))
        self._level_colors = self._make_level_colors(is_bright)
        self._default_color = self._make_default_color(is_bright)

    def format(self, record):

        level_name = record.levelname
        level_color = self._level_colors.get(level_name)

        if level_color:
            record.levelname = level_color
        else:
            record.levelname = self._default_color + level_name

        return logging.Formatter.format(self, record)

    def _base_fmt(self, is_bright: bool):

        if is_bright:
            base_color = self._make_ansi_code(self._DARK_BASE, self._DEFAULT_WHITE, is_bold=False)
            message_color = self._make_ansi_code(self._LIGHT_BASE, self._CYAN, is_bold=False)
        else:
            base_color = self._make_ansi_code(self._DARK_BASE, self._WHITE, is_bold=False)
            message_color = self._make_ansi_code(self._DARK_BASE, self._CYAN, is_bold=False)

        return f"{base_color}%(asctime)s [%(threadName)s] %(levelname)s{base_color} %(name)s" + \
               f" - {message_color}%(message)s"

    def _make_level_colors(self, is_bright: bool):

        base_code = self._LIGHT_BASE if is_bright else self._DARK_BASE

        green = self._make_ansi_code(base_code, self._GREEN, is_bold=is_bright)
        yellow = self._make_ansi_code(base_code, self._YELLOW, is_bold=is_bright)
        red = self._make_ansi_code(base_code, self._RED, is_bold=is_bright)

        level_colors = {
            'CRITICAL': f"{red}CRITICAL",
            'ERROR': f"{red}ERROR",
            'WARNING': f"{yellow}WARNING",
            'INFO': f"{green}INFO"
        }

        return level_colors

    def _make_default_color(self, is_bright: bool):

        base_code = self._LIGHT_BASE if is_bright else self._DARK_BASE
        blue = self._make_ansi_code(base_code, self._BLUE, is_bold=is_bright)

        return blue

    @classmethod
    def _make_ansi_code(cls, base_code: int, color_offset: int, is_bold: bool):
        return f"\033[{1 if is_bold else 0};{base_code + color_offset}m"


def configure_logging(enable_debug=False):

    root_logger = logging.getLogger()
    log_level = logging.DEBUG if enable_debug else logging.INFO

    if not root_logger.hasHandlers():

        console_formatter = ColorFormatter(is_bright=True)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)
        root_logger.setLevel(log_level)

        # Use is_bright=False for logs from the TRAC runtime, so model logs stand out

        trac_logger = logging.getLogger("tracdap.rt")

        console_formatter = ColorFormatter(is_bright=False)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(log_level)
        trac_logger.addHandler(console_handler)
        trac_logger.propagate = False


def logger_for_object(obj: object) -> logging.Logger:
    return logger_for_class(obj.__class__)


def logger_for_class(clazz: type) -> logging.Logger:
    qualified_class_name = f"{clazz.__module__}.{clazz.__name__}"
    return logging.getLogger(qualified_class_name)


def logger_for_namespace(namespace: str) -> logging.Logger:
    return logging.getLogger(namespace)


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

    err = f"Missing required {selector.objectType} resource [{object_key(selector)}]"
    raise ex.EJobValidation(err)


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


def try_clean_dir(dir_path: pathlib.Path, remove: bool = False) -> bool:

    clean_ok = True
    normalized_path = dir_path.resolve()

    for item in normalized_path.iterdir():

        if item.is_dir():
            clean_ok &= try_clean_dir(item, remove=True)

        else:
            try:
                # Windows MAX_PATH = 260 characters, including the drive letter and terminating nul character
                # In Python the path string does not include a nul, so we need to limit to 259 characters
                if is_windows() and len(str(item)) >= 259 and not str(item).startswith("\\\\?\\"):
                    unc_item = pathlib.Path("\\\\?\\" + str(item))
                    unc_item.unlink()
                    return True
                else:
                    item.unlink()
                    return True
            except Exception as e:  # noqa
                return False

    if remove:
        try:
            normalized_path.rmdir()
            return clean_ok
        except Exception:  # noqa
            return False
