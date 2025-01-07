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

import io as _io
import sys as _sys
import typing as _tp

from logging import *


class PlainFormatter(Formatter):

    FORMAT = f"%(asctime)s [%(threadName)s] %(levelname)s %(name)s" + \
                     f" - %(message)s"

    def __init__(self):
        super().__init__(self.FORMAT)


class ColorFormatter(Formatter):

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

        return Formatter.format(self, record)

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

    root_logger = getLogger()
    log_level = DEBUG if enable_debug else INFO

    if not root_logger.hasHandlers():

        console_formatter = ColorFormatter(is_bright=True)
        console_handler = StreamHandler(_sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(INFO)
        root_logger.addHandler(console_handler)
        root_logger.setLevel(log_level)

        # Use is_bright=False for logs from the TRAC runtime, so model logs stand out

        trac_logger = getLogger("tracdap.rt")

        console_formatter = ColorFormatter(is_bright=False)
        console_handler = StreamHandler(_sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(log_level)
        trac_logger.addHandler(console_handler)
        trac_logger.propagate = False


def logger_for_object(obj: object) -> Logger:
    return logger_for_class(obj.__class__)


def logger_for_class(clazz: type) -> Logger:
    qualified_class_name = f"{clazz.__module__}.{clazz.__name__}"
    return getLogger(qualified_class_name)


def logger_for_namespace(namespace: str) -> Logger:
    return getLogger(namespace)


class JobLogger(Logger):

    def __init__(self, sys_log: Logger, *handlers: Handler):

        super().__init__(sys_log.name, sys_log.level)
        self._sys_log = sys_log._log
        self._job_log = super()._log

        for handler in handlers:
            self.addHandler(handler)

    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):

        self._sys_log(level, msg, args, exc_info, extra, stack_info, stacklevel)
        self._job_log(level, msg, args, exc_info, extra, stack_info, stacklevel)


class LogProvider:

    def logger_for_object(self, obj: object) -> Logger:
        return logger_for_object(obj)

    def logger_for_class(self, clazz: type) -> Logger:
        return logger_for_class(clazz)

    def logger_for_namespace(self, namespace: str) -> Logger:
        return logger_for_namespace(namespace)


class JobLogProvider(LogProvider):

    def __init__(self, *handlers: Handler):
        self.__handlers = handlers

    def logger_for_object(self, obj: object) -> Logger:
        base_logger = logger_for_object(obj)
        return JobLogger(base_logger, *self.__handlers)

    def logger_for_class(self, clazz: type) -> Logger:
        base_logger = logger_for_class(clazz)
        return JobLogger(base_logger, *self.__handlers)

    def logger_for_namespace(self, namespace: str) -> Logger:
        base_logger = logger_for_namespace(namespace)
        return JobLogger(base_logger, *self.__handlers)


def job_log_provider(target: _tp.BinaryIO) -> JobLogProvider:

    stream = _io.TextIOWrapper(target, newline="\r\n")
    formatter = PlainFormatter()

    handler = StreamHandler(stream)
    handler.setFormatter(formatter)

    return JobLogProvider(handler)
