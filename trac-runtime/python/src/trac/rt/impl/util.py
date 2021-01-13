#  Copyright 2021 Accenture Global Solutions Limited
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

import logging
import sys

_BLACK = '\033[0;30m'
_RED = '\033[0;31m'
_GREEN = '\033[0;32m'
_BROWN = '\033[0;33m'
_BLUE = '\033[0;34m'
_PURPLE = '\033[0;35m'
_CYAN = '\033[1;36m'
_GREY = '\033[0;37m'

_DARK_GREY = '\033[1;30m'
_LIGHT_RED = '\033[1;31m'
_LIGHT_GREEN = '\033[1;32m'
_YELLOW = '\033[1;33m'
_LIGHT_BLUE = '\033[1;34m'
_LIGHT_PURPLE = '\033[1;35m'
_LIGHT_CYAN = '\033[0;36m'
_WHITE = '\033[0;38m'


def configure_logging(clazz: type = None):

    root_logger = logging.getLogger()

    if not root_logger.hasHandlers():

        message_format = f"{_WHITE}%(asctime)s [%(threadName)s] %(levelname)s %(name)s - {_LIGHT_CYAN}%(message)s"
        console_formatter = logging.Formatter(message_format)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.INFO)

        # Put logs from the TRAC runtime in grey, so model logs stand out - quick solution!
        # A more sophisticated approach is needed anyway to output job logs as datasets

        trac_logger = logging.getLogger("trac")

        message_format = f"{_GREY}%(asctime)s [%(threadName)s] %(levelname)s %(name)s - %(message)s"
        console_formatter = logging.Formatter(message_format)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        trac_logger.addHandler(console_handler)
        trac_logger.propagate = False

    if clazz is None:
        startup_logger = root_logger
    else:
        startup_logger = logger_for_class(clazz)

    startup_logger.info("Logging enabled")


def logger_for_object(obj: object) -> logging.Logger:
    return logger_for_class(obj.__class__)


def logger_for_class(clazz: type) -> logging.Logger:
    qualified_class_name = f"{clazz.__module__}.{clazz.__name__}"
    return logging.getLogger(qualified_class_name)
