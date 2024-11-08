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


# These private helper functions are common across 2 or more plugins
# The _plugins package should not refer to internal code in rt._impl
# And we don't want to put them .ext, those are public APIs that need to be maintained

import logging
import pathlib
import platform
import urllib.parse
import typing as tp

import tracdap.rt.exceptions as _ex


def get_plugin_property(properties: tp.Dict[str, str], property_name: str):

    if property_name in properties:
        return properties[property_name]

    # Allow for properties set up via env variables on Windows
    # Python for Windows makes env var names uppercase when querying the environment
    # This will allow properties to be found, even if the case has been changed
    if is_windows():

        for key, value in properties.items():
            if key.lower() == property_name.lower():
                return value

    return None


def get_plugin_property_boolean(properties: tp.Dict[str, str], property_name: str, property_default: bool = False):

    property_value = get_plugin_property(properties, property_name)

    if property_value is None:
        return property_default

    if isinstance(property_value, bool):
        return property_value

    if isinstance(property_value, str):

        if len(property_value.strip()) == 0:
            return property_default

        if property_value.strip().lower() == "true":
            return True

        if property_value.strip().lower() == "false":
            return False

    raise _ex.EConfigParse(f"Invalid value for [{property_name}]: Expected a boolean value, got [{property_value}]")


# Handling for credentials supplied via HTTP(S) URLs

__HTTP_TOKEN_KEY = "token"
__HTTP_USER_KEY = "username"
__HTTP_PASS_KEY = "password"


def get_http_credentials(url: urllib.parse.ParseResult, properties: tp.Dict[str, str]) -> tp.Optional[str]:

    token = get_plugin_property(properties, __HTTP_TOKEN_KEY)
    username = get_plugin_property(properties, __HTTP_USER_KEY)
    password = get_plugin_property(properties, __HTTP_PASS_KEY)

    if token is not None:
        return token

    if username is not None and password is not None:
        return f"{username}:{password}"

    if url.username:
        credentials_sep = url.netloc.index("@")
        return url.netloc[:credentials_sep]

    return None


def split_http_credentials(credentials: str) -> tp.Tuple[tp.Optional[str], tp.Optional[str]]:

    if credentials is None:
        return None, None

    elif ":" in credentials:
        sep = credentials.index(":")
        username = credentials[:sep]
        password = credentials[sep + 1:]
        return username, password

    else:
        return credentials, None


def apply_http_credentials(url: urllib.parse.ParseResult, credentials: str) -> urllib.parse.ParseResult:

    if credentials is None:
        return url

    if url.username is None:
        location = f"{credentials}@{url.netloc}"

    else:
        location_sep = url.netloc.index("@")
        location = f"{credentials}@{url.netloc[location_sep + 1:]}"

    return url._replace(netloc=location)


# Logging helpers


def log_safe(param: tp.Any):

    if isinstance(param, urllib.parse.ParseResult) or isinstance(param, urllib.parse.ParseResultBytes):
        return log_safe_url(param)

    if isinstance(param, str):
        try:
            url = urllib.parse.urlparse(param)
            return log_safe_url(url)
        except ValueError:
            return param

    return param


def log_safe_url(url: tp.Union[str, urllib.parse.ParseResult, urllib.parse.ParseResultBytes]):

    if isinstance(url, str):
        url = urllib.parse.urlparse(url)

    if url.password:

        user_sep = url.netloc.index(":")
        pass_sep = url.netloc.index("@")

        user = url.netloc[:user_sep]
        safe_location = f"{user}:*****@{url.netloc[pass_sep + 1:]}"

        return url._replace(netloc=safe_location).geturl()  # noqa

    elif url.username:

        separator = url.netloc.index("@")
        safe_location = f"*****@{url.netloc[separator + 1:]}"

        return url._replace(netloc=safe_location).geturl()  # noqa

    else:
        return url.geturl()


# Duplicated code from rt._impl.util
# To avoid both duplication and circular dependencies,
# this code would need to be in .ext or a public module visible from .ext
# For the sake of 10 lines we can avoid maintaining a public API of util functions
# If lots more code gets added here, it might be time to re-think


__IS_WINDOWS = platform.system() == "Windows"


def is_windows():
    return __IS_WINDOWS


def windows_unc_path(path: pathlib.Path) -> pathlib.Path:

    # Convert a path to its UNC form on Windows

    if is_windows() and not str(path).startswith("\\\\?\\"):
        return pathlib.Path("\\\\?\\" + str(path.resolve()))
    else:
        return path


def logger_for_object(obj: object) -> logging.Logger:
    return logger_for_class(obj.__class__)


def logger_for_class(clazz: type) -> logging.Logger:
    qualified_class_name = f"{clazz.__module__}.{clazz.__name__}"
    return logging.getLogger(qualified_class_name)


def logger_for_namespace(namespace: str) -> logging.Logger:
    return logging.getLogger(namespace)
