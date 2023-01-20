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


# These private helper functions are common across 2 or more plugins
# The _plugins package should not refer to internal code in rt._impl
# And we don't want to put them .ext, those are public APIs that need to be maintained

import logging as _log
import platform as _platform
import urllib.parse as _url_parse
import typing as _tp


__IS_WINDOWS = _platform.system() == "Windows"


def is_windows():
    return __IS_WINDOWS


def logger_for_object(obj: object) -> _log.Logger:
    return logger_for_class(obj.__class__)


def logger_for_class(clazz: type) -> _log.Logger:
    qualified_class_name = f"{clazz.__module__}.{clazz.__name__}"
    return _log.getLogger(qualified_class_name)


def logger_for_namespace(namespace: str) -> _log.Logger:
    return _log.getLogger(namespace)


def log_safe(param: _tp.Any):

    if isinstance(param, _url_parse.ParseResult) or isinstance(param, _url_parse.ParseResultBytes):
        return log_safe_url(param)

    if isinstance(param, str):
        try:
            url = _url_parse.urlparse(param)
            return log_safe_url(url)
        except ValueError:
            return param

    return param


def log_safe_url(url: _tp.Union[str, _url_parse.ParseResult, _url_parse.ParseResultBytes]):

    if isinstance(url, str):
        url = _url_parse.urlparse(url)

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


def get_plugin_property(properties: _tp.Dict[str, str], property_name: str):

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


# Helper functions for handling credentials supplied via HTTP(S) URLs

__HTTP_TOKEN_KEY = "token"
__HTTP_USER_KEY = "username"
__HTTP_PASS_KEY = "password"


def get_http_credentials(url: _url_parse.ParseResult, properties: _tp.Dict[str, str]) -> _tp.Optional[str]:

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


def apply_http_credentials(url: _url_parse.ParseResult, credentials: str) -> _url_parse.ParseResult:

    if credentials is None:
        return url

    if url.username is None:
        location = f"{credentials}@{url.netloc}"

    else:
        location_sep = url.netloc.index("@")
        location = f"{credentials}@{url.netloc[location_sep + 1:]}"

    return url._replace(netloc=location)
