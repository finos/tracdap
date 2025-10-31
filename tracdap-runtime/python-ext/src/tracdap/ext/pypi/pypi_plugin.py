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

import html.parser
import json.decoder
import pathlib
import re
import typing as tp
import urllib.parse
import zipfile
import io
import time

import tracdap.rt.config as cfg
import tracdap.rt.metadata as meta
import tracdap.rt.exceptions as ex
import tracdap.rt.ext.plugins as plugins
import tracdap.rt.ext.util as util

# Import repo interface
from tracdap.rt.ext.repos import *


class PyPiRepository(IModelRepository):

    PIP_INDEX_KEY = "pipIndex"
    PIP_INDEX_URL_KEY = "pipIndexUrl"
    USERNAME_KEY = "username"
    PASSWORD_KEY = "password"
    TOKEN_KEY = "token"

    SIMPLE_PACKAGE_PATH = "{}/{}/"
    JSON_PACKAGE_PATH = "{}/{}/{}/json"

    PIP_SIMPLE_FORMAT_KEY = "pipSimpleFormat"
    PIP_SIMPLE_FORMAT_JSON = "json"
    PIP_SIMPLE_FORMAT_HTML = "html"
    PIP_SIMPLE_FORMAT_DEFAULT = PIP_SIMPLE_FORMAT_JSON

    PIP_SIMPLE_CONTENT_TYPES = {
        PIP_SIMPLE_FORMAT_JSON: "application/vnd.pypi.simple.v1+json",
        PIP_SIMPLE_FORMAT_HTML: "text/html"
    }

    PIP_SIMPLE_TYPE_JSON = "application/vnd.pypi.simple.v1+json"
    PIP_SIMPLE_TYPE_HTML = "text/html"

    def __init__(
            self, config: cfg.PluginConfig,
            network_manager: plugins.INetworkManager,
            log_provider: plugins.ILogProvider):

        self._log = log_provider.logger_for_object(self)

        self._config = config
        self._pool_manager = network_manager.create_urllib3_pool_manager(config)

        self._pip_index = util.read_plugin_config(self._config, self.PIP_INDEX_KEY, optional=True)
        self._pip_index_url = util.read_plugin_config(self._config, self.PIP_INDEX_URL_KEY, optional=True)
        self._pip_simple_format = util.read_plugin_config(self._config, self.PIP_SIMPLE_FORMAT_KEY, optional=True)

        if self._pip_index is None and self._pip_index_url is None:
            message = f"Neither [{self.PIP_INDEX_KEY}] nor [{self.PIP_INDEX_URL_KEY} is set in PyPi repository config"
            raise ex.EConfigParse(message)

    def package_path(
            self, model_def: meta.ModelDefinition,
            checkout_dir: pathlib.Path) -> pathlib.Path:

        return checkout_dir

    def do_checkout(self, model_def: meta.ModelDefinition, checkout_dir: pathlib.Path) -> pathlib.Path:

        self._log.info(
            f"PyPI checkout: repo = [{model_def.repository}], " +
            f"package = [{model_def.package}], version = [{model_def.version}]")

        self._log.info(f"Checkout location: [{checkout_dir}]")

        if self._pip_index_url is not None:
            package_filename, package_url = self._pypi_simple_query(model_def)

        else:
            package_filename, package_url = self._pypi_json_query(model_def)

        self._log.info(f"Downloading [{package_filename}]")
        self._log.info(f"GET: {util.hide_http_credentials(package_url)}")

        download = None

        try:
            start = time.perf_counter()
            download = self._pool_manager.request("GET", package_url.geturl())
            content = download.data
            elapsed = time.perf_counter() -  start

            self._log.info(f"Downloaded [{len(content) / 1024:.1f}] KB in [{elapsed:.1f}] seconds")

            download_whl = zipfile.ZipFile(io.BytesIO(content))
            download_whl.extractall(checkout_dir)

            self._log.info(f"Unpacked [{len(download_whl.filelist)}] files")
            self._log.info(f"PyPI checkout succeeded for {model_def.package} {model_def.version}")

            return self.package_path(model_def, checkout_dir)

        finally:
            if download is not None:
                download.close()

    def _pypi_simple_query(self, model_def: meta.ModelDefinition):

        # PEP describing PyPI simple protocol
        # https://peps.python.org/pep-0691/

        package_query = None

        try:

            simple_root_url = urllib.parse.urlparse(self._pip_index_url)
            simple_content_type = self._pypi_simple_content_type()
            simple_headers = {"accept": simple_content_type}

            credentials = self._get_http_credentials()

            self._log.info(f"Query package: [{model_def.package}]")

            package_query = self._pypi_package_query(
                self.SIMPLE_PACKAGE_PATH, simple_root_url, simple_headers,
                credentials, model_def)

            # Default content type is text/html
            # Content type can contain modifiers, e.g. text/html; charset=utf-8
            # We only want the mime type part
            received_content_type = package_query.headers.get("content-type") or self.PIP_SIMPLE_TYPE_HTML
            received_mime_type = received_content_type.split(";")[0].strip()

            if received_mime_type == self.PIP_SIMPLE_TYPE_JSON:
                filename, url = self._pypi_simple_parse_response(model_def, json.loads(package_query.data))

            elif received_mime_type == self.PIP_SIMPLE_TYPE_HTML:
                package_parser = _PypiSimpleHtmlParser(model_def.package, package_query.url)
                package_parser.feed(package_query.data.decode("utf-8"))
                filename, url = self._pypi_simple_parse_response(model_def, package_parser.response)

            else:
                err = f"Invalid response from package repository: Content type = [{received_content_type}]"
                self._log.error(err)
                raise ex.EModelRepo(err)

            package_url = urllib.parse.urlparse(url)
            package_url = util.apply_http_credentials(package_url, credentials)

            return filename, package_url

        except json.JSONDecodeError as e:
            msg = f"Invalid response from model repository: {str(e)}"
            self._log.error(msg)
            raise ex.EModelRepo("Invalid response from model repository") from e

        finally:
            if package_query is not None:
                package_query.close()

    def _pypi_simple_content_type(self):

        if self._pip_simple_format in self.PIP_SIMPLE_CONTENT_TYPES:
            return self.PIP_SIMPLE_CONTENT_TYPES[self._pip_simple_format]

        if self._pip_simple_format is None:
            return self.PIP_SIMPLE_CONTENT_TYPES[self.PIP_SIMPLE_FORMAT_DEFAULT]

        self._log.warning(f"Unknown PyPI format [{self._pip_simple_format}], using [{self.PIP_SIMPLE_FORMAT_DEFAULT}]")

        return self.PIP_SIMPLE_CONTENT_TYPES[self.PIP_SIMPLE_FORMAT_DEFAULT]

    def _pypi_simple_parse_response(self, model_def, package_obj):

        package_name = package_obj.get("name") or model_def.package
        files = package_obj.get("files") or []

        name_pattern = package_name.replace("-", "_")  # dash is replaced by underscore in wheel names
        version_pattern = model_def.version.replace(".", "\\.")
        file_pattern = re.compile(f"^{name_pattern}-{version_pattern}-(?P<target>.*)\\.whl")

        matches = []

        for file_info in files:

            filename = file_info.get("filename") or ""
            yanked = file_info.get("yanked") or False
            match = file_pattern.match(filename)

            if match and not yanked:

                url = file_info.get("url")
                target = match.group("target")

                match_info = filename, url, target
                matches.append(match_info)

        if len(matches) == 0:
            message = f"No package found for [{package_name}] version [{model_def.version}]"
            self._log.error(message)
            raise ex.EModelRepo(message)

        if len(matches) > 1:
            message = f"Multiple packages found for [{package_name}] version [{model_def.version}]" + \
                      f" (targets: " + ", ".join(map(lambda m: m[2], matches)) + ")"
            self._log.error(message)
            raise ex.EModelRepo(message)

        filename, url, target = matches[0]

        self._log.info(f"Found package [{package_name}] version [{model_def.version}], target = [{target}]")

        return filename, url

    def _pypi_json_query(self, model_def: meta.ModelDefinition):

        package_query = None

        try:

            json_root_url = urllib.parse.urlparse(self._pip_index)
            json_headers = {"accept": "application/json"}

            credentials = self._get_http_credentials()

            self._log.info(f"Query package: [{model_def.package}], version = [{model_def.version}]")

            package_query = self._pypi_package_query(
                self.JSON_PACKAGE_PATH, json_root_url, json_headers,
                credentials, model_def)

            package_obj = json.loads(package_query.data)
            package_info = package_obj.get("info") or {}
            summary = package_info.get("summary") or "(summary not available)"

            self._log.info(f"Package summary: {summary}")

            urls = package_obj.get("urls") or []
            bdist_urls = list(filter(lambda d: d.get("packagetype") == "bdist_wheel", urls))

            if not bdist_urls:
                message = "No compatible packages found"
                self._log.error(message)
                raise ex.EModelRepo(message)

            if len(bdist_urls) > 1:
                message = "Multiple compatible packages found (specialized distributions are not supported yet)"
                self._log.error(message)
                raise ex.EModelRepo(message)

            package_url_info = bdist_urls[0]
            package_filename = package_url_info.get("filename")
            package_url = urllib.parse.urlparse(package_url_info.get("url"))
            package_url = util.apply_http_credentials(package_url, credentials)

            return package_filename, package_url

        finally:
            if package_query is not None:
                package_query.close()

    def _pypi_package_query(self, package_path_template, root_url, headers, credentials, model_def):

        root_url: urllib.parse.ParseResult = util.apply_http_credentials(root_url, credentials)
        package_path = package_path_template.format(root_url.path, model_def.package, model_def.version)
        package_url = root_url._replace(path=package_path)

        self._log.info(f"GET: {util.hide_http_credentials(package_url)}")

        # Pool manager handles connection lifecycle, explicit release is not required
        package_query = self._pool_manager.request("GET", package_url.geturl(), headers=headers)

        if package_query.status != 200:
            message = f"Package lookup failed: [{package_query.status}] {package_query.reason}"
            package_query.close()
            self._log.error(message)
            raise ex.EModelRepo(message)  # todo status code for access, not found etc

        return package_query

    def _get_http_credentials(self) -> str | None:

        token = util.read_plugin_config(self._config, self.TOKEN_KEY, optional=True)
        username = util.read_plugin_config(self._config, self.USERNAME_KEY, optional=True)
        password = util.read_plugin_config(self._config, self.PASSWORD_KEY, optional=True)

        if token is not None:
            return token

        if username is not None and password is not None:
            return f"{username}:{password}"

        return None


class _PypiSimpleHtmlParser(html.parser.HTMLParser):

    def __init__(self, package_name: str, package_url: str):

        super().__init__()

        self._package_url = package_url
        self._stack = []
        self._data = None

        # Empty response object as per PEP-691
        # https://peps.python.org/pep-0691/#project-detail

        self.response = {
            "meta": {"api-version": None},
            "name": package_name,
            "files": []
        }

    def handle_starttag(self, tag: str, attrs: tp.List[tp.Tuple[str, tp.Union[str, None]]]):

        element = (tag, attrs)
        self._stack.append(element)
        self._data = None

        if tag == "meta" and any(map(lambda a: a[0] == "name" and a[1] == "pypi:repository-version", attrs)):
            self._record_meta(attrs)

    def handle_endtag(self, tag: str):

        stack_tag, stack_attrs = self._stack.pop()

        if tag == "a" and stack_tag == "a" and self._data is not None:
            self._record_link(self._data, stack_attrs)

        while stack_tag != tag and len(self._stack) > 0:
            stack_tag, _ = self._stack.pop()

    def handle_data(self, data: str):

        stripped_data = data.strip()
        self._data = stripped_data if len(stripped_data) > 0 else None

    def _record_meta(self, attrs: tp.List[tp.Tuple[str, tp.Union[str, None]]]):

        content_attr = list(filter(lambda a: a[0] == "content", attrs))

        if len(content_attr) == 1:
            content, version = content_attr[0]
            self.response["meta"]["api-version"] = version

    def _record_link(self, filename, attrs: tp.List[tp.Tuple[str, tp.Union[str, None]]]):

        href_attr = list(filter(lambda a: a[0] == "href", attrs))
        yanked_attr = list(filter(lambda a: a[0] == "data-yanked", attrs))
        python_version_attr = list(filter(lambda a: a[0] == "data-requires-python", attrs))

        if len(href_attr) != 1:
            return

        _, href_url = href_attr[0]
        is_yanked = any(yanked_attr)
        _, required_python = python_version_attr[0] if len(python_version_attr) == 1 else (None, None)

        if "#" in href_url:
            sep = href_url.index("#")
            hash_info = href_url[sep + 1:].split("=")
            href_url = href_url[:sep]
        else:
            hash_info = None

        # URL in the HREF attribute can be absolute or relative
        file_url = urllib.parse.urljoin(self._package_url, href_url)

        if hash_info and len(hash_info) == 2:
            hashes = {hash_info[0]: hash_info[1]}
        else:
            hashes = {}

        file_info = {
            "filename": filename,
            "url": file_url,
            "hashes": hashes,
            "yanked": is_yanked
        }

        self.response["files"].append(file_info)


# Register plugin
plugins.PluginManager.register_plugin(IModelRepository, PyPiRepository, ["pypi"])
