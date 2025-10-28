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

import unittest
import http.server
import os
import pathlib
import ssl
import subprocess
import tempfile
import threading
import time

import tracdap.rt._impl.core.config_parser as _cfg  # noqa
import tracdap.rt._impl.core.util as _util  # noqa
import tracdap.rt._impl.core.network as _net  # noqa

import urllib3 as _ul3  # noqa
import httpx as _hx  # noqa

# When a private SSL cert is available for testing
# This requires openssl to be installed and available
PRIVATE_SSL_AVAILABLE = "CI" in os.environ or not _util.is_windows()
CERT_FILE = "unit_test_cert.pem"
KEY_FILE = "unit_test_key.pem"


class PrivateServer:

    def __init__(self, temp_dir, host='localhost', port=0):
        self.cert_file = temp_dir.joinpath(CERT_FILE)
        self.key_file = temp_dir.joinpath(KEY_FILE)
        self.host = host
        self.port = port
        self.server = None
        self.thread = None
        self._stop_event = threading.Event()

    def start(self):

        if self.server is not None:
            raise RuntimeError("Server already running")

        # Create HTTP server
        handler = self._create_handler()
        self.server = http.server.HTTPServer((self.host, self.port), handler)  # noqa

        # Wrap with SSL
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert_file, self.key_file)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)

        # Get the actual port (useful when port=0 for auto-assigned)
        self.port = self.server.socket.getsockname()[1]

        # Start server in background thread
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()

        # Wait for server to start
        time.sleep(0.5)
        print(f"HTTPS server started on https://{self.host}:{self.port}")

    def stop(self):

        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=5)
            self.server = None
            self.thread = None

            print("HTTPS server stopped")

    def _create_handler(self):

        class TestHTTPRequestHandler(http.server.BaseHTTPRequestHandler):

            def do_GET(self):  # noqa
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Hello from test HTTPS server!")

            def log_message(self, format, *args):  # noqa
                # Suppress normal logging for tests
                pass

        return TestHTTPRequestHandler

    @property
    def url(self):
        return f"https://{self.host}:{self.port}"


class NetworkManagerTest(unittest.TestCase):

    PRIVATE_SERVER_HOST = "localhost"
    PRIVATE_SERVER_PORT = 12345

    _temp_dir: tempfile.TemporaryDirectory = None
    _config_mgr: _cfg.ConfigManager = None
    _server: PrivateServer = None

    _default_config: _cfg.RuntimeConfig = None
    _tls_config: _cfg.RuntimeConfig = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._nmgr: _net.NetworkManager = None  # noqa

    @classmethod
    def setUpClass(cls):

        cls._temp_dir = tempfile.TemporaryDirectory()
        cls._config_mgr = _cfg.ConfigManager.for_root_dir(cls._temp_dir.name)

        cls._default_config = _cfg.RuntimeConfig()
        cls._tls_config = _cfg.RuntimeConfig(properties={
            "network.ssl.caCertificates": pathlib.Path(cls._temp_dir.name).joinpath(CERT_FILE).name
        })

        if PRIVATE_SSL_AVAILABLE:
            cls._generate_self_signed_cert()
            cls._server = PrivateServer(pathlib.Path(cls._temp_dir.name), cls.PRIVATE_SERVER_HOST, cls.PRIVATE_SERVER_PORT)
            cls._server.start()

    @classmethod
    def _generate_self_signed_cert(cls):

        temp_dir = pathlib.Path(cls._temp_dir.name)
        cert_file = temp_dir.joinpath(CERT_FILE)
        key_file = temp_dir.joinpath(KEY_FILE)

        subprocess.run([
            "openssl", "req", "-new", "-newkey", "rsa:2048", "-days", "365",
            "-nodes", "-x509", "-out", cert_file, "-keyout", key_file,
            "-subj", f"/C=US/ST=State/L=City/O=Org/CN={cls.PRIVATE_SERVER_HOST}"
        ])

    @classmethod
    def tearDownClass(cls):

        if cls._server is not None:
            cls._server.stop()

        if cls._temp_dir is not None:
            cls._temp_dir.cleanup()

    def setUp(self):
        _net.NetworkManager.initialize(self._config_mgr, self._default_config)
        self._nmgr = _net.NetworkManager.instance()

    def test_http_connection_no_tls(self):

        conn = self._nmgr.create_http_connection("google.com", 80, False)

        try:

            conn.connect()
            conn.request("GET", "/")
            response = conn.getresponse()

            # Expect a redirect
            self.assertTrue(300 <= response.status < 400)
            self.assertTrue("location" in response.headers)

        finally:
            conn.close()

    def test_http_connection_public_tls(self):

        conn = self._nmgr.create_http_connection("google.com", 443, True)

        try:

            conn.connect()
            conn.request("GET", "/")
            response = conn.getresponse()

            # Expect a redirect
            self.assertTrue(300 <= response.status < 400)
            self.assertTrue("location" in response.headers)

        finally:
            conn.close()

    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_http_connection_private_tls(self):

        _net.NetworkManager.initialize(self._config_mgr, self._tls_config)

        conn = self._nmgr.create_http_connection(self.PRIVATE_SERVER_HOST, self.PRIVATE_SERVER_PORT, True)

        try:

            conn.connect()
            conn.request("GET", "/")
            response = conn.getresponse()
            data = response.read()

            self.assertEqual(200, response.status)
            self.assertEqual(b"Hello from test HTTPS server!", data)

        finally:
            conn.close()

    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_http_connection_private_tls_no_config(self):

        conn = self._nmgr.create_http_connection(self.PRIVATE_SERVER_HOST, self.PRIVATE_SERVER_PORT, True)

        try:

            self.assertRaises(ssl.SSLCertVerificationError, lambda: conn.connect())

        finally:
            conn.close()

    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_http_connection_private_tls_custom_config(self):

        config = _cfg.PluginConfig()
        config.properties["network.profile"] = "custom"
        config.properties["network.ssl.caCertificates"] = str(pathlib.Path(self._temp_dir.name).joinpath(CERT_FILE))

        conn = self._nmgr.create_http_connection(self.PRIVATE_SERVER_HOST, self.PRIVATE_SERVER_PORT, True, config)

        try:

            conn.connect()
            conn.request("GET", "/")
            response = conn.getresponse()
            data = response.read()

            self.assertEqual(200, response.status)
            self.assertEqual(b"Hello from test HTTPS server!", data)

        finally:
            conn.close()

    def test_urllib3_no_tls(self):

        pool = self._nmgr.create_urllib3_connection_pool("www.google.com", 80, False)
        pool_manager = self._nmgr.create_urllib3_pool_manager()

        try:

            pool_response = pool.request("GET", "/")
            self.assertEqual(200, pool_response.status)
            self.assertTrue("content-type" in pool_response.headers)

            mgr_response = pool_manager.request("GET", "http://www.google.com")  # noqa
            self.assertEqual(200, mgr_response.status)
            self.assertTrue("content-type" in mgr_response.headers)

        finally:
            pool.close()
            pool_manager.clear()

    def test_urllib3_public_tls(self):

        pool = self._nmgr.create_urllib3_connection_pool("www.google.com", 443, True)
        pool_manager = self._nmgr.create_urllib3_pool_manager()

        try:

            pool_response = pool.request("GET", "/")
            self.assertEqual(200, pool_response.status)
            self.assertTrue("content-type" in pool_response.headers)

            mgr_response = pool_manager.request("GET", "https://www.google.com")
            self.assertEqual(200, mgr_response.status)
            self.assertTrue("content-type" in mgr_response.headers)

        finally:
            pool.close()
            pool_manager.clear()


    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_urllib3_private_tls(self):

        _net.NetworkManager.initialize(self._config_mgr, self._tls_config)

        pool = self._nmgr.create_urllib3_connection_pool(self.PRIVATE_SERVER_HOST, self.PRIVATE_SERVER_PORT, True)
        pool_manager = self._nmgr.create_urllib3_pool_manager()

        try:

            pool_response = pool.request("GET", "/")
            self.assertEqual(200, pool_response.status)
            self.assertEqual(b"Hello from test HTTPS server!", pool_response.data)

            mgr_response = pool_manager.request("GET", f"https://{self.PRIVATE_SERVER_HOST}:{self.PRIVATE_SERVER_PORT}")
            self.assertEqual(200, mgr_response.status)
            self.assertEqual(b"Hello from test HTTPS server!", mgr_response.data)

        finally:
            pool.close()
            pool_manager.clear()


    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_urllib3_private_tls_no_config(self):

        pool = self._nmgr.create_urllib3_connection_pool(self.PRIVATE_SERVER_HOST, self.PRIVATE_SERVER_PORT, True)
        pool_manager = self._nmgr.create_urllib3_pool_manager()

        try:

            self.assertRaises(_ul3.exceptions.MaxRetryError, lambda: pool.request("GET", "/"))
            self.assertRaises(_ul3.exceptions.MaxRetryError, lambda:
                pool.request("GET", f"https://{self.PRIVATE_SERVER_HOST}:{self.PRIVATE_SERVER_PORT}"))

        finally:
            pool.close()
            pool_manager.clear()

    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_urllib3_private_tls_custom_config(self):

        config = _cfg.PluginConfig()
        config.properties["network.profile"] = "custom"
        config.properties["network.ssl.caCertificates"] = str(pathlib.Path(self._temp_dir.name).joinpath(CERT_FILE))

        pool = self._nmgr.create_urllib3_connection_pool(self.PRIVATE_SERVER_HOST, self.PRIVATE_SERVER_PORT, True, config)
        pool_manager = self._nmgr.create_urllib3_pool_manager(config)

        try:

            pool_response = pool.request("GET", "/")
            self.assertEqual(200, pool_response.status)
            self.assertEqual(b"Hello from test HTTPS server!", pool_response.data)

            mgr_response = pool_manager.request("GET", f"https://{self.PRIVATE_SERVER_HOST}:{self.PRIVATE_SERVER_PORT}")
            self.assertEqual(200, mgr_response.status)
            self.assertEqual(b"Hello from test HTTPS server!", mgr_response.data)

        finally:
            pool.close()
            pool_manager.clear()

    def test_httx_client_no_tls(self):

        client = self._nmgr.create_httpx_client(follow_redirects=True)

        try:

            response = client.request("GET", "http://google.com/")  # noqa
            self.assertEqual(200, response.status_code)
            self.assertTrue("content-type" in response.headers)

            # Turn off follow redirects - expect a redirect response
            response = client.request("GET", "http://google.com/", follow_redirects=False)  # noqa
            self.assertTrue(300 <= response.status_code < 400)
            self.assertTrue("location" in response.headers)

        finally:
            client.close()

    def test_httx_client_public_tls(self):

        client = self._nmgr.create_httpx_client(follow_redirects=True)

        try:

            response = client.request("GET", "https://google.com/")
            self.assertEqual(200, response.status_code)
            self.assertTrue("content-type" in response.headers)

            # Turn off follow redirects - expect a redirect response
            response = client.request("GET", "https://google.com/", follow_redirects=False)
            self.assertTrue(300 <= response.status_code < 400)
            self.assertTrue("location" in response.headers)

        finally:
            client.close()

    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_httx_client_private_tls(self):

        _net.NetworkManager.initialize(self._config_mgr, self._tls_config)

        client = self._nmgr.create_httpx_client()

        try:

            response = client.request("GET", f"https://{self.PRIVATE_SERVER_HOST}:{self.PRIVATE_SERVER_PORT}")
            self.assertEqual(200, response.status_code)
            self.assertEqual(b"Hello from test HTTPS server!", response.read())

        finally:
            client.close()

    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_httx_client_private_tls_no_config(self):

        client = self._nmgr.create_httpx_client()

        try:

            self.assertRaises(_hx.ConnectError, lambda:
                client.request("GET", f"https://{self.PRIVATE_SERVER_HOST}:{self.PRIVATE_SERVER_PORT}"))

        finally:
            client.close()

    @unittest.skipIf(not PRIVATE_SSL_AVAILABLE, "Private SSL keys are not available (requires openssl)")
    def test_httx_client_private_tls_custom_config(self):

        config = _cfg.PluginConfig()
        config.properties["network.profile"] = "custom"
        config.properties["network.ssl.caCertificates"] = str(pathlib.Path(self._temp_dir.name).joinpath(CERT_FILE))

        client = self._nmgr.create_httpx_client(config)

        try:

            response = client.request("GET", f"https://{self.PRIVATE_SERVER_HOST}:{self.PRIVATE_SERVER_PORT}")
            self.assertEqual(200, response.status_code)
            self.assertEqual(b"Hello from test HTTPS server!", response.read())

        finally:
            client.close()


if __name__ == '__main__':
    unittest.main()
