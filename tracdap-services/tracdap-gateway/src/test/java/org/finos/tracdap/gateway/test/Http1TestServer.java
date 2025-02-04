/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.gateway.test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public class Http1TestServer {

    // Use the JDK HttpServer in com.sun.net as the test server for HTTP proxy testing
    // This provides a standard implementation, rather than relying on another implementation in Netty
    // It also avoids any duplication of assumptions between the gateway and the test server

    private static final long TIMEOUT_DURATION = 5 * 1000;  // Wait 1 minute for timeout tests

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int port;
    private final Path contentRoot;
    private final boolean simulateTimeout;

    private HttpServer jdkServer;

    public Http1TestServer(int port, Path contentRoot) {
        this(port, contentRoot, false);
    }

    public Http1TestServer(int port, Path contentRoot, boolean simulateTimeout) {
        this.port = port;
        this.contentRoot = contentRoot;
        this.simulateTimeout = simulateTimeout;
    }

    public void start() throws Exception {

        // Create an HTTP server
        jdkServer = HttpServer.create(new InetSocketAddress(port), 0);

        if (simulateTimeout)
            // Handler that will time out the request
            jdkServer.createContext("/", new TimeoutHandler());
        else
            // Add a context to serve files
            jdkServer.createContext("/", new FileHandler(contentRoot));

        jdkServer.setExecutor(null); // Use a default executor
        jdkServer.start();

        log.info("Server is running on http://localhost:{} and serving files from {}", port, contentRoot);
    }

    public void stop() {

        jdkServer.stop(0);
    }

    static class FileHandler implements HttpHandler {

        private final Path contentRoot;

        public FileHandler(Path contentRoot) {

            this.contentRoot = contentRoot.toAbsolutePath();

            if (!Files.isDirectory(this.contentRoot)) {
                throw new IllegalArgumentException("Root directory does not exist or is not a directory: " + contentRoot);
            }
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {

            var requestPath = exchange.getRequestURI().getPath();
            var requestedFile = contentRoot.resolve("." + requestPath).normalize(); // Resolve file path safely

            // Check if the requested file is within the root directory
            if (!requestedFile.startsWith(contentRoot)) {
                send404(exchange);
                return;
            }

            // Check if the file exists and is readable
            if (!Files.exists(requestedFile) || !Files.isReadable(requestedFile)) {
                send404(exchange);
                return;
            }

            // Do not send content or content length for HEAD requests
            if (exchange.getRequestMethod().equalsIgnoreCase("HEAD")) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }

            // Regular request - Serve the file

            var fileBytes = Files.readAllBytes(requestedFile);
            exchange.sendResponseHeaders(200, fileBytes.length);

            try (var os = exchange.getResponseBody()) {
                os.write(fileBytes);
            }
        }

        // Helper method to send a 404 response
        private void send404(HttpExchange exchange) throws IOException {

            var response = "NOT FOUND";
            exchange.sendResponseHeaders(404, response.getBytes().length);

            try (var os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    static class TimeoutHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) {

            try {
                Thread.sleep(TIMEOUT_DURATION);
                exchange.close();
            }
            catch (InterruptedException e) {
                exchange.close();
            }
        }
    }
}
