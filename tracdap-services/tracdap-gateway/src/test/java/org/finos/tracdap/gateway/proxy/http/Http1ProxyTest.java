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

package org.finos.tracdap.gateway.proxy.http;

import org.finos.tracdap.common.startup.StandardArgs;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.gateway.TracPlatformGateway;
import org.finos.tracdap.test.helpers.PlatformTestHelpers;
import org.finos.tracdap.gateway.test.Http1TestServer;
import org.finos.tracdap.tools.secrets.SecretTool;

import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class Http1ProxyTest {

    private static final String HTTP1_PROXY_TEST_CONFIG = "/trac-unit-gateway-http1.yaml";

    private static final String TEST_URL_SAMPLE_DOC = "/static/docs/index.rst";
    private static final String TEST_URL_MISSING_DOC = "/static/docs/does_not_exist.md";
    private static final String TEST_URL_SERVER_DOWN = "/static/server_down/foo.md";
    private static final String TEST_URL_SERVER_TIMEOUT = "/static/server_timeout/bar.md";

    private static final String TEST_FILE_LOCAL_PATH = "doc/index.rst";
    private static final short TEST_GW_PORT = 8080;
    private static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    private static Path rootDir;
    private static final int svrPort = 8090;
    private static final int timeoutSvrPort = 8091;
    private static Http1TestServer svr;
    private static Http1TestServer timeoutSvr;
    private static TracPlatformGateway gateway;

    private static HttpClient client;

    @BeforeAll
    public static void setupServer() throws Exception {

        // Set up auth keys

        var secretKey = "very-secret";

        var configFile = Http1ProxyTest.class.getResource(HTTP1_PROXY_TEST_CONFIG);
        Assertions.assertNotNull(configFile);

        var authTasks = new ArrayList<StandardArgs.Task>();
        authTasks.add(StandardArgs.task(SecretTool.CREATE_ROOT_AUTH_KEY, List.of("EC", "256"), ""));
        PlatformTestHelpers.runSecretTool(rootDir, configFile, secretKey, authTasks);


        // Gradle sometimes runs tests out of the sub-project folder instead of the root
        // Find the top level root dir, we need it as a base for content, config files etc.

        var cwd = Paths.get(".").toAbsolutePath().normalize();
        rootDir = cwd.endsWith("tracdap-gateway")
                ? Paths.get("../..").toAbsolutePath().normalize()
                : cwd;

        var svrContentDir = rootDir.resolve("doc");
        var testFile = rootDir.resolve(TEST_FILE_LOCAL_PATH);

        Assertions.assertTrue(
                Files.exists(testFile),
                "HTTP/1 proxy test must be run from the source root folder");

        // Start up our test server to use as a target

        svr = new Http1TestServer(svrPort, svrContentDir);
        svr.start();

        timeoutSvr = new Http1TestServer(timeoutSvrPort, svrContentDir, true);
        timeoutSvr.start();

        // Start the gateway

        var configPath = rootDir
                .relativize(Paths.get(configFile.toURI()).toAbsolutePath())
                .toString()
                .replace("\\", "/");

        var startup = Startup.useConfigFile(TracPlatformGateway.class, rootDir, configPath, secretKey);
        startup.runStartupSequence();

        var plugins = startup.getPlugins();
        var config = startup.getConfig();

        gateway = new TracPlatformGateway(plugins, config);
        gateway.start();
    }

    @BeforeAll
    static void setupClient() {
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @AfterAll
    public static void tearDownServer() {

        if (gateway != null)
            gateway.stop();

        if (svr != null)
            svr.stop();

        if (timeoutSvr != null)
            timeoutSvr.stop();
    }

    @Test
    void http1SimpleProxy_head() throws Exception {

        var request = java.net.http.HttpRequest.newBuilder()
                .method("HEAD", java.net.http.HttpRequest.BodyPublishers.noBody())
                .uri(new URI("http://localhost:" + TEST_GW_PORT + TEST_URL_SAMPLE_DOC))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
    }

    @Test
    void http1SimpleProxy_get() throws Exception {

        var request = java.net.http.HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + TEST_URL_SAMPLE_DOC))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var contentLength = response.headers().firstValueAsLong("content-length").orElseThrow();
        var content = response.body();

        var fsPath = rootDir.resolve(TEST_FILE_LOCAL_PATH);
        var fsLength = Files.size(fsPath);
        var fsContent = Files.readString(fsPath, StandardCharsets.UTF_8);

        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(fsLength, contentLength);
        Assertions.assertEquals(fsContent, content);
    }

    @Test @Disabled
    void http1SimpleProxy_put() {
        Assertions.fail();
    }

    @Test @Disabled
    void http1SimpleProxy_post() {
        Assertions.fail();
    }

    @Test @Disabled
    void http1SimpleProxy_redirect() {
        Assertions.fail();
    }

    @Test
    void http1SimpleProxy_notFound() throws Exception {

        var request = java.net.http.HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + TEST_URL_MISSING_DOC))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Should be a successful response with error code 404
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    void http1SimpleProxy_serverDown() throws Exception {

        var request = java.net.http.HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + TEST_URL_SERVER_DOWN))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Should be a successful response with error code 503, source server is not available
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(503, response.statusCode());
    }

    @Test @Disabled
    void http1SimpleProxy_serverTimeout() throws Exception {

        // Timeout handling not implemented yet in GW

        var request = java.net.http.HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + TEST_URL_SERVER_TIMEOUT))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Should be a successful response with error code 504, source server timed out
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(504, response.statusCode());
    }

    @Test
    void http1SimpleProxy_gatewayRedirect() throws Exception {

        var request = java.net.http.HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + "/"))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Expect the gateway to respond with a redirect

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var location = response.headers().firstValue("location").orElseThrow();

        Assertions.assertEquals(302, response.statusCode());
        Assertions.assertEquals("/static/docs/", location);
    }

}
