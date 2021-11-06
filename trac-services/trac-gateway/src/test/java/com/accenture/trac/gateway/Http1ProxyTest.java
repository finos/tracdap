/*
 * Copyright 2021 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.accenture.trac.gateway;

import com.accenture.trac.common.startup.Startup;

import io.netty.handler.codec.http.*;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.netty.util.NetUtil.LOCALHOST;


public class Http1ProxyTest {

    private static final String HTTP1_PROXY_TEST_CONFIG = "/trac-unit-gateway-http1.yaml";

    private static final String TEST_URL_SAMPLE_DOC = "/static/docs/design_principals.md";
    private static final String TEST_URL_MISSING_DOC = "/static/docs/does_not_exist.md";
    private static final String TEST_URL_SERVER_DOWN = "/static/server_down/foo.md";
    private static final String TEST_URL_SERVER_TIMEOUT = "/static/server_timeout/bar.md";

    private static final String TEST_FILE_LOCAL_PATH = "doc/design_principals.md";
    private static final short TEST_GW_PORT = 8080;
    private static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    private static Path rootDir;
    private static final int svrPort = 8090;
    private static final int timeoutSvrPort = 8091;
    private static Http1Server svr;
    private static Http1Server timeoutSvr;
    private static TracPlatformGateway gateway;

    @BeforeAll
    public static void setupServer() throws Exception {

        // Gradle sometimes runs tests out of the sub-project folder instead of the root
        // Find the top level root dir, we need it as a base for content, config files etc.

        var cwd = Paths.get(".").toAbsolutePath().normalize();
        rootDir = cwd.endsWith("trac-gateway")
                ? Paths.get("../..").toAbsolutePath().normalize()
                : cwd;

        var svrContentDir = rootDir.resolve("doc");
        var testFile = rootDir.resolve(TEST_FILE_LOCAL_PATH);

        Assertions.assertTrue(
                Files.exists(testFile),
                "HTTP/1 proxy test must be run from the source root folder");

        // Start up our test server to use as a target

        svr = new Http1Server(svrPort, svrContentDir);
        svr.run();

        timeoutSvr = new Http1Server(timeoutSvrPort, svrContentDir, true);
        timeoutSvr.run();

        // Start the gateway

        var configFile = Http1ProxyTest.class.getResource(HTTP1_PROXY_TEST_CONFIG);
        Assertions.assertNotNull(configFile);

        var configPath = rootDir
                .relativize(Paths.get(configFile.toURI()).toAbsolutePath())
                .toString()
                .replace("\\", "/");

        var config = Startup.useConfigFile(TracPlatformGateway.class, rootDir, configPath, "");

        gateway = new TracPlatformGateway(config);
        gateway.start();
    }

    @AfterAll
    public static void tearDownServer() {

        if (gateway != null)
            gateway.stop();

        if (svr != null)
            svr.shutdown();

        if (timeoutSvr != null)
            timeoutSvr.shutdown();
    }

    @Test
    void http1SimpleProxy_head() throws Exception {

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var request = client.headRequest(TEST_URL_SAMPLE_DOC);
        request.await(TEST_TIMEOUT);

        Assertions.assertTrue(request.isDone());
        Assertions.assertTrue(request.isSuccess());

        var response = request.getNow();
        var contentLength = response.headers().getInt(HttpHeaderNames.CONTENT_LENGTH);

        var fsPath = rootDir.resolve(TEST_FILE_LOCAL_PATH);
        var fsLength = Files.size(fsPath);

        Assertions.assertEquals(HttpResponseStatus.OK, response.status());
        Assertions.assertEquals(fsLength, (long) contentLength);
    }

    @Test
    void http1SimpleProxy_get() throws Exception {

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var request = client.getRequest(TEST_URL_SAMPLE_DOC);
        request.await(TEST_TIMEOUT);

        Assertions.assertTrue(request.isDone());
        Assertions.assertTrue(request.isSuccess());

        var response = request.getNow();
        var contentLength = response.headers().getInt(HttpHeaderNames.CONTENT_LENGTH);
        var content = response.content().readCharSequence(contentLength, StandardCharsets.UTF_8);

        var fsPath = rootDir.resolve(TEST_FILE_LOCAL_PATH);
        var fsLength = Files.size(fsPath);
        var fsContent = Files.readString(fsPath, StandardCharsets.UTF_8);

        Assertions.assertEquals(HttpResponseStatus.OK, response.status());
        Assertions.assertEquals(fsLength, (long) contentLength);
        Assertions.assertEquals(fsContent, content);

        response.release();
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

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var request = client.getRequest(TEST_URL_MISSING_DOC);
        request.await(TEST_TIMEOUT);

        // Should be a successful response with error code 404

        Assertions.assertTrue(request.isDone());
        Assertions.assertTrue(request.isSuccess());

        var response = request.getNow();

        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, response.status());

        response.release();
    }

    @Test
    void http1SimpleProxy_serverDown() throws Exception {

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var request = client.getRequest(TEST_URL_SERVER_DOWN);
        request.await(TEST_TIMEOUT);

        // Should be a successful response with error code 503, source server is not available

        Assertions.assertTrue(request.isDone());
        Assertions.assertTrue(request.isSuccess());

        var response = request.getNow();

        Assertions.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.status());
    }

    @Test @Disabled
    void http1SimpleProxy_serverTimeout() throws Exception {

        // Timeout handling not implemented yet in GW

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var request = client.getRequest(TEST_URL_SERVER_TIMEOUT);
        request.await(TEST_TIMEOUT);

        // Should be a successful response with error code 504, source server timed out

        Assertions.assertTrue(request.isDone());
        Assertions.assertTrue(request.isSuccess());

        var response = request.getNow();

        Assertions.assertEquals(HttpResponseStatus.GATEWAY_TIMEOUT, response.status());
    }

}
