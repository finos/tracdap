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

import com.accenture.trac.common.config.ConfigBootstrap;

import io.netty.handler.codec.http.*;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.netty.util.NetUtil.LOCALHOST;


public class Http1ProxyTest {

    private static final Logger log = LoggerFactory.getLogger(Http1ProxyTest.class);

    private static final String HTTP1_PROXY_TEST_CONFIG = "etc/trac-devlocal-gw.properties";

    private static final String TEST_FILE_REMOTE_PATH = "/design_principals.md";
    private static final String TEST_FILE_LOCAL_PATH = "doc/design_principals.md";
    private static final short TEST_GW_PORT = 8080;
    private static final long TEST_TIMEOUT = 1000;

    private static Path rootDir;
    private static final int svrPort = 8090;
    private static Http1Server svr;
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

        // Start the gateway

        var config = ConfigBootstrap.useConfigFile(TracPlatformGateway.class, rootDir, HTTP1_PROXY_TEST_CONFIG, "");
        gateway = new TracPlatformGateway(config);
        gateway.start();
    }

    @AfterAll
    public static void tearDownServer() throws Exception {

        if (gateway != null)
            gateway.stop();

        if (svr != null)
            svr.shutdown();
    }

//
//    @Test
//    public void runServer() throws Exception {
//
//        var config = ConfigBootstrap.useConfigFile(
//                TracPlatformGateway.class,
//                HTTP1_PROXY_TEST_CONFIG);
//
//        var gateway = new TracPlatformGateway(config);
//        gateway.start();
//
//        gateway.stop();
//        //var gw = new TracPlatformGateway()
//    }

    @Test
    void http1SimpleProxy_head() throws Exception {

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var request = client.headRequest(TEST_FILE_REMOTE_PATH);
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
        var request = client.getRequest(TEST_FILE_REMOTE_PATH);
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
    void http1SimpleProxy_put() throws Exception {
        Assertions.fail();
    }

    @Test @Disabled
    void http1SimpleProxy_post() throws Exception {
        Assertions.fail();
    }

    @Test @Disabled
    void http1SimpleProxy_redirect() throws Exception {
        Assertions.fail();
    }

    @Test
    void http1SimpleProxy_notFound() throws Exception {

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var request = client.getRequest("/some/bogus/path");
        request.await(TEST_TIMEOUT);

        // Should be a successful response with error code 404

        Assertions.assertTrue(request.isDone());
        Assertions.assertTrue(request.isSuccess());

        var response = request.getNow();

        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, response.status());

        response.release();
    }
//
//    @Test
//    void http1SimpleProxy_serverDown() throws Exception {
//
//        // Shut down the back end server before contacting the gateway
//        svr.shutdown();
//        svr = null;
//
//        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
//        var request = client.getRequest(TEST_FILE_REMOTE_PATH);
//        request.await(TEST_TIMEOUT);
//
//        // Should be a successful response with error code 503, source server is not available
//
//        Assertions.assertTrue(request.isDone());
//        Assertions.assertTrue(request.isSuccess());
//
//        var response = request.getNow();
//
//        Assertions.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.status());
//
//        response.release();
//    }
//
//    @Test
//    void http1SimpleProxy_serverTimeout() throws Exception {
//
//        // TODO: set up timeout on svr
//
//        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
//        var request = client.getRequest(TEST_FILE_REMOTE_PATH);
//        request.await(TEST_TIMEOUT);
//
//        // Should be a successful response with error code 504, source server timed out
//
//        Assertions.assertTrue(request.isDone());
//        Assertions.assertTrue(request.isSuccess());
//
//        var response = request.getNow();
//
//        Assertions.assertEquals(HttpResponseStatus.GATEWAY_TIMEOUT, response.status());
//
//        response.release();
//    }

}
