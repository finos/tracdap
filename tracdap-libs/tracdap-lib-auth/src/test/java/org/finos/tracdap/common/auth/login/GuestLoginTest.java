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

package org.finos.tracdap.common.auth.login;

import org.finos.tracdap.common.auth.internal.JwtProcessor;
import org.finos.tracdap.common.auth.internal.JwtSetup;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.test.helpers.PlatformTest;

import io.netty.channel.ChannelHandler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;


public class GuestLoginTest {

    // HttpClient is a closable resource in Java 21 but not in Java 17
    private static class CloseWrapper<T> implements AutoCloseable {

        static <T> CloseWrapper<T> wrap(T obj) {
            return new CloseWrapper<>(obj);
        }

        private CloseWrapper(T obj) {
            this.obj = obj;
        }

        public T get() {
            return obj;
        }

        @Override
        public void close() throws Exception {
            if (obj instanceof AutoCloseable)
                ((AutoCloseable) obj).close();
        }

        private final T obj;
    }

    public static final String TRAC_CONFIG_AUTH_UNIT = "config/auth-svc-login-guest.yaml";

    private static final String REDIRECT_HTML = "<meta http-equiv=\"refresh\" content=\"1; URL=%s\" />";
    private static final short AUTH_SVC_PORT = 8765;

    // Use PlatformTest to prepare TRAC config, secrets, auth keys etc
    // Do not start any services, we want to test the login handler in isolation

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_AUTH_UNIT)
            .runDbDeploy(false)
            .build();

    private static Runnable shutdownFunc;

    @BeforeAll
    static void startNettyWithHandler() throws Exception {

        LoggerFactory.getLogger(GuestLoginTest.class).info("Setting up handler");

        var pluginManager = platform.pluginManager();
        var configManager = platform.configManager();
        var platformConfig = platform.platformConfig();

        pluginManager.initRegularPlugins();

        var authConfig = platformConfig.getAuthentication();
        var jwtProcessor = JwtSetup.createProcessor(platformConfig, configManager);

        shutdownFunc = LoginTestHelpers.setupNettyHttp1(
                () -> createHandler(authConfig, jwtProcessor),
                AUTH_SVC_PORT);
    }

    private static ChannelHandler createHandler(
            AuthenticationConfig authConfig,
            JwtProcessor jwtProcessor) {

        var pluginManager = platform.pluginManager();
        var configManager = platform.configManager();

        var providerConfig = authConfig.getProvider();
        var provider = pluginManager.createService(ILoginProvider.class, providerConfig, configManager);

        return new Http1LoginHandler(authConfig, jwtProcessor, provider);
    }

    @AfterAll
    static void stopNettyWithHandler() {

        if (shutdownFunc != null) {
            shutdownFunc.run();
            shutdownFunc = null;
        }
    }

    @Test
    void testLoginFromScratch() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/browser";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());

            var cookies = response.headers().allValues("set-cookie");

            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac_auth_token")).findFirst();
            var expiryCookie = cookies.stream().filter(c -> c.startsWith("trac_auth_expiry")).findFirst();
            var userIdCookie = cookies.stream().filter(c -> c.startsWith("trac_user_id")).findFirst();
            var userNameCookie = cookies.stream().filter(c -> c.startsWith("trac_user_name")).findFirst();

            Assertions.assertTrue(tokenCookie.isPresent());
            Assertions.assertTrue(expiryCookie.isPresent());
            Assertions.assertTrue(userIdCookie.isPresent());
            Assertions.assertTrue(userNameCookie.isPresent());

            var content = response.body();

            var returnPath = "/client-app/home";
            var redirectHtml = String.format(REDIRECT_HTML, returnPath);

            Assertions.assertTrue(content.contains(redirectHtml));
        }
    }

    @Test
    void testLoginRedirect() throws Exception {

        var returnPath = "/custom/redirect";
        var loginUriTemplate = "http://localhost:%d/login/browser?return-path=%s";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT, returnPath));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());

            var cookies = response.headers().allValues("set-cookie");
            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac_auth_token")).findFirst();
            Assertions.assertTrue(tokenCookie.isPresent());

            var content = response.body();
            var redirectHtml = String.format(REDIRECT_HTML, returnPath);

            Assertions.assertTrue(content.contains(redirectHtml));
        }
    }

    @Test
    void testValidToken() throws Exception {
        Assertions.fail("Not yet implemented");
    }

    @Test
    void testValidTokenMissingFields() throws Exception {
        Assertions.fail("Not yet implemented");
    }

    @Test
    void testTokenRefresh() throws Exception {
        Assertions.fail("Not yet implemented");
    }

    @Test
    void testTokenExpired() throws Exception {
        Assertions.fail("Not yet implemented");
    }

    @Test
    void testTokenGarbled() throws Exception {
        Assertions.fail("Not yet implemented");
    }

    @Test
    void testBadPaths() throws Exception {
        Assertions.fail("Not yet implemented");
    }
}
