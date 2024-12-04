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

package org.finos.tracdap.auth.login;

import org.finos.tracdap.common.auth.internal.*;
import org.finos.tracdap.config.AuthenticationConfig;
import org.finos.tracdap.test.helpers.CloseWrapper;
import org.finos.tracdap.test.helpers.PlatformTest;

import io.netty.channel.ChannelHandler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;


public class GuestLoginTest {

    public static final String TRAC_CONFIG_AUTH_UNIT = "config/auth-svc-login-guest.yaml";

    private static final String REDIRECT_HTML = "<meta http-equiv=\"refresh\" content=\"1; URL=%s\" />";
    private static final short AUTH_SVC_PORT = 8765;

    // Use PlatformTest to prepare TRAC config, secrets, auth keys etc
    // Do not start any services, we want to test the login handler in isolation

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_AUTH_UNIT)
            .runDbDeploy(false)
            .build();

    private static JwtProcessor jwtProcessor;
    private static Runnable shutdownFunc;

    @BeforeAll
    static void startNettyWithHandler() throws Exception {

        var pluginManager = platform.pluginManager();
        var configManager = platform.configManager();
        var platformConfig = platform.platformConfig();
        var authConfig = platformConfig.getAuthentication();

        pluginManager.initRegularPlugins();

        jwtProcessor = JwtSetup.createProcessor(platformConfig, configManager);

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

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(200, response.statusCode());

            var cookies = response.headers().allValues("set-cookie");

            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var expiryCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-expiry-utc")).findFirst();
            var userIdCookie = cookies.stream().filter(c -> c.startsWith("trac-user-id")).findFirst();
            var userNameCookie = cookies.stream().filter(c -> c.startsWith("trac-user-name")).findFirst();

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

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(200, response.statusCode());

            var cookies = response.headers().allValues("set-cookie");
            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            Assertions.assertTrue(tokenCookie.isPresent());

            var content = response.body();
            var redirectHtml = String.format(REDIRECT_HTML, returnPath);

            Assertions.assertTrue(content.contains(redirectHtml));
        }
    }

    @Test
    void testLoginExistingToken() throws Exception {

        // Should ignore the token and do a normal login

        var loginUriTemplate = "http://localhost:%d/login/browser";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var user = new UserInfo();
        user.setUserId("test.user");
        user.setDisplayName("Test User");

        var session = new SessionInfo();
        session.setUserInfo(user);
        session.setIssueTime(Instant.now());
        session.setExpiryTime(session.getIssueTime().plusSeconds(60));
        session.setExpiryLimit(session.getIssueTime().plusSeconds(300));

        var token = jwtProcessor.encodeToken(session);

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header(AuthConstants.TRAC_AUTH_TOKEN, token)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(200, response.statusCode());

            var cookies = response.headers().allValues("set-cookie");

            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var expiryCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-expiry-utc")).findFirst();
            var userIdCookie = cookies.stream().filter(c -> c.startsWith("trac-user-id")).findFirst();
            var userNameCookie = cookies.stream().filter(c -> c.startsWith("trac-user-name")).findFirst();

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
    void testLoginTokenExpired() throws Exception {

        // Should ignore the token and do a normal login

        var loginUriTemplate = "http://localhost:%d/login/browser";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var user = new UserInfo();
        user.setUserId("test.user");
        user.setDisplayName("Test User");

        var session = new SessionInfo();
        session.setUserInfo(user);
        session.setIssueTime(Instant.now().minusSeconds(6000));
        session.setExpiryTime(session.getIssueTime().plusSeconds(60));
        session.setExpiryLimit(session.getIssueTime().plusSeconds(300));

        var token = jwtProcessor.encodeToken(session);

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header(AuthConstants.TRAC_AUTH_TOKEN, token)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(200, response.statusCode());

            var cookies = response.headers().allValues("set-cookie");

            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var expiryCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-expiry-utc")).findFirst();
            var userIdCookie = cookies.stream().filter(c -> c.startsWith("trac-user-id")).findFirst();
            var userNameCookie = cookies.stream().filter(c -> c.startsWith("trac-user-name")).findFirst();

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
    void testLoginTokenGarbled() throws Exception {

        // Should ignore the token and do a normal login

        var token = "xxxxxxNOT_A_VALID_TOKENyyyyyy";

        var loginUriTemplate = "http://localhost:%d/login/browser";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header(AuthConstants.TRAC_AUTH_TOKEN, token)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(200, response.statusCode());

            var cookies = response.headers().allValues("set-cookie");

            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var expiryCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-expiry-utc")).findFirst();
            var userIdCookie = cookies.stream().filter(c -> c.startsWith("trac-user-id")).findFirst();
            var userNameCookie = cookies.stream().filter(c -> c.startsWith("trac-user-name")).findFirst();

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
    void testBadPaths() throws Exception {

        var badPaths = List.of(
                "http://localhost:%d/login/browser.bad_extension",
                "http://localhost:%d/login/refresh.bad_extension",
                "http://localhost:%d/login/does-not-exist.html",
                "http://localhost:%d/login/folder/does-not-exist.html",
                "http://localhost:%d/login/$&-%%2F-123n?a=6",
                "http://localhost:%d/not-login-at-all/brwoser",
                "http://localhost:%d/../",
                "http://localhost:%d//dev/null");

        for (var loginUriTemplate : badPaths) {

            var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

            try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

                var client = clientWrap.get();
                var request = java.net.http.HttpRequest.newBuilder().GET()
                        .uri(loginUri)
                        .version(HttpClient.Version.HTTP_1_1)
                        .timeout(Duration.ofSeconds(3))
                        .build();

                var response = client.send(request, HttpResponse.BodyHandlers.ofString());

                Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
                Assertions.assertEquals(404, response.statusCode());
            }
        }
    }
}
