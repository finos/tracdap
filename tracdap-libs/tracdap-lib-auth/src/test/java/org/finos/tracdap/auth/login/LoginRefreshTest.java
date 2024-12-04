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
import io.netty.handler.codec.http.HttpHeaderNames;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.HttpCookie;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;


public class LoginRefreshTest {

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
    void testRefreshBrowser() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/refresh";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        // jwtRefresh set to 60 seconds in config, so 2 minutes should trigger refresh

        var issueTime = Instant.now()
                .minus(Duration.of(2, ChronoUnit.MINUTES))
                .truncatedTo(ChronoUnit.SECONDS);

        var user = new UserInfo();
        user.setUserId("test.user");
        user.setDisplayName("Test User");

        var session = new SessionInfo();
        session.setUserInfo(user);
        session.setValid(true);
        session.setIssueTime(issueTime);
        session.setExpiryTime(issueTime.plus(Duration.of(15, ChronoUnit.MINUTES)));
        session.setExpiryLimit(issueTime.plus(Duration.of(1, ChronoUnit.HOURS)));

        var token = jwtProcessor.encodeToken(session);

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();
            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("authorization", "Bearer " + token)
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

            var newToken = HttpCookie.parse(tokenCookie.get()).stream().findFirst();

            Assertions.assertTrue(newToken.isPresent());

            var newSession = jwtProcessor.decodeAndValidate(newToken.get().getValue());

            Assertions.assertTrue(newSession.isValid());
            Assertions.assertEquals(session.getUserInfo().getUserId(), newSession.getUserInfo().getUserId());
            Assertions.assertEquals(session.getUserInfo().getDisplayName(), newSession.getUserInfo().getDisplayName());
            Assertions.assertEquals(session.getExpiryLimit(), newSession.getExpiryLimit());
            Assertions.assertTrue(newSession.getIssueTime().isAfter(session.getIssueTime()));
            Assertions.assertTrue(newSession.getExpiryTime().isAfter(session.getExpiryTime()));

            var content = response.body();

            var returnPath = "/client-app/home";
            var redirectHtml = String.format(REDIRECT_HTML, returnPath);

            Assertions.assertTrue(content.contains(redirectHtml));
        }
    }

    @Test
    void testRefreshApi() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/refresh";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var issueTime = Instant.now()
                .minus(Duration.of(2, ChronoUnit.MINUTES))
                .truncatedTo(ChronoUnit.SECONDS);

        var user = new UserInfo();
        user.setUserId("test.user");
        user.setDisplayName("Test User");

        var session = new SessionInfo();
        session.setUserInfo(user);
        session.setValid(true);
        session.setIssueTime(issueTime);
        session.setExpiryTime(issueTime.plus(Duration.of(15, ChronoUnit.MINUTES)));
        session.setExpiryLimit(issueTime.plus(Duration.of(1, ChronoUnit.HOURS)));

        var token = jwtProcessor.encodeToken(session);

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();
            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("authorization", "Bearer " + token)
                    .header("content-type", "application/trac-login")
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(200, response.statusCode());

            var tokenHeader = response.headers().allValues("trac-auth-token").stream().findFirst();

            Assertions.assertTrue(tokenHeader.isPresent());

            var newSession = jwtProcessor.decodeAndValidate(tokenHeader.get());

            Assertions.assertTrue(newSession.isValid());
            Assertions.assertEquals(session.getUserInfo().getUserId(), newSession.getUserInfo().getUserId());
            Assertions.assertEquals(session.getUserInfo().getDisplayName(), newSession.getUserInfo().getDisplayName());
            Assertions.assertEquals(session.getExpiryLimit(), newSession.getExpiryLimit());
            Assertions.assertTrue(newSession.getIssueTime().isAfter(session.getIssueTime()));
            Assertions.assertTrue(newSession.getExpiryTime().isAfter(session.getExpiryTime()));
        }
    }

    @Test
    void testRefreshApiWantCookies() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/refresh";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var issueTime = Instant.now()
                .minus(Duration.of(2, ChronoUnit.MINUTES))
                .truncatedTo(ChronoUnit.SECONDS);

        var user = new UserInfo();
        user.setUserId("test.user");
        user.setDisplayName("Test User");

        var session = new SessionInfo();
        session.setUserInfo(user);
        session.setValid(true);
        session.setIssueTime(issueTime);
        session.setExpiryTime(issueTime.plus(Duration.of(15, ChronoUnit.MINUTES)));
        session.setExpiryLimit(issueTime.plus(Duration.of(1, ChronoUnit.HOURS)));

        var token = jwtProcessor.encodeToken(session);

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();
            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("authorization", "Bearer " + token)
                    .header("content-type", "application/trac-login")
                    .header("trac-auth-cookies", "true")
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(200, response.statusCode());

            var cookies = response.headers().allValues("set-cookie");
            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var newToken = HttpCookie.parse(tokenCookie.orElseThrow()).stream().findFirst();
            var newSession = jwtProcessor.decodeAndValidate(newToken.orElseThrow().getValue());

            Assertions.assertTrue(newSession.isValid());
            Assertions.assertEquals(session.getUserInfo().getUserId(), newSession.getUserInfo().getUserId());
            Assertions.assertEquals(session.getUserInfo().getDisplayName(), newSession.getUserInfo().getDisplayName());
            Assertions.assertEquals(session.getExpiryLimit(), newSession.getExpiryLimit());
            Assertions.assertTrue(newSession.getIssueTime().isAfter(session.getIssueTime()));
            Assertions.assertTrue(newSession.getExpiryTime().isAfter(session.getExpiryTime()));
        }
    }

    @Test
    void testRefreshNoTokenBrowser() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/refresh";
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
            Assertions.assertEquals(307, response.statusCode());

            var loginRedirect = response.headers().firstValue(HttpHeaderNames.LOCATION.toString());

            Assertions.assertTrue(loginRedirect.isPresent());
            Assertions.assertEquals("/login/browser", loginRedirect.get());
        }
    }

    @Test
    void testRefreshNoTokenApi() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/refresh";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();
            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("content-type", "application/trac-login")
                    .header("accept", "application/trac-login")
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(401, response.statusCode());
        }
    }

    @Test
    void testRefreshTokenExpired() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/refresh";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var issueTime = Instant.now()
                .minus(Duration.of(30, ChronoUnit.MINUTES))
                .truncatedTo(ChronoUnit.SECONDS);

        var user = new UserInfo();
        user.setUserId("test.user");
        user.setDisplayName("Test User");

        var session = new SessionInfo();
        session.setUserInfo(user);
        session.setValid(true);
        session.setIssueTime(issueTime);
        session.setExpiryTime(issueTime.plus(Duration.of(15, ChronoUnit.MINUTES)));
        session.setExpiryLimit(issueTime.plus(Duration.of(1, ChronoUnit.HOURS)));

        var token = jwtProcessor.encodeToken(session);

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();
            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("authorization", "Bearer " + token)
                    .header("content-type", "application/trac-login")
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(401, response.statusCode());
        }
    }

    @Test
    void testRefreshTokenGarbled() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/refresh";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var token = "xxxx_INVALID_TOKEN";

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();
            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("authorization", "Bearer " + token)
                    .header("content-type", "application/trac-login")
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(401, response.statusCode());
        }
    }
}
