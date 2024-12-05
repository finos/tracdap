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

package org.finos.tracdap.svc.auth;

import org.finos.tracdap.common.auth.internal.JwtSetup;
import org.finos.tracdap.common.auth.internal.JwtValidator;
import org.finos.tracdap.test.helpers.CloseWrapper;
import org.finos.tracdap.test.helpers.PlatformTest;
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


public class ProviderSwitchingTest {

    // Test switching between auth providers when multiple are loaded

    public static final String AUTH_SVC_UNIT_CONFIG = "config/auth-svc-unit-test.yaml";
    private static final short AUTH_SVC_PORT = 8081;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(AUTH_SVC_UNIT_CONFIG)
            .runDbDeploy(false)
            .startAuth()
            .build();

    private static JwtValidator jwtValidator;

    @BeforeAll
    public static void setup() {

        jwtValidator = JwtSetup.createValidator(platform.platformConfig(), platform.configManager());
    }

    @Test
    void testLoginThenDummy() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/browser";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var dummyUriTemplate = "http://localhost:%d/dummy/get-token";
        var dummyUri = new URI(String.format(dummyUriTemplate, AUTH_SVC_PORT));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var loginRequest = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var dummyRequest = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(dummyUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var loginResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
            var dummyResponse = client.send(dummyRequest, HttpResponse.BodyHandlers.ofString());

            // Check login response

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, loginResponse.version());
            Assertions.assertEquals(200, loginResponse.statusCode());

            var cookies = loginResponse.headers().allValues("set-cookie");
            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var token = HttpCookie.parse(tokenCookie.orElseThrow()).stream().findFirst();
            var session = jwtValidator.decodeAndValidate(token.orElseThrow().getValue());

            Assertions.assertTrue(session.isValid());
            Assertions.assertTrue(session.getExpiryTime().isAfter(Instant.now()));
            Assertions.assertEquals("test.user", session.getUserInfo().getUserId());

            // Check dummy response

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, dummyResponse.version());
            Assertions.assertEquals(200, dummyResponse.statusCode());

            var dummyToken = dummyResponse.headers().allValues("x-dummy-token").stream().findFirst();

            Assertions.assertTrue(dummyToken.isPresent());
            Assertions.assertEquals("DUMMY_TOKEN", dummyToken.get());
        }
    }

    @Test
    void testDummyThenLogin() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/browser";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var dummyUriTemplate = "http://localhost:%d/dummy/get-token";
        var dummyUri = new URI(String.format(dummyUriTemplate, AUTH_SVC_PORT));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var dummyRequest = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(dummyUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var loginRequest = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var dummyResponse = client.send(dummyRequest, HttpResponse.BodyHandlers.ofString());
            var loginResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());

            // Check dummy response

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, dummyResponse.version());
            Assertions.assertEquals(200, dummyResponse.statusCode());

            var dummyToken = dummyResponse.headers().allValues("x-dummy-token").stream().findFirst();

            Assertions.assertTrue(dummyToken.isPresent());
            Assertions.assertEquals("DUMMY_TOKEN", dummyToken.get());

            // Check login response

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, loginResponse.version());
            Assertions.assertEquals(200, loginResponse.statusCode());

            var cookies = loginResponse.headers().allValues("set-cookie");
            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var token = HttpCookie.parse(tokenCookie.orElseThrow()).stream().findFirst();
            var session = jwtValidator.decodeAndValidate(token.orElseThrow().getValue());

            Assertions.assertTrue(session.isValid());
            Assertions.assertTrue(session.getExpiryTime().isAfter(Instant.now()));
            Assertions.assertEquals("test.user", session.getUserInfo().getUserId());
        }
    }

    @Test
    void testSequenceIncluding404() throws Exception {

        var loginUriTemplate = "http://localhost:%d/login/browser";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT));

        var dummyUriTemplate = "http://localhost:%d/dummy/get-token";
        var dummyUri = new URI(String.format(dummyUriTemplate, AUTH_SVC_PORT));

        var unknownUriTemplate = "http://localhost:%d/unknown/get-token";
        var unknownUri = new URI(String.format(unknownUriTemplate, AUTH_SVC_PORT));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var loginRequest = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var dummyRequest = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(dummyUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var unknownRequest = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(unknownUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var login1 = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
            var unknown1 = client.send(unknownRequest, HttpResponse.BodyHandlers.ofString());
            var dummy1 = client.send(dummyRequest, HttpResponse.BodyHandlers.ofString());

            var login2 = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
            var unknown2 = client.send(unknownRequest, HttpResponse.BodyHandlers.ofString());
            var dummy2 = client.send(dummyRequest, HttpResponse.BodyHandlers.ofString());

            // Check login 1 response

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, login1.version());
            Assertions.assertEquals(200, login1.statusCode());

            var cookies = login1.headers().allValues("set-cookie");
            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var token = HttpCookie.parse(tokenCookie.orElseThrow()).stream().findFirst();
            var session = jwtValidator.decodeAndValidate(token.orElseThrow().getValue());

            Assertions.assertTrue(session.isValid());
            Assertions.assertTrue(session.getExpiryTime().isAfter(Instant.now()));
            Assertions.assertEquals("test.user", session.getUserInfo().getUserId());

            // Check unknown 1 response

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, dummy1.version());
            Assertions.assertEquals(404, unknown1.statusCode());

            // Check dummy 1 response

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, dummy1.version());
            Assertions.assertEquals(200, dummy1.statusCode());

            var dummyToken = dummy1.headers().allValues("x-dummy-token").stream().findFirst();

            Assertions.assertTrue(dummyToken.isPresent());
            Assertions.assertEquals("DUMMY_TOKEN", dummyToken.get());

            // Check second responses

            Assertions.assertEquals(200, login2.statusCode());
            Assertions.assertEquals(404, unknown2.statusCode());
            Assertions.assertEquals(200, dummy2.statusCode());
        }
    }
}
