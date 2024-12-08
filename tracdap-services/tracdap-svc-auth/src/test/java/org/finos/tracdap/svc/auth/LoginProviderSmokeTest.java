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


import org.finos.tracdap.common.auth.JwtSetup;
import org.finos.tracdap.common.auth.JwtValidator;
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


public class LoginProviderSmokeTest {

    // More complete tests for core login providers are included in -lib-auth
    // This smoke test is to make sure login requests go to the login provider

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

            var token = HttpCookie.parse(tokenCookie.get()).stream().findFirst();
            var session = jwtValidator.decodeAndValidate(token.orElseThrow().getValue());

            Assertions.assertTrue(session.isValid());
            Assertions.assertTrue(session.getExpiryTime().isAfter(Instant.now()));
            Assertions.assertEquals("test.user", session.getUserInfo().getUserId());
        }
    }

    @Test
    void testMultipleRequests() throws Exception {

        // Multiple requests on a single connection

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

            var response2 = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response2.version());
            Assertions.assertEquals(200, response2.statusCode());

            var response3 = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response3.version());
            Assertions.assertEquals(200, response3.statusCode());

            var cookies = response3.headers().allValues("set-cookie");
            var tokenCookie = cookies.stream().filter(c -> c.startsWith("trac-auth-token")).findFirst();
            var token = HttpCookie.parse(tokenCookie.orElseThrow()).stream().findFirst();
            var session = jwtValidator.decodeAndValidate(token.orElseThrow().getValue());

            Assertions.assertTrue(session.isValid());
            Assertions.assertTrue(session.getExpiryTime().isAfter(Instant.now()));
            Assertions.assertEquals("test.user", session.getUserInfo().getUserId());
        }
    }
}
