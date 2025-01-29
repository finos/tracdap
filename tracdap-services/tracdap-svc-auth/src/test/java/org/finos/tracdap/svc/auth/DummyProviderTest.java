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

import org.finos.tracdap.test.helpers.CloseWrapper;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class DummyProviderTest {

    // Test communication with a dummy (test) auth provider

    public static final String AUTH_SVC_UNIT_CONFIG = "config/auth-svc-unit-test.yaml";
    private static final short AUTH_SVC_PORT = 8081;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(AUTH_SVC_UNIT_CONFIG)
            .runDbDeploy(false)
            .startService(TracAuthenticationService.class)
            .build();

    @Test
    void testDummySingleCall() throws Exception {

        var loginUriTemplate = "http://localhost:%d/dummy/get-token";
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

            var dummyToken = response.headers().allValues("x-dummy-token").stream().findFirst();

            Assertions.assertTrue(dummyToken.isPresent());
            Assertions.assertEquals("DUMMY_TOKEN", dummyToken.get());
        }
    }

    @Test
    void testDummyMultiCall() throws Exception {

        var loginUriTemplate = "http://localhost:%d/dummy/get-token";
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

            var dummyToken = response3.headers().allValues("x-dummy-token").stream().findFirst();

            Assertions.assertTrue(dummyToken.isPresent());
            Assertions.assertEquals("DUMMY_TOKEN", dummyToken.get());
        }
    }

    @Test
    void testDummyQueryParam() throws Exception {

        var loginUriTemplate = "http://localhost:%d/dummy/get-token?dummy-param=DUMMY_VALUE";
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

            var dummyToken = response.headers().allValues("x-dummy-token").stream().findFirst();
            var dummyParam = response.headers().allValues("x-dummy-param").stream().findFirst();

            Assertions.assertTrue(dummyToken.isPresent());
            Assertions.assertEquals("DUMMY_TOKEN", dummyToken.get());

            Assertions.assertTrue(dummyParam.isPresent());
            Assertions.assertEquals("DUMMY_VALUE", dummyParam.get());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 303, 307, 308, 401, 403, 501, 508})
    void testDummyStatusCode(int statusCode) throws Exception {

        var loginUriTemplate = "http://localhost:%d/dummy/get-token?dummy-status=%d";
        var loginUri = new URI(String.format(loginUriTemplate, AUTH_SVC_PORT, statusCode));

        try (var clientWrap = CloseWrapper.wrap(HttpClient.newHttpClient())) {

            var client = clientWrap.get();

            var request = java.net.http.HttpRequest.newBuilder().GET()
                    .uri(loginUri)
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofSeconds(3))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());
            Assertions.assertEquals(statusCode, response.statusCode());
        }
    }

    @Test
    void testBadPaths() throws Exception {

        var badPaths = List.of(
                "http://localhost:%d/dummy/get-token.bad_extension",
                "http://localhost:%d/dummy/refresh.bad_extension",
                "http://localhost:%d/dummy/does-not-exist.html",
                "http://localhost:%d/dummy/folder/does-not-exist.html",
                "http://localhost:%d/dummy/$&-%%2F-123n?a=6",
                "http://localhost:%d/not-dummy-at-all/brwoser",
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
