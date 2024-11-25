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

import org.finos.tracdap.test.helpers.PlatformTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;


public class GuestLoginTest {

    public static final String TRAC_CONFIG_AUTH_UNIT = "config/auth-svc-login-guest.yaml";

    private static final short AUTH_SVC_PORT = 8081;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_AUTH_UNIT)
            .runDbDeploy(false)
            .startAuth()
            .build();

    @Test
    void startAuthService() {

        Assertions.assertTrue(true);
    }

    @Test
    void testLoginFromScratch() throws Exception {

        var loginUri = new URI("http://localhost:" + AUTH_SVC_PORT + "/login/browser");

        var request = java.net.http.HttpRequest.newBuilder().GET()
                .uri(loginUri)
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3))
                .build();

        var client = HttpClient.newHttpClient();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(HttpClient.Version.HTTP_1_1, response.version());

        System.out.println(response.headers().toString());
    }

    @Test
    void testLoginRedirect() throws Exception {
        Assertions.fail("Not yet implemented");
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
