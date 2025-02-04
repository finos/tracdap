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

package org.finos.tracdap.gateway.proxy.rest;

import org.finos.tracdap.api.*;
import org.finos.tracdap.gateway.TracPlatformGateway;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.helpers.ResourceHelpers;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;

import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.finos.tracdap.test.meta.SampleMetadata.selectorForTag;


public class RestProxyTest {

    public static final short TEST_GW_PORT = 9100;
    public static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startService(TracMetadataService.class)
            .startService(TracDataService.class)
            .startService(TracPlatformGateway.class)
            .build();

    private static final Path tracRepoDir = ResourceHelpers.findTracProjectRoot();

    private static HttpClient client;

    @BeforeAll
    static void setupClient() {
        client = HttpClient.newHttpClient();
    }

    @Test
    void platformInfo() throws Exception {

        var method = "/trac-meta/api/v1/trac/platform-info";

        var request = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var responseMessage = parseJson(response.body(), PlatformInfoResponse.class);

        Assertions.assertEquals(200, response.statusCode());

        // This is set in trac-unit.yaml
        Assertions.assertEquals("TEST_ENVIRONMENT", responseMessage.getEnvironment());
        Assertions.assertFalse(responseMessage.getProduction());

        System.out.println("Platform info says TRAC version = " + responseMessage.getTracVersion());
    }

    @Test
    void listTenants() throws Exception {

        var method = "/trac-meta/api/v1/trac/list-tenants";

        var request = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        var responseMessage = parseJson(response.body(), ListTenantsResponse.class);

        Assertions.assertEquals(200, response.statusCode());

        // There should be at least one tenant and the ACME_CORP tenant used in the test setup should exist

        var tenants = responseMessage.getTenantsList();
        var acmeTenant = tenants.stream().filter(t -> t.getTenantCode().equals("ACME_CORP")).findFirst();
        Assertions.assertFalse(tenants.isEmpty());
        Assertions.assertTrue(acmeTenant.isPresent());

        System.out.println("List tenants found the testing tenant: " + acmeTenant.get().getDescription());
    }

    @Test
    void createSearchAndGet() throws Exception {

        // Create a new FLOW object

        var createMethod = "/trac-meta/api/v1/ACME_CORP/create-object";
        var createJson = "examples/rest_calls/create_flow.json";
        var createBody = Files.readAllBytes(tracRepoDir.resolve(createJson));

        var createRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(createBody))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + createMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var createResponse = client.send(createRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, createResponse.statusCode());

        var objectId = parseJson(createResponse.body(), TagHeader.class);

        // Search for FLOW objects matching a set of criteria

        var searchMethod = "/trac-meta/api/v1/ACME_CORP/search";
        var searchJson = "examples/rest_calls/search.json";
        var searchBody = Files.readAllBytes(tracRepoDir.resolve(searchJson));

        var searchRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(searchBody))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + searchMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var searchResponse = client.send(searchRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, searchResponse.statusCode());

        var results = parseJson(searchResponse.body(), MetadataSearchResponse.class);
        var expectedResult = results.getSearchResultList().stream()
                .filter(record -> record.getHeader().equals(objectId))
                .findFirst();

        Assertions.assertTrue(results.getSearchResultCount() > 0);
        Assertions.assertTrue(expectedResult.isPresent());

        // Get the FLOW object with its full definition

        var readMethod = "/trac-meta/api/v1/ACME_CORP/read-object";
        var selector = selectorForTag(objectId);

        var readBody = JsonFormat.printer().print(selector).getBytes(StandardCharsets.UTF_8);

        var readRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(readBody))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + readMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var readResponse = client.send(readRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, readResponse.statusCode());

        // Compare the definition after round-trip with the original that was saved

        var tag = parseJson(readResponse.body(), Tag.class);
        var definition = tag.getDefinition();

        var originalRequestBytes = Files.readAllBytes(tracRepoDir.resolve(createJson));
        var originalRequest = new String(originalRequestBytes, StandardCharsets.UTF_8);
        var originalDefinition = parseJson(originalRequest, MetadataWriteRequest.class).getDefinition();

        Assertions.assertEquals(originalDefinition, definition);
    }

    @Test
    void missingRoute() throws Exception {

        var method = "/trac-unknown-service/api/v1/trac/get-something";

        var request = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Request succeeds but will have an HTTP error code
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Expect 404 NOT FOUND
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    void missingTenant() throws Exception {

        var methodTemplate = "/trac-meta/api/v1/UNKNOWN_TENANT/FLOW/%s/versions/latest/tags/latest";
        var method = String.format(methodTemplate, UUID.randomUUID());

        var request = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Request succeeds but will have an HTTP error code
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Expect 404 NOT FOUND
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    void missingObject() throws Exception {

        var methodTemplate = "/trac-meta/api/v1/ACME_CORP/FLOW/%s/versions/1/tags/latest";
        var method = String.format(methodTemplate, UUID.randomUUID());

        var request = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Request succeeds but will have an HTTP error code
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Expect 404 NOT FOUND
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    void validationFailure() throws Exception {

        var method = "/trac-meta/api/v1/ACME_CORP/read-object";

        // Read request missing required field "selector", should fail validation
        var bodyBytes = "{}".getBytes(StandardCharsets.UTF_8);

        var request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Request succeeds but will have an HTTP error code
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Expect 400 BAD REQUEST
        Assertions.assertEquals(400, response.statusCode());
    }

    @Test
    // Sending garbled requests is causing problems in CI
    // TODO: Enable this test in CI
    @DisabledIfEnvironmentVariable(named = "CI", matches = "true")
    void sendGarbledContent() throws Exception {

        var method = "/trac-meta/api/v1/ACME_CORP/create-object";

        var bodyBytes = new byte[1024];
        var random = new Random();
        random.nextBytes(bodyBytes);

        var request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Request succeeds but will have an HTTP error code
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Expect 400 BAD REQUEST
        Assertions.assertEquals(400, response.statusCode());
    }

    @Test
    void sendWrongHeaders() throws Exception {

        var method = "/trac-meta/api/v1/ACME_CORP/create-object";
        var json = "examples/rest_calls/create_flow.json";

        var bodyBytes = Files.readAllBytes(tracRepoDir.resolve(json));

        var request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + method))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/xml")
                .header("accept", "application/xml")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        // Request succeeds but will have an HTTP error code
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Expect 406 NOT ACCEPTABLE
        Assertions.assertEquals(406, response.statusCode());
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T parseJson(String json, Class<T> msgClass) throws Exception {

        var builder = (T.Builder) msgClass.getMethod("newBuilder").invoke(null);
        var jsonParser = JsonFormat.parser();
        jsonParser.merge(json, builder);

        return (T) builder.build();
    }
}
