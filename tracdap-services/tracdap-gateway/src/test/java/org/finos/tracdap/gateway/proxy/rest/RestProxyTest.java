/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.gateway.proxy.rest;

import org.finos.tracdap.api.*;
import org.finos.tracdap.gateway.proxy.http.Http1Client;
import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;

import org.finos.tracdap.test.helpers.ResourceHelpers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static io.netty.util.NetUtil.LOCALHOST;
import static org.finos.tracdap.test.meta.TestData.TEST_TENANT;
import static org.finos.tracdap.test.meta.TestData.selectorForTag;


public class RestProxyTest {

    public static final short TEST_GW_PORT = 8080;
    public static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
            .addTenant(TEST_TENANT)
            .startMeta()
            .startGateway()
            .build();

    private static final Path tracRepoDir = ResourceHelpers.findTracProjectRoot();

    @Test
    void platformInfo() throws Exception {

        var method = "/trac-meta/api/v1/trac/platform-info";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.getRequest(method, headers);
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());

        var response = call.getNow();
        var responseMessage = parseJson(response.content(), PlatformInfoResponse.class);

        // This is set in trac-unit.yaml
        Assertions.assertEquals("TEST_ENVIRONMENT", responseMessage.getEnvironment());
        Assertions.assertFalse(responseMessage.getProduction());

        System.out.println("Platform info says TRAC version = " + responseMessage.getTracVersion());
    }

    @Test
    void listTenants() throws Exception {

        var method = "/trac-meta/api/v1/trac/list-tenants";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.getRequest(method, headers);
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());

        var response = call.getNow();
        var responseMessage = parseJson(response.content(), ListTenantsResponse.class);

        // There should be at least one tenant and the ACME_CORP tenant used in the test setup should exist

        var tenants = responseMessage.getTenantsList();
        var acmeTenant = tenants.stream().filter(t -> t.getTenantCode().equals("ACME_CORP")).findFirst();
        Assertions.assertTrue(tenants.size() > 0);
        Assertions.assertTrue(acmeTenant.isPresent());

        System.out.println("List tenants found the testing tenant: " + acmeTenant.get().getDescription());
    }

    @Test
    void createSearchAndGet() throws Exception {

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var commonHeaders = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.CONTENT_TYPE, "application/json"),
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        // Create a new FLOW object

        var createMethod = "/trac-meta/api/v1/ACME_CORP/create-object";
        var createJson = "examples/rest_calls/create_flow.json";

        var createBody = Files.readAllBytes(tracRepoDir.resolve(createJson));
        var createCall = client.postRequest(createMethod, commonHeaders, Unpooled.wrappedBuffer(createBody));
        createCall.await(TEST_TIMEOUT);

        var createResponse = createCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, createResponse.status());

        var objectId = parseJson(createResponse.content(), TagHeader.class);

        // Search for FLOW objects matching a set of criteria

        var searchMethod = "/trac-meta/api/v1/ACME_CORP/search";
        var searchJson = "examples/rest_calls/search.json";

        var searchBody = Files.readAllBytes(tracRepoDir.resolve(searchJson));
        var searchCall = client.postRequest(searchMethod, commonHeaders, Unpooled.wrappedBuffer(searchBody));
        searchCall.await(TEST_TIMEOUT);

        var searchResponse = searchCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, searchResponse.status());

        var results = parseJson(searchResponse.content(), MetadataSearchResponse.class);
        var expectedResult = results.getSearchResultList().stream()
                .filter(record -> record.getHeader().equals(objectId))
                .findFirst();

        Assertions.assertTrue(results.getSearchResultCount() > 0);
        Assertions.assertTrue(expectedResult.isPresent());

        // Get the FLOW object with its full definition

        var readMethod = "/trac-meta/api/v1/ACME_CORP/read-object";
        var selector = selectorForTag(objectId);

        var readBody = JsonFormat.printer().print(selector).getBytes(StandardCharsets.UTF_8);
        var readCall = client.postRequest(readMethod, commonHeaders, Unpooled.wrappedBuffer(readBody));
        readCall.await(TEST_TIMEOUT);

        var readResponse = readCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, readResponse.status());

        // Compare the definition after round-trip with the original that was saved

        var tag = parseJson(readResponse.content(), Tag.class);
        var definition = tag.getDefinition();

        var originalRequest = Files.readAllBytes(tracRepoDir.resolve(createJson));
        var original = parseJson(Unpooled.wrappedBuffer(originalRequest), MetadataWriteRequest.class).getDefinition();

        Assertions.assertEquals(original, definition);
    }

    @Test
    void missingRoute() throws Exception {

        var method = "/trac-unknown-service/api/v1/trac/get-something";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.getRequest(method, headers);
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());  // Request succeeds but will have an HTTP error code

        var response = call.getNow();

        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
    }

    @Test
    void missingTenant() throws Exception {

        var methodTemplate = "/trac-meta/api/v1/UNKNOWN_TENANT/FLOW/%s/versions/latest/tags/latest";
        var method = String.format(methodTemplate, UUID.randomUUID());

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.getRequest(method, headers);
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());  // Request succeeds but will have an HTTP error code

        var response = call.getNow();

        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
    }

    @Test
    void missingObject() throws Exception {

        var methodTemplate = "/trac-meta/api/v1/ACME_CORP/FLOW/%s/versions/1/tags/latest";
        var method = String.format(methodTemplate, UUID.randomUUID());

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.getRequest(method, headers);
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());  // Request succeeds but will have an HTTP error code

        var response = call.getNow();

        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
    }

    @Test
    void validationFailure() throws Exception {

        var method = "/trac-meta/api/v1/ACME_CORP/read-object";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.CONTENT_TYPE, "application/json"),
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        // Read request missing required field "selector", should fail validation

        var bodyBytes = "{}".getBytes(StandardCharsets.UTF_8);

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.postRequest(method, headers, Unpooled.wrappedBuffer(bodyBytes));
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());  // Request succeeds but will have an HTTP error code

        var response = call.getNow();

        Assertions.assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
    }

    @Test
    void sendGarbledContent() throws Exception {

        var method = "/trac-meta/api/v1/ACME_CORP/create-object";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.CONTENT_TYPE, "application/json"),
                Map.entry(HttpHeaderNames.ACCEPT, "application/json"));

        var bodyBytes = new byte[1024];
        var random = new Random();
        random.nextBytes(bodyBytes);

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.postRequest(method, headers, Unpooled.wrappedBuffer(bodyBytes));
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());  // Request succeeds but will have an HTTP error code

        var response = call.getNow();

        Assertions.assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
    }

    @Test
    void sendWrongHeaders() throws Exception {

        var method = "/trac-meta/api/v1/ACME_CORP/create-object";
        var json = "examples/rest_calls/create_flow.json";

        var headers = Map.<CharSequence, Object>ofEntries(
                Map.entry(HttpHeaderNames.CONTENT_TYPE, "application/xml"),
                Map.entry(HttpHeaderNames.ACCEPT, "application/xml"));

        var bodyBytes = Files.readAllBytes(tracRepoDir.resolve(json));

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var call = client.postRequest(method, headers, Unpooled.wrappedBuffer(bodyBytes));
        call.await(TEST_TIMEOUT);

        Assertions.assertTrue(call.isDone());
        Assertions.assertTrue(call.isSuccess());  // Request succeeds but will have an HTTP error code

        var response = call.getNow();

        Assertions.assertEquals(HttpResponseStatus.NOT_ACCEPTABLE, response.status());
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T parseJson(ByteBuf buffer, Class<T> msgClass) throws Exception {

        var builder = (T.Builder) msgClass.getMethod("newBuilder").invoke(null);

        try (var jsonStream = new ByteBufInputStream(buffer);
             var jsonReader = new InputStreamReader(jsonStream)) {

            var jsonParser = JsonFormat.parser();
            jsonParser.merge(jsonReader, builder);

            return (T) builder.build();
        }
        finally {
            buffer.release();
        }
    }
}
