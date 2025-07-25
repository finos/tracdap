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
import org.finos.tracdap.common.async.Flows;
import org.finos.tracdap.gateway.TracPlatformGateway;
import org.finos.tracdap.metadata.TagHeader;
import org.finos.tracdap.svc.admin.TracAdminService;
import org.finos.tracdap.svc.data.TracDataService;
import org.finos.tracdap.svc.meta.TracMetadataService;
import org.finos.tracdap.test.data.DataApiTestHelpers;
import org.finos.tracdap.test.data.SampleData;
import org.finos.tracdap.test.helpers.PlatformTest;
import org.finos.tracdap.test.helpers.ResourceHelpers;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.getResultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;
import static org.finos.tracdap.test.meta.SampleMetadata.selectorForTag;


public class RestDataApiTest {

    public static final short TEST_GW_PORT = 9100;
    public static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    public static final String TEST_FILE = "README.md";
    public static final String SMALL_TEST_FILE = "tracdap-libs/tracdap-lib-test/src/main/resources/sample_data/csv_basic.csv";
    public static final String SMALL_TEST_FILE_V2 = "tracdap-libs/tracdap-lib-test/src/main/resources/sample_data/csv_basic_v2.csv";
    public static final String LARGE_TEST_FILE = "tracdap-services/tracdap-svc-data/src/test/resources/large_csv_data_100000.csv";

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_TENANTS_UNIT = "config/trac-unit-tenants.yaml";

    public static final long UPLOAD_CHUNK_SIZE = 2 * 1024 * 1024;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT, List.of(TRAC_TENANTS_UNIT))
            .runDbDeploy(true)
            .addTenant(TEST_TENANT)
            .startService(TracAdminService.class)
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
    void smallDatasetRoundTrip() throws Exception {

        /*
            Test REST API for small datasets with this sequence:
            createSmallDataset()
            readSmallDataset()
            updateSmallDataset()
            readSmallDataset()
         */

        var createMethod = "/trac-data/api/v1/ACME_CORP/create-small-dataset";
        var createDataBytes = Files.readAllBytes(tracRepoDir.resolve(SMALL_TEST_FILE));
        var createDataBase64 = Base64.getEncoder().encodeToString(createDataBytes);
        var createSchema = SampleData.BASIC_TABLE_SCHEMA;
        var createSchemaJson = JsonFormat.printer().print(createSchema);
        var createRequestJson = "{\n" +
                "    \"schema\": " + createSchemaJson + ",\n" +
                "    \"format\": \"text/csv\",\n" +
                "    \"content\": \"" + createDataBase64+ "\"\n" +
                "}";

        var createRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(createRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + createMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var createResponse = client.send(createRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, createResponse.statusCode());

        var dataId = parseJson(createResponse.body(), TagHeader.class);
        var dataSelector = selectorForTag(dataId);
        var dataSelectorJson = JsonFormat.printer().print(dataSelector);

        var readMethod = "/trac-data/api/v1/ACME_CORP/read-small-dataset";
        var readRequestJson = "{ \"selector\": " + dataSelectorJson + ", \"format\": \"text/csv\" }";

        var readRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(readRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + readMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var readResponse = client.send(readRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, readResponse.statusCode());

        var readResponseMessage = parseJson(readResponse.body(), DataReadResponse.class);
        var readResponseSchema = readResponseMessage.getSchema();
        var readResponseContent = readResponseMessage.getContent().toByteArray();

        var nCreatedLines = new String(createDataBytes).split("\n").length;
        var nResponseLines = new String(readResponseContent).split("\n").length;

        Assertions.assertEquals(createSchema, readResponseSchema);
        Assertions.assertEquals(nCreatedLines, nResponseLines);

        var updateMethod = "/trac-data/api/v1/ACME_CORP/update-small-dataset";
        var updateDataBytes = Files.readAllBytes(tracRepoDir.resolve(SMALL_TEST_FILE_V2));
        var updateDataBase64 = Base64.getEncoder().encodeToString(updateDataBytes);
        var updateSchema = SampleData.BASIC_TABLE_SCHEMA_V2;
        var updateSchemaJson = JsonFormat.printer().print(updateSchema);
        var updateRequestJson = "{\n" +
                "    \"schema\": " + updateSchemaJson + ",\n" +
                "    \"priorVersion\": " + dataSelectorJson + ",\n" +
                "    \"format\": \"text/csv\",\n" +
                "    \"content\": \"" + updateDataBase64+ "\"\n" +
                "}";

        var updateRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(updateRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + updateMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var updateResponse = client.send(updateRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, updateResponse.statusCode());

        var updatedId = parseJson(updateResponse.body(), TagHeader.class);
        var updatedSelector = selectorForTag(updatedId);
        var updatedSelectorJson = JsonFormat.printer().print(updatedSelector);

        var updatedReadRequestJson = "{ \"selector\": " + updatedSelectorJson + ", \"format\": \"text/csv\" }";

        var updatedReadRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(updatedReadRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + readMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var updatedReadResponse = client.send(updatedReadRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, updatedReadResponse.statusCode());

        var updatedResponseMessage = parseJson(updatedReadResponse.body(), DataReadResponse.class);
        var updatedResponseSchema = updatedResponseMessage.getSchema();
        var updatedResponseContent = updatedResponseMessage.getContent().toByteArray();

        var nUpdatedLines = new String(updateDataBytes).split("\n").length;
        var nUpdatedResponseLines = new String(updatedResponseContent).split("\n").length;

        Assertions.assertEquals(updateSchema, updatedResponseSchema);
        Assertions.assertEquals(nUpdatedLines, nUpdatedResponseLines);
    }

    @Test
    void smallFileRoundTrip() throws Exception {

        /*
            Test REST API for small files with this sequence:
            createSmallFile()
            readSmallFile()
            updateSmallFile()
            readSmallFile()
         */

        var createMethod = "/trac-data/api/v1/ACME_CORP/create-small-file";
        var createFilePath = "examples/rest_calls/create_flow.json";
        var createFileBytes = Files.readAllBytes(tracRepoDir.resolve(createFilePath));
        var createFileBase64 = Base64.getEncoder().encodeToString(createFileBytes);
        var createRequestJson = "{\n" +
                "    \"name\": \"create_flow.json\",\n" +
                "    \"mimeType\": \"application/json\",\n" +
                "    \"size\": " + createFileBytes.length + ",\n" +
                "    \"content\": \"" + createFileBase64+ "\"\n" +
                "}";

        var createRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(createRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + createMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var createResponse = client.send(createRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, createResponse.statusCode());

        var fileId = parseJson(createResponse.body(), TagHeader.class);
        var fileSelector = selectorForTag(fileId);
        var fileSelectorJson = JsonFormat.printer().print(fileSelector);

        var readMethod = "/trac-data/api/v1/ACME_CORP/read-small-file";
        var readRequestJson = "{ \"selector\": " + fileSelectorJson + " }";

        var readRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(readRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + readMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var readResponse = client.send(readRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, readResponse.statusCode());

        var readResponseMessage = parseJson(readResponse.body(), FileReadResponse.class);
        var readResponseContent = readResponseMessage.getContent().toByteArray();

        Assertions.assertArrayEquals(createFileBytes, readResponseContent);

        var updateMethod = "/trac-data/api/v1/ACME_CORP/update-small-file";
        var updateFilePath = "examples/rest_calls/search.json";
        var updateFileBytes = Files.readAllBytes(tracRepoDir.resolve(updateFilePath));
        var updateFileBase64 = Base64.getEncoder().encodeToString(updateFileBytes);
        var updateRequestJson = "{\n" +
                "    \"name\": \"create_flow.json\",\n" +
                "    \"priorVersion\": " + fileSelectorJson + ",\n" +
                "    \"mimeType\": \"application/json\",\n" +
                "    \"size\": " + updateFileBytes.length + ",\n" +
                "    \"content\": \"" + updateFileBase64+ "\"\n" +
                "}";

        var updateRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(updateRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + updateMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var updateResponse = client.send(updateRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, updateResponse.statusCode());

        var updatedId = parseJson(updateResponse.body(), TagHeader.class);
        var updatedSelector = selectorForTag(updatedId);
        var updatedSelectorJson = JsonFormat.printer().print(updatedSelector);

        var updatedReadRequestJson = "{ \"selector\": " + updatedSelectorJson + " }";

        var updatedReadRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(updatedReadRequestJson))
                .uri(new URI("http://localhost:" + TEST_GW_PORT + readMethod))
                .version(HttpClient.Version.HTTP_1_1)
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var updatedReadResponse = client.send(updatedReadRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, updatedReadResponse.statusCode());

        var updatedReadResponseMessage = parseJson(updatedReadResponse.body(), FileReadResponse.class);
        var updatedReadResponseContent = updatedReadResponseMessage.getContent().toByteArray();

        Assertions.assertArrayEquals(updateFileBytes, updatedReadResponseContent);
    }

    @Test
    void simpleDownload() throws Exception {

        var dataClient = platform.dataClientBlocking();

        var content = Files.readAllBytes(tracRepoDir.resolve(TEST_FILE));

        var upload = FileWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setName("README.md")
                .setMimeType("text/markdown")
                .setContent(ByteString.copyFrom(content))
                .build();

        var fileId = dataClient.createSmallFile(upload);

        var downloadUrl = String.format(
                "/trac-data/api/v1/%s/FILE/%s/versions/%d/README.md",
                TEST_TENANT, fileId.getObjectId(), fileId.getObjectVersion());

        var downloadRequest = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + downloadUrl))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var downloadResponse = client.send(downloadRequest, HttpResponse.BodyHandlers.ofByteArray());
        var contentTypeHeader = downloadResponse.headers().firstValue("content-type");
        var contentLengthHeader = downloadResponse.headers().firstValueAsLong("content-length");
        Assertions.assertEquals(200, downloadResponse.statusCode());
        Assertions.assertTrue(contentTypeHeader.isPresent());
        Assertions.assertTrue(contentLengthHeader.isPresent());
        Assertions.assertEquals("text/markdown", contentTypeHeader.get());
        Assertions.assertEquals(content.length, contentLengthHeader.getAsLong());

        var downloadContent = downloadResponse.body();

        Assertions.assertArrayEquals(content, downloadContent);
    }

    @Test
    void largeFileDownload() throws Exception {

        var dataClient = platform.dataClient();

        var msg0 = FileWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setName("large_csv_data_100000.csv")
                .setMimeType("text/csv")
                .build();

        var content = Files.readAllBytes(tracRepoDir.resolve(LARGE_TEST_FILE));

        var msgs = new ArrayList<FileWriteRequest>();
        msgs.add(msg0);

        for (long offset = 0; offset < content.length; offset += UPLOAD_CHUNK_SIZE) {

            var length = Math.min(UPLOAD_CHUNK_SIZE, content.length - offset);
            var msg = FileWriteRequest.newBuilder()
                    .setContent(ByteString.copyFrom(content, (int) offset, (int) length))
                    .build();
            msgs.add(msg);
        }

        var upload = DataApiTestHelpers.clientStreaming(dataClient::createFile, Flows.publish(msgs));
        waitFor(Duration.ofMillis(TEST_TIMEOUT), upload);

        var fileId = getResultOf(upload);

        var downloadUrl = String.format(
                "/trac-data/api/v1/%s/FILE/%s/versions/%d/large_csv_data_100000.csv",
                TEST_TENANT, fileId.getObjectId(), fileId.getObjectVersion());

        var downloadRequest = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + downloadUrl))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var downloadResponse = client.send(downloadRequest, HttpResponse.BodyHandlers.ofByteArray());
        var contentTypeHeader = downloadResponse.headers().firstValue("content-type");
        var contentLengthHeader = downloadResponse.headers().firstValueAsLong("content-length");
        Assertions.assertEquals(200, downloadResponse.statusCode());
        Assertions.assertTrue(contentTypeHeader.isPresent());
        Assertions.assertTrue(contentLengthHeader.isPresent());
        Assertions.assertEquals("text/csv", contentTypeHeader.get());
        Assertions.assertEquals(content.length, contentLengthHeader.getAsLong());

        var downloadContent = downloadResponse.body();

        Assertions.assertArrayEquals(content, downloadContent);
    }

    @Test
    void latestVersionDownload(@TempDir Path tempDir) throws Exception {

        var dataClient = platform.dataClientBlocking();

        var originalFile = tracRepoDir.resolve(TEST_FILE);
        var testFile = tempDir.resolve(TEST_FILE);
        Files.copy(originalFile, testFile);

        var content = Files.readAllBytes(testFile);

        var upload = FileWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setName("README.md")
                .setMimeType("text/markdown")
                .setContent(ByteString.copyFrom(content))
                .build();

        var fileId = dataClient.createSmallFile(upload);

        var downloadUrl = String.format(
                "/trac-data/api/v1/%s/FILE/%s/versions/latest/README.md",
                TEST_TENANT, fileId.getObjectId());

        var downloadRequest = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + downloadUrl))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var downloadResponse = client.send(downloadRequest, HttpResponse.BodyHandlers.ofByteArray());
        var contentTypeHeader = downloadResponse.headers().firstValue("content-type");
        var contentLengthHeader = downloadResponse.headers().firstValueAsLong("content-length");
        Assertions.assertEquals(200, downloadResponse.statusCode());
        Assertions.assertTrue(contentTypeHeader.isPresent());
        Assertions.assertTrue(contentLengthHeader.isPresent());
        Assertions.assertEquals("text/markdown", contentTypeHeader.get());
        Assertions.assertEquals(content.length, contentLengthHeader.getAsLong());

        var downloadContent = downloadResponse.body();

        Assertions.assertArrayEquals(content, downloadContent);

        try (var writer = new FileWriter(testFile.toFile(), /* append = */ true)) {
            writer.append("Adding something extra to the end of the file");
        }

        var updatedContent = Files.readAllBytes(testFile);

        Assertions.assertTrue(updatedContent.length > content.length);

        var update = FileWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setName("README.md")
                .setMimeType("text/markdown")
                .setPriorVersion(selectorForTag(fileId))
                .setContent(ByteString.copyFrom(updatedContent))
                .build();

        var fileIdV2 = dataClient.updateSmallFile(update);

        Assertions.assertTrue(fileIdV2.getObjectVersion() > fileId.getObjectVersion());

        var downloadRequest2 = HttpRequest.newBuilder()
                .GET()
                .uri(new URI("http://localhost:" + TEST_GW_PORT + downloadUrl))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofMillis(TEST_TIMEOUT))
                .build();

        var downloadResponse2 = client.send(downloadRequest2, HttpResponse.BodyHandlers.ofByteArray());
        var contentTypeHeader2 = downloadResponse2.headers().firstValue("content-type");
        var contentLengthHeader2 = downloadResponse2.headers().firstValueAsLong("content-length");
        Assertions.assertEquals(200, downloadResponse2.statusCode());
        Assertions.assertTrue(contentTypeHeader2.isPresent());
        Assertions.assertTrue(contentLengthHeader2.isPresent());
        Assertions.assertEquals("text/markdown", contentTypeHeader2.get());
        Assertions.assertEquals(updatedContent.length, contentLengthHeader2.getAsLong());

        var downloadContent2 = downloadResponse2.body();

        Assertions.assertArrayEquals(updatedContent, downloadContent2);
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T parseJson(String json, Class<T> msgClass) throws Exception {

        var builder = (T.Builder) msgClass.getMethod("newBuilder").invoke(null);
        var jsonParser = JsonFormat.parser();
        jsonParser.merge(json, builder);

        return (T) builder.build();
    }
}
