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

import io.netty.handler.codec.http.HttpHeaderNames;
import org.finos.tracdap.api.*;
import org.finos.tracdap.common.async.Flows;
import org.finos.tracdap.gateway.proxy.http.Http1Client;
import org.finos.tracdap.test.data.DataApiTestHelpers;
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;

import org.finos.tracdap.test.helpers.ResourceHelpers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

import static io.netty.util.NetUtil.LOCALHOST;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.getResultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.finos.tracdap.test.meta.TestData.TEST_TENANT;
import static org.finos.tracdap.test.meta.TestData.selectorForTag;


public class RestDownloadTest {

    public static final short TEST_GW_PORT = 8080;
    public static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    public static final String TEST_FILE = "README.md";
    public static final String LARGE_TEST_FILE = "tracdap-services/tracdap-svc-data/src/test/resources/large_csv_data_100000.csv";

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";

    public static final long UPLOAD_CHUNK_SIZE = 2 * 1024 * 1024;

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT)
            .addTenant(TEST_TENANT)
            .startMeta()
            .startData()
            .startGateway()
            .build();

    private static final Path tracRepoDir = ResourceHelpers.findTracProjectRoot();

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

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var commonHeaders = Map.<CharSequence, Object>ofEntries();

        var downloadUrl = String.format(
                "/trac-data/api/v1/%s/FILE/%s/versions/%d/README.md",
                TEST_TENANT, fileId.getObjectId(), fileId.getObjectVersion());


        var downloadCall = client.getRequest(downloadUrl, commonHeaders);
        downloadCall.await(TEST_TIMEOUT);

        Assertions.assertTrue(downloadCall.isDone());
        if (!downloadCall.isSuccess())
            Assertions.fail(downloadCall.cause());

        var downloadResponse = downloadCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, downloadResponse.status());
        Assertions.assertEquals("text/markdown", downloadResponse.headers().get(HttpHeaderNames.CONTENT_TYPE));
        Assertions.assertEquals(content.length, downloadResponse.headers().getInt(HttpHeaderNames.CONTENT_LENGTH));

        var downloadBuffer = downloadResponse.content();
        var downloadLength = downloadBuffer.readableBytes();
        var downloadContent = new byte[downloadLength];
        downloadBuffer.readBytes(downloadContent);

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

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var commonHeaders = Map.<CharSequence, Object>ofEntries();

        var downloadUrl = String.format(
                "/trac-data/api/v1/%s/FILE/%s/versions/%d/large_csv_data_100000.csv",
                TEST_TENANT, fileId.getObjectId(), fileId.getObjectVersion());


        var downloadCall = client.getRequest(downloadUrl, commonHeaders);
        downloadCall.await(TEST_TIMEOUT);

        Assertions.assertTrue(downloadCall.isDone());
        if (!downloadCall.isSuccess())
            Assertions.fail(downloadCall.cause());

        var downloadResponse = downloadCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, downloadResponse.status());
        Assertions.assertEquals("text/csv", downloadResponse.headers().get(HttpHeaderNames.CONTENT_TYPE));
        Assertions.assertEquals(content.length, downloadResponse.headers().getInt(HttpHeaderNames.CONTENT_LENGTH));

        var downloadBuffer = downloadResponse.content();
        var downloadLength = downloadBuffer.readableBytes();
        var downloadContent = new byte[downloadLength];
        downloadBuffer.readBytes(downloadContent);

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

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var commonHeaders = Map.<CharSequence, Object>ofEntries();

        var downloadUrl = String.format(
                "/trac-data/api/v1/%s/FILE/%s/versions/latest/README.md",
                TEST_TENANT, fileId.getObjectId());

        var downloadCall = client.getRequest(downloadUrl, commonHeaders);
        downloadCall.await(TEST_TIMEOUT);

        Assertions.assertTrue(downloadCall.isDone());
        if (!downloadCall.isSuccess())
            Assertions.fail(downloadCall.cause());

        var downloadResponse = downloadCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, downloadResponse.status());
        Assertions.assertEquals("text/markdown", downloadResponse.headers().get(HttpHeaderNames.CONTENT_TYPE));
        Assertions.assertEquals(content.length, downloadResponse.headers().getInt(HttpHeaderNames.CONTENT_LENGTH));

        var downloadBuffer = downloadResponse.content();
        var downloadLength = downloadBuffer.readableBytes();
        var downloadContent = new byte[downloadLength];
        downloadBuffer.readBytes(downloadContent);

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

        var downloadV2 = client.getRequest(downloadUrl, commonHeaders);
        downloadV2.await(TEST_TIMEOUT);

        Assertions.assertTrue(downloadV2.isDone());
        if (!downloadV2.isSuccess())
            Assertions.fail(downloadV2.cause());

        var downloadResponseV2 = downloadV2.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, downloadResponseV2.status());
        Assertions.assertEquals("text/markdown", downloadResponseV2.headers().get(HttpHeaderNames.CONTENT_TYPE));
        Assertions.assertEquals(updatedContent.length, downloadResponseV2.headers().getInt(HttpHeaderNames.CONTENT_LENGTH));

        var downloadBufferV2 = downloadResponseV2.content();
        var downloadLengthV2 = downloadBufferV2.readableBytes();
        var downloadContentV2 = new byte[downloadLengthV2];
        downloadBufferV2.readBytes(downloadContentV2);

        Assertions.assertArrayEquals(updatedContent, downloadContentV2);
    }
}
