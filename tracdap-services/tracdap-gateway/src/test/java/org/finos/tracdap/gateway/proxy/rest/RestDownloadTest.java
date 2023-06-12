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
import org.finos.tracdap.test.helpers.PlatformTest;

import com.google.protobuf.ByteString;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;

import org.finos.tracdap.test.helpers.ResourceHelpers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.netty.util.NetUtil.LOCALHOST;
import static org.finos.tracdap.test.meta.TestData.TEST_TENANT;


public class RestDownloadTest {

    public static final short TEST_GW_PORT = 8080;
    public static final long TEST_TIMEOUT = 10 * 1000;  // 10 second timeout

    public static final String TRAC_CONFIG_UNIT = "config/trac-unit.yaml";
    public static final String TRAC_GW_CONFIG_UNIT = "config/trac-gw-unit.yaml";

    @RegisterExtension
    public static final PlatformTest platform = PlatformTest.forConfig(TRAC_CONFIG_UNIT, TRAC_GW_CONFIG_UNIT)
            .addTenant(TEST_TENANT)
            .startMeta()
            .startData()
            .startGateway()
            .build();

    private static final Path tracRepoDir = ResourceHelpers.findTracProjectRoot();

    @Test
    void simpleDownload() throws Exception {

        var dataClient = platform.dataClientBlocking();

        var content = Files.readAllBytes(tracRepoDir.resolve("README.md"));

        var upload = FileWriteRequest.newBuilder()
                .setName("README.md")
                .setMimeType("text/markdown")
                .setContent(ByteString.copyFrom(content))
                .build();

        var fileId = dataClient.updateSmallFile(upload);

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var commonHeaders = Map.<CharSequence, Object>ofEntries();

        var downloadUrl = String.format(
                "/trac-data/api/v1/FILE/%s/versions/%d/README.md",
                fileId.getObjectId(), fileId.getObjectVersion());


        var downloadCall = client.getRequest(downloadUrl, commonHeaders);
        downloadCall.await(TEST_TIMEOUT);

        var downloadResponse = downloadCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, downloadResponse.status());

        var downloadBuffer = downloadResponse.content();
        var downloadLength = downloadBuffer.readableBytes();
        var downloadContent = new byte[downloadLength];
        downloadBuffer.readBytes(downloadContent);

        Assertions.assertArrayEquals(content, downloadContent);
    }

    @Test
    void latestVersionDownload() throws Exception {

        var dataClient = platform.dataClientBlocking();

        var content = Files.readAllBytes(tracRepoDir.resolve("README.md"));

        var upload = FileWriteRequest.newBuilder()
                .setName("README.md")
                .setMimeType("text/markdown")
                .setContent(ByteString.copyFrom(content))
                .build();

        var fileId = dataClient.updateSmallFile(upload);

        var client = new Http1Client(HttpScheme.HTTP, LOCALHOST, TEST_GW_PORT);
        var commonHeaders = Map.<CharSequence, Object>ofEntries();

        var downloadUrl = String.format(
                "/trac-data/api/v1/FILE/%s/versions/latest/README.md",
                fileId.getObjectId());


        var downloadCall = client.getRequest(downloadUrl, commonHeaders);
        downloadCall.await(TEST_TIMEOUT);

        var downloadResponse = downloadCall.getNow();
        Assertions.assertEquals(HttpResponseStatus.OK, downloadResponse.status());

        var downloadBuffer = downloadResponse.content();
        var downloadLength = downloadBuffer.readableBytes();
        var downloadContent = new byte[downloadLength];
        downloadBuffer.readBytes(downloadContent);

        Assertions.assertArrayEquals(content, downloadContent);
    }
}
