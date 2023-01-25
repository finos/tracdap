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

package org.finos.tracedap.webserver;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.finos.tracdap.test.helpers.ServiceHelpers;
import org.finos.tracdap.webserver.TracWebServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;


public class TracWebServerHttp1Test {

    private static final String CONFIG_PATH = "src/test/resources/config/trac-platform.yaml";
    private static final String CONFIG_SECRET_KEY = "no_secrets";

    private static final String SERVER_ADDRESS = "http://localhost:8090";

    @TempDir
    private static Path tempDir;

    private static TracWebServer webServer = null;

    @BeforeAll
    static void startServer() throws Exception {

        var configUrl = Paths.get(CONFIG_PATH).toAbsolutePath().toUri().toURL();

        webServer = ServiceHelpers.startService(TracWebServer.class, tempDir, configUrl, CONFIG_SECRET_KEY);
    }

    @AfterAll
    static void stopServer() {

        if (webServer != null) {
            webServer.stop();
            webServer = null;
        }
    }

    @Test
    void testHeadGet_ok() throws Exception {

        var url = SERVER_ADDRESS + "/dir1/foo.html";

        var request = HttpRequest.newBuilder(new URI(url))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3));

        var client = HttpClient.newHttpClient();

        var head = request.method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        var headResponse = client.send(head, HttpResponse.BodyHandlers.ofString());

        var get = request.GET().build();
        var getResponse = client.send(get, HttpResponse.BodyHandlers.ofString());

        var sizeHeader = headResponse.headers().firstValueAsLong(HttpHeaderNames.CONTENT_LENGTH.toString());
        var typeHeader = headResponse.headers().firstValue(HttpHeaderNames.CONTENT_TYPE.toString());

        Assertions.assertTrue(sizeHeader.isPresent());
        Assertions.assertTrue(typeHeader.isPresent());

        var size = sizeHeader.getAsLong();

        Assertions.assertEquals(size, getResponse.body().length());
    }

    @Test
    void testHeadGet_dir() throws Exception {

        var url = SERVER_ADDRESS + "/dir1";  // Note no trailing slash - server should handle it

        var request = HttpRequest.newBuilder(new URI(url))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3));

        var client = HttpClient.newHttpClient();

        var head = request.method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        var headResponse = client.send(head, HttpResponse.BodyHandlers.ofString());

        var get = request.GET().build();
        var getResponse = client.send(get, HttpResponse.BodyHandlers.ofString());

        var sizeHeader = headResponse.headers().firstValueAsLong(HttpHeaderNames.CONTENT_LENGTH.toString());
        var typeHeader = headResponse.headers().firstValue(HttpHeaderNames.CONTENT_TYPE.toString());

        Assertions.assertTrue(sizeHeader.isPresent());
        Assertions.assertTrue(typeHeader.isPresent());

        var size = sizeHeader.getAsLong();

        Assertions.assertEquals(size, getResponse.body().length());
    }

    @Test
    void testHeadGet_rootDir() throws Exception {

        var url = SERVER_ADDRESS + "/";

        var request = HttpRequest.newBuilder(new URI(url))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3));

        var client = HttpClient.newHttpClient();

        var head = request.method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        var headResponse = client.send(head, HttpResponse.BodyHandlers.ofString());

        var get = request.GET().build();
        var getResponse = client.send(get, HttpResponse.BodyHandlers.ofString());

        var sizeHeader = headResponse.headers().firstValueAsLong(HttpHeaderNames.CONTENT_LENGTH.toString());
        var typeHeader = headResponse.headers().firstValue(HttpHeaderNames.CONTENT_TYPE.toString());

        Assertions.assertTrue(sizeHeader.isPresent());
        Assertions.assertTrue(typeHeader.isPresent());

        var size = sizeHeader.getAsLong();

        Assertions.assertEquals(size, getResponse.body().length());
    }

    @Test
    void testHeadGet_missing() throws Exception {

        var url = SERVER_ADDRESS + "/bar.html";

        var request = HttpRequest.newBuilder(new URI(url))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3));

        var client = HttpClient.newHttpClient();

        var head = request.method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        var headResponse = client.send(head, HttpResponse.BodyHandlers.ofString());

        var get = request.GET().build();
        var getResponse = client.send(get, HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND.code(), headResponse.statusCode());
        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND.code(), getResponse.statusCode());
    }

    @Test
    void testHeadGet_missingIndex() throws Exception {

        var url = SERVER_ADDRESS + "/dir2/";

        var request = HttpRequest.newBuilder(new URI(url))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3));

        var client = HttpClient.newHttpClient();

        var head = request.method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        var headResponse = client.send(head, HttpResponse.BodyHandlers.ofString());

        var get = request.GET().build();
        var getResponse = client.send(get, HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND.code(), headResponse.statusCode());
        Assertions.assertEquals(HttpResponseStatus.NOT_FOUND.code(), getResponse.statusCode());
    }

}
