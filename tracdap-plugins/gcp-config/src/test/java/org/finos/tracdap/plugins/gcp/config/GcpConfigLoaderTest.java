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

package org.finos.tracdap.plugins.gcp.config;

import org.finos.tracdap.common.exception.EStartup;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Tag("integration")
@Tag("int-config")
class GcpConfigLoaderTest {

    private static final String BUCKET_NAME = "tracdap-ci-config-loader-test";
    private static final String FILE_NAME = "test-config.properties";
    private static final String TEST_CONTENT = "trac-test = true";

    GcpConfigLoader gcpConfigLoader;

    public GcpConfigLoaderTest() {
        gcpConfigLoader = new GcpConfigLoader();
    }

    @BeforeAll
    static void setUp() {
        StorageOptions options = StorageOptions.newBuilder().build();
        Storage storage = options.getService();
        Bucket bucket = storage.get(BUCKET_NAME);
        bucket.create(FILE_NAME, TEST_CONTENT.getBytes(), "text/plain");
    }

    @AfterAll
    static void tearDown() {
        StorageOptions options = StorageOptions.newBuilder().build();
        Storage storage = options.getService();
        storage.delete(BUCKET_NAME, FILE_NAME);
    }

    @Test
    void loadConfig() throws Exception {
        String path = String.format("gcp://%s/%s", BUCKET_NAME, FILE_NAME);
        String config = gcpConfigLoader.loadTextFile(new URI(path));
        assertNotNull(config);
    }

    @Test
    void loadMissingConfig() {
        String path = String.format("gcp://%s/%s", BUCKET_NAME, "missing");

        assertThrows(EStartup.class, () -> gcpConfigLoader.loadTextFile(new URI(path)));
    }
}
