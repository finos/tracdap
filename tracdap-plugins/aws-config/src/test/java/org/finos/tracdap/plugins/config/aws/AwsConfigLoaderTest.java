/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.plugins.config.aws;

import org.finos.tracdap.common.exception.EStartup;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
@Tag("int-aws")
class AwsConfigLoaderTest {

    private static String BUCKET_NAME = "accenture-trac-test";
    private static String FILE_NAME = "test-config.properties";
    private static String TEST_CONTENT = "trac-test = true";

    AwsConfigLoader awsConfigLoader;

    public AwsConfigLoaderTest() {
        awsConfigLoader = new AwsConfigLoader();
    }

    @BeforeAll
    static void setUp() {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        if (!s3.doesBucketExist(BUCKET_NAME)) {
            s3.createBucket(BUCKET_NAME);
        }
        s3.putObject(BUCKET_NAME, FILE_NAME, TEST_CONTENT);
    }

    @AfterAll
    static void tearDown() {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        if (s3.doesObjectExist(BUCKET_NAME, FILE_NAME)) {
            s3.deleteObject(BUCKET_NAME, FILE_NAME);
        }
    }

    @Test
    void loadConfig() throws Exception {
        String path = String.format("s3://%s/%s", BUCKET_NAME, FILE_NAME);
        String config = awsConfigLoader.loadTextFile(new URI(path));
        assertNotNull(config);
    }

    @Test
    void loadMissingConfig() {
        String path = String.format("s3://%s/%s", BUCKET_NAME, "missing");

        assertThrows(EStartup.class, () -> awsConfigLoader.loadTextFile(new URI(path)));
    }
}
