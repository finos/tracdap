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

package org.finos.tracdap.plugins.azure.storage;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.storage.CommonFileStorage;
import org.finos.tracdap.common.storage.StorageReadOnlyTestSuite;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Random;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;


@Tag("integration")
@Tag("int-storage")
@Tag("azure-platform")
public class AzureStorageReadOnlyTest extends StorageReadOnlyTestSuite {

    static Duration SETUP_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    static Properties storageProps;
    static String testSuiteDir;

    static EventLoopGroup elg;
    static BufferAllocator allocator;

    static DataContext setupCtx, testCtx;
    static AzureBlobStorage setupStorage, testRwStorage, testRoStorage;

    static int testNumber;

    @BeforeAll
    static void setupStorage() throws Exception {

        var timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).replace(':', '.');
        var random = new Random().nextLong();

        storageProps = AzureStorageEnvProps.readStorageEnvProps();
        testSuiteDir = String.format(
                "platform_storage_ops_test_suite_%s_0x%h/",
                timestamp, random);

        elg = new NioEventLoopGroup(2, new DefaultThreadFactory("ops-test"));
        allocator = new RootAllocator();

        setupCtx = new DataContext(elg.next(), allocator);
        setupStorage = new AzureBlobStorage("STORAGE_SETUP", storageProps);
        setupStorage.start(elg);

        var mkdir = setupStorage.mkdir(testSuiteDir, true, setupCtx);
        waitFor(SETUP_TIMEOUT, mkdir);
        resultOf(mkdir);

        storageProps.put(AzureBlobStorage.PREFIX_PROPERTY, testSuiteDir);
        testRwStorage = new AzureBlobStorage("TEST_" + testNumber + "_RW", storageProps);
        testRwStorage.start(elg);

        var roProps = new Properties();
        roProps.putAll(storageProps);
        roProps.put(CommonFileStorage.READ_ONLY_CONFIG_KEY, "true");

        testRoStorage = new AzureBlobStorage("TEST_" + testNumber + "_RO", roProps);
        testRoStorage.start(elg);

        testCtx = new DataContext(elg.next(), allocator);
    }

    @BeforeEach
    void setup() {

        rwStorage = testRwStorage;
        roStorage = testRoStorage;
        dataContext = testCtx;
    }

    @AfterAll
    static void tearDownStorage() throws Exception {

        testRwStorage.stop();
        testRoStorage.stop();

        var rm = setupStorage.rmdir(testSuiteDir, setupCtx);
        waitFor(Duration.ofSeconds(10), rm);
        resultOf(rm);

        setupStorage.stop();

        elg.shutdownGracefully();
        allocator.close();
    }
}
