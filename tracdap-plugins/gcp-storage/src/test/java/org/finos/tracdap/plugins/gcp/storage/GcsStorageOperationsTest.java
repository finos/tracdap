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

package org.finos.tracdap.plugins.gcp.storage;

import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.storage.StorageOperationsTestSuite;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

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
@Tag("gcp-platform")
public class GcsStorageOperationsTest extends StorageOperationsTestSuite {

    static Duration SETUP_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    static Properties storageProps;
    static String testSuiteDir;

    static EventLoopGroup elg;
    static BufferAllocator allocator;

    static DataContext setupCtx, testCtx;
    static GcsObjectStorage setupStorage, testStorage;

    @BeforeAll
    static void setupStorage() throws Exception {

        storageProps = GcsStorageEnvProps.readStorageEnvProps();

        var timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).replace(':', '.');
        var random = new Random().nextLong();

        testSuiteDir = String.format(
                "platform_storage_ops_test_suite_%s_0x%h/",
                timestamp, random);

        elg = new NioEventLoopGroup(2, new DefaultThreadFactory("ops-test"));
        allocator = new RootAllocator();

        setupCtx = new DataContext(elg.next(), allocator);
        setupStorage = new GcsObjectStorage("SETUP_STORAGE", storageProps);
        setupStorage.start(elg);

        var mkdir = setupStorage.mkdir(testSuiteDir, true, setupCtx);
        waitFor(SETUP_TIMEOUT, mkdir);
        resultOf(mkdir);

        storageProps.put(GcsObjectStorage.PREFIX_PROPERTY, testSuiteDir);

        testCtx = new DataContext(elg.next(), allocator);
        testStorage = new GcsObjectStorage("TEST_STORAGE", storageProps);
        testStorage.start(elg);
    }

    @BeforeEach
    void setup() {

        dataContext = testCtx;
        storage = testStorage;
    }

    @AfterAll
    static void tearDownStorage() throws Exception {

        testStorage.stop();

        var rm = setupStorage.rmdir(testSuiteDir, setupCtx);
        waitFor(Duration.ofSeconds(10), rm);
        resultOf(rm);

        setupStorage.stop();

        elg.shutdownGracefully();
        elg = null;
    }
}
