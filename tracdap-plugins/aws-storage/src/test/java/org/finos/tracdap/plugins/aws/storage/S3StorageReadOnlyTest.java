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

package org.finos.tracdap.plugins.aws.storage;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.finos.tracdap.common.concurrent.ExecutionContext;
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
public class S3StorageReadOnlyTest extends StorageReadOnlyTestSuite {

    static Duration SETUP_TIMEOUT = Duration.of(5, ChronoUnit.SECONDS);

    static Properties storageProps;
    static String testSuiteDir;

    static EventLoopGroup elg;
    static BufferAllocator allocator;

    static S3ObjectStorage setupStorage;
    static ExecutionContext setupCtx;

    static int testNumber;

    @BeforeAll
    static void setupStorage() throws Exception {

        var timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).replace(':', '.');
        var random = new Random().nextLong();

        testSuiteDir = String.format(
                "platform_storage_ro_test_suite_%s_0x%h/",
                timestamp, random);

        setupCtx = new ExecutionContext(new DefaultEventExecutor(new DefaultThreadFactory("t-setup")));

        storageProps = S3StorageEnvProps.readStorageEnvProps();

        elg = new NioEventLoopGroup(2);
        allocator = new RootAllocator();

        setupStorage = new S3ObjectStorage("STORAGE_SETUP", storageProps);
        setupStorage.start(elg);

        var mkdir = setupStorage.mkdir(testSuiteDir, true, setupCtx);
        waitFor(Duration.ofSeconds(10), mkdir);
        resultOf(mkdir);
    }

    @BeforeEach
    void setup() throws Exception {

        var testDir = String.format("%stest_%d", testSuiteDir, ++testNumber);

        var mkdir = setupStorage.mkdir(testDir, false, setupCtx);
        waitFor(SETUP_TIMEOUT, mkdir);
        resultOf(mkdir);

        storageProps.put(S3ObjectStorage.PREFIX_PROPERTY, testDir);
        rwStorage = new S3ObjectStorage("TEST_" + testNumber + "_RW", storageProps);
        rwStorage.start(elg);

        var roProps = new Properties();
        roProps.putAll(storageProps);
        roProps.put(CommonFileStorage.READ_ONLY_CONFIG_KEY, "true");

        roStorage = new S3ObjectStorage("TEST_" + testNumber + "_RO", roProps);
        roStorage.start(elg);

        execContext = new ExecutionContext(elg.next());
        dataContext = new DataContext(execContext.eventLoopExecutor(), allocator);
    }

    @AfterEach
    void tearDown() {

        rwStorage.stop();
        rwStorage = null;

        roStorage.stop();
        roStorage = null;
    }

    @AfterAll
    static void tearDownStorage() throws Exception {

        var rm = setupStorage.rmdir(testSuiteDir, setupCtx);
        waitFor(Duration.ofSeconds(10), rm);
        resultOf(rm);

        setupStorage.stop();
        setupStorage = null;

        elg.shutdownGracefully();
        elg = null;
    }
}
