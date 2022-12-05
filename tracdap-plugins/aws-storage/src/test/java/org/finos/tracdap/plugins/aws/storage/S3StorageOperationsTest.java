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

package org.finos.tracdap.plugins.aws.storage;

import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.RootAllocator;
import org.finos.tracdap.common.concurrent.ExecutionContext;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.storage.StorageOperationsTestSuite;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;


@Tag("integration")
@Tag("int-storage")
public class S3StorageOperationsTest extends StorageOperationsTestSuite {

    static Properties storageProps;
    static String testDir;
    static S3ObjectStorage setup;

    static EventLoopGroup elg;
    static ExecutionContext setupExecCtx;

    @BeforeAll
    static void setupStorage() {

        var random = new Random();

        testDir = String.format(
                "/tracdap_test/test_%s_0x%h",
                DateTimeFormatter.ISO_INSTANT.format(Instant.now()),
                random.nextLong());

        setupExecCtx = new ExecutionContext(new DefaultEventExecutor(new DefaultThreadFactory("t-setup")));

        storageProps = StorageEnvProps.readStorageEnvProps();

        elg = new NioEventLoopGroup(2);

        setup = new S3ObjectStorage(storageProps);
        setup.start(elg);
        setup.mkdir(testDir.substring(1), true, setupExecCtx);
    }

    @BeforeEach
    void setup() {

        execContext = new ExecutionContext(new DefaultEventExecutor(new DefaultThreadFactory("t-events")));
        dataContext = new DataContext(execContext.eventLoopExecutor(), new RootAllocator());

        storageProps.put(S3ObjectStorage.PATH_PROPERTY, testDir);
        storage = new S3ObjectStorage(storageProps);
        storage.start(elg);
    }

    @AfterEach
    void tearDown() {
        storage.stop();
        storage = null;
    }

    @AfterAll
    static void tearDownStorage() {

        setup.rm(testDir.substring(1), true, setupExecCtx)
                .toCompletableFuture()
                .join();

        setup.stop();
        setup = null;

        elg.shutdownGracefully();
        elg = null;
    }
}
