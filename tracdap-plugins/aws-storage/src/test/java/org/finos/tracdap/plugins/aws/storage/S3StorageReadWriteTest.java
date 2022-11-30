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

import org.finos.tracdap.common.concurrent.ExecutionContext;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.storage.StorageReadWriteTestSuite;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;


@Tag("integration")
@Tag("int-storage")
public class S3StorageReadWriteTest extends StorageReadWriteTestSuite {

    static Properties storageProps;
    static String testDir;
    static S3ObjectStorage setup;

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

        setup = new S3ObjectStorage(storageProps);
        setup.mkdir(testDir.substring(1), true, setupExecCtx);
    }

    @BeforeEach
    void setup() {

        execContext = new ExecutionContext(new DefaultEventExecutor(new DefaultThreadFactory("t-events")));
        dataContext = new DataContext(execContext.eventLoopExecutor(), new RootAllocator());

        storageProps.put(S3ObjectStorage.PATH_PROPERTY, testDir);
        storage = new S3ObjectStorage(storageProps);
    }

    @AfterAll
    static void tearDownStorage() {

        setup.rm(testDir, true, setupExecCtx);
    }
}
