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

package org.finos.tracdap.common.storage.local;

import org.apache.arrow.memory.BufferAllocator;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.common.storage.StorageOperationsTestSuite;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;


public class LocalStorageOperationsTest extends StorageOperationsTestSuite {

    @TempDir
    static Path storageDir;

    static BufferAllocator allocator;

    static LocalFileStorage storageInstance;
    static DataContext contextInstance;

    @BeforeAll
    static void setupStorage() {


        var storageProps = new Properties();
        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE");
        storageProps.put(LocalFileStorage.CONFIG_ROOT_PATH, storageDir.toString());
        storageInstance = new LocalFileStorage("TEST_STORAGE", storageProps);

        allocator = new RootAllocator();

        var elExecutor = new DefaultEventExecutor(new DefaultThreadFactory("t-events"));
        contextInstance = new DataContext(elExecutor, allocator);
    }

    @BeforeEach
    void useStorageInstance() {

        storage = storageInstance;
        dataContext = contextInstance;
    }

    @AfterAll
    static void tearDownStorage() {

        storageInstance.close();
        allocator.close();
    }
}
