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

package org.finos.tracdap.common.storage.local;

import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.common.storage.StorageOperationsTestSuite;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;


public class LocalStorageOperationsTest extends StorageOperationsTestSuite {

    @TempDir
    Path storageDir;

    @BeforeEach
    void setupStorage() {

        var storageProps = new Properties();
        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE");
        storageProps.put(LocalFileStorage.CONFIG_ROOT_PATH, storageDir.toString());
        storage = new LocalFileStorage("TEST_STORAGE", storageProps);

        var elExecutor = new DefaultEventExecutor(new DefaultThreadFactory("t-events"));
        dataContext = new DataContext(elExecutor, new RootAllocator());
    }
}
