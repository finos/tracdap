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

package org.finos.tracdap.common.storage.local;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.arrow.memory.RootAllocator;
import org.finos.tracdap.common.concurrent.ExecutionContext;
import org.finos.tracdap.common.data.DataContext;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.common.storage.LocalStorageNotWritableTestSuite;

import org.junit.jupiter.api.BeforeEach;

import java.util.Properties;

public class LocalLocalStorageNotWritableTest extends LocalStorageNotWritableTestSuite {

    @BeforeEach
    void setupStorage() {

        var storageProps = new Properties();

        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE_NOT_WRITABLE");
        storageProps.put(LocalFileStorage.CONFIG_ROOT_PATH, storageDir.toString());
        storageProps.put(LocalFileStorage.CONFIG_READ_ONLY, "true");

        storage = new LocalFileStorage(storageProps);

        execContext = new ExecutionContext(new DefaultEventExecutor(new DefaultThreadFactory("t-events")));
        dataContext = new DataContext(execContext.eventLoopExecutor(), new RootAllocator());
    }
}