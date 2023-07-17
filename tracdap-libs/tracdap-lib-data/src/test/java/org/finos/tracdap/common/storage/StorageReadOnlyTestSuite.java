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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EStorageAccess;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.getResultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.finos.tracdap.test.storage.StorageTestHelpers.makeSmallFile;
import static org.mockito.Mockito.*;


public abstract class StorageReadOnlyTestSuite {

    //Test suite for readOnly protection on IFileStorage
    // This should be available for all storage types that use CommonFileStorage as a base

    public static final Duration TEST_TIMEOUT = Duration.ofSeconds(20);
    public static final Duration ASYNC_DELAY = Duration.ofMillis(100);

    protected IFileStorage rwStorage;
    protected IFileStorage roStorage;
    protected IDataContext dataContext;

    @Test
    void testMkdir_readOnly() throws Exception {

        var dir = roStorage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, dir);
        Assertions.assertThrows(EStorageAccess.class, () -> getResultOf(dir));

        var dirPresent = roStorage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, dirPresent);
        Assertions.assertFalse(getResultOf(dirPresent));
    }

    @Test
    void testRm_readOnly() throws Exception {

        // Simplest case - create one file and delete it.
        // The file is created independently of the storage, because the storage is assumed to be read only
        // and the file cannot be created in it.

        var prepare = makeSmallFile("test_file.txt", rwStorage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var created = roStorage.exists("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, created);
        Assertions.assertTrue(getResultOf(created));

        var rm = roStorage.rm("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, rm);
        Assertions.assertThrows(EStorageAccess.class, () -> getResultOf(rm));

        // File should not be gone

        var exists = roStorage.exists("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertTrue(getResultOf(exists));
    }

    @Test
    void testRmdir_readOnly() throws Exception {

        // Simplest case - create one file and delete it.
        // The file is created independently of the storage, because the storage is assumed to be read only
        // and the file cannot be created in it.

        var prepare = rwStorage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var created = roStorage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, created);
        Assertions.assertTrue(getResultOf(created));

        var rmdir = roStorage.rmdir("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, rmdir);
        Assertions.assertThrows(EStorageAccess.class, () -> getResultOf(rmdir));

        // Dir should not be gone

        var exists = roStorage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertTrue(getResultOf(exists));
    }

    @Test
    void testWriter_readOnly() {

        var storagePath = "any_file.txt";

        var writeSignal = new CompletableFuture<Long>();
        var writer = roStorage.writer(storagePath, writeSignal, dataContext);

        var subscription = mock(Flow.Subscription.class);
        writer.onSubscribe(subscription);
        verify(subscription, timeout(ASYNC_DELAY.toMillis())).cancel();

        waitFor(TEST_TIMEOUT, writeSignal);
        Assertions.assertThrows(EStorageAccess.class, () -> getResultOf(writeSignal));
    }
}
