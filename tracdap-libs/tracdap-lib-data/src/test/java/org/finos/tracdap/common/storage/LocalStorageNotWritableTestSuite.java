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

import io.netty.buffer.ByteBufAllocator;
import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EStorageAccess;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;

public abstract class LocalStorageNotWritableTestSuite {

    @TempDir
    protected Path storageDir;

    public static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    protected IFileStorage storage;
    protected IExecutionContext execContext;
    protected IDataContext dataContext;


    @Test
    void testRm_fail() throws Exception {

        // Simplest case - create one file and delete it.
        // The file is created independently of the storage, because the storage is assumed to be read only
        // and the file cannot be created in it.

        File smallFile = new File(storageDir.resolve("test_file.txt").toString());

        boolean fileCreated;
        String message = null;

        try {
            fileCreated = smallFile.createNewFile();
        } catch(Exception e) {
            fileCreated = false;
            message = e.getMessage();
        }

        Assertions.assertTrue(fileCreated, message==null?"The test file could not be created.":message);

        var rm = storage.rm("test_file.txt", false, execContext);
        waitFor(TEST_TIMEOUT, rm);

        Assertions.assertThrows(EStorageAccess.class, () -> resultOf(rm));

        // File should not be gone

        var exists = storage.exists("test_file.txt", execContext);
        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertTrue(resultOf(exists));

        boolean fileDeleted;
        String message2 = null;

        try {
            fileDeleted = smallFile.delete();
        } catch (Exception e) {
            fileDeleted = false;
            message2 = e.getMessage();
        }

        Assertions.assertTrue(fileDeleted, message2==null?"The test file could not be deleted.":message2);
    }

    @Test
    void testMkdir_fail() throws Exception {

        var dir = storage.mkdir("test_dir", false, execContext);
        waitFor(TEST_TIMEOUT, dir);

        Assertions.assertThrows(EStorageAccess.class, () -> resultOf(dir));

        var dirPresent = storage.exists("test_dir", execContext);
        waitFor(TEST_TIMEOUT, dirPresent);

        Assertions.assertFalse(resultOf(dirPresent));
    }

    @Test
    void writer_basic_fail() {

        var storagePath = "any_file.txt";

        Assertions.assertThrows(EStorageAccess.class, () -> writerTest(storagePath, storage, dataContext));
    }

    static void writerTest(String storagePath, IFileStorage storage, IDataContext dataContext) {

        var writeSignal = new CompletableFuture<Long>();

        storage.writer(storagePath, writeSignal, dataContext);
    }
}
