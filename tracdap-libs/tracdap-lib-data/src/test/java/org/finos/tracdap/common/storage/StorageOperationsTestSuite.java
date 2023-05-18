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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.exception.EStorageRequest;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EValidationGap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.function.BiFunction;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.*;
import static org.finos.tracdap.test.storage.StorageTestHelpers.makeFile;
import static org.finos.tracdap.test.storage.StorageTestHelpers.makeSmallFile;


public abstract class StorageOperationsTestSuite {

    /* >>> Test suite for IFileStorage - file system operations, functional tests

    These tests are implemented purely in terms of the IFileStorage interface. The test suite can be run for
    any storage implementation and a valid storage implementations must pass this test suite.

    NOTE: To test a new storage implementation, setupStorage() must be replaced
    with a method to provide a storage implementation based on a supplied test config.

    Storage implementations may also wish to supply their own tests that use native APIs to set up and control
    tests. This can allow for finer grained control, particularly when testing corner cases and error conditions.
     */

    // Unit test implementation for local storage is in LocalStorageOperationsTest

    public static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    protected IFileStorage storage;
    protected IDataContext dataContext;


    // -----------------------------------------------------------------------------------------------------------------
    // EXISTS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testExists_file() throws Exception {

        var prepare = makeSmallFile("test_file.txt", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var filePresent = storage.exists("test_file.txt", dataContext);
        var fileNotPresent = storage.exists("other_file.txt", dataContext);

        waitFor(TEST_TIMEOUT, filePresent, fileNotPresent);

        Assertions.assertTrue(getResultOf(filePresent));
        Assertions.assertFalse(getResultOf(fileNotPresent));
    }

    @Test
    void testExists_emptyFile() throws Exception {

        var empty = dataContext.arrowAllocator().getEmpty();
        var prepare = makeFile("test_file.txt", empty, storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var emptyFileExist = storage.exists("test_file.txt", dataContext);

        waitFor(TEST_TIMEOUT, emptyFileExist);

        Assertions.assertTrue(getResultOf(emptyFileExist));
    }


    @Test
    void testExists_dir() throws Exception {

        var prepare = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var dirPresent = storage.exists("test_dir", dataContext);
        var dirNotPresent = storage.exists("other_dir", dataContext);

        waitFor(TEST_TIMEOUT, dirPresent, dirNotPresent);

        Assertions.assertTrue(getResultOf(dirPresent));
        Assertions.assertFalse(getResultOf(dirNotPresent));
    }

    @Test
    void testExists_parentDir() throws Exception {

        var prepare = storage.mkdir("parent_dir/child_dir", true, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var dirPresent = storage.exists("parent_dir", dataContext);
        var dirNotPresent = storage.exists("other_dir", dataContext);

        waitFor(TEST_TIMEOUT, dirPresent, dirNotPresent);

        Assertions.assertTrue(getResultOf(dirPresent));
        Assertions.assertFalse(getResultOf(dirNotPresent));
    }

    @Test
    void testExists_storageRoot() throws Exception {

        // Storage root should always exist

        var exists = storage.exists(".", dataContext);

        waitFor(TEST_TIMEOUT, exists);

        Assertions.assertTrue(getResultOf(exists));
    }

    @Test
    void testExists_badPaths() {

        testBadPaths(storage::exists);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SIZE
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testSize_ok() throws Exception {

        var bytes = "Content of a certain size\n".getBytes(StandardCharsets.UTF_8);
        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());

        var expectedSize = content.readableBytes();
        var prepare = makeFile("test_file.txt", content, storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var size = storage.size("test_file.txt", dataContext);

        waitFor(TEST_TIMEOUT, size);

        Assertions.assertEquals(expectedSize, getResultOf(size));
    }

    @Test
    void testSize_emptyFile() throws Exception {

        var empty = dataContext.arrowAllocator().getEmpty();
        var prepare = makeFile("test_file.txt", empty, storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var size =  storage.size("test_file.txt", dataContext);

        waitFor(TEST_TIMEOUT, size);

        Assertions.assertEquals(0, getResultOf(size));
    }

    @Test
    void testSize_dir() {

        var prepare = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var size = storage.size("test_dir", dataContext);

        waitFor(TEST_TIMEOUT, size);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(size));
    }

    @Test
    void testSize_missing() {

        var size = storage.size("missing_file.txt", dataContext);

        waitFor(TEST_TIMEOUT, size);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(size));
    }

    @Test
    void testSize_storageRoot() {

        // Storage root is a directory, size operation should fail with EStorageRequest

        var size = storage.size(".", dataContext);

        waitFor(TEST_TIMEOUT, size);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(size));
    }

    @Test
    void testSize_badPaths() {

        testBadPaths(storage::size);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // STAT
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testStat_fileOk() throws Exception {

        // Simple case - stat a file

        var bytes = "Sample content for stat call\n".getBytes(StandardCharsets.UTF_8);
        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());

        var expectedSize = content.readableBytes();
        var prepare = storage
                .mkdir("some_dir", false, dataContext)
                .thenCompose(x -> makeFile("some_dir/test_file.txt", content, storage, dataContext));

        waitFor(TEST_TIMEOUT, prepare);

        var stat = storage.stat("some_dir/test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        var statResult = getResultOf(stat);

        Assertions.assertEquals("some_dir/test_file.txt", statResult.storagePath);
        Assertions.assertEquals("test_file.txt", statResult.fileName);
        Assertions.assertEquals(FileType.FILE, statResult.fileType);
        Assertions.assertEquals(expectedSize, statResult.size);
    }

    @Test
    void testStat_fileMTime() throws Exception {

        // All storage implementations must implement mtime for files
        // Using 1 second as the required resolution (at least one FS, AWS S3, has 1 second resolution)

        var testStart = Instant.now();
        Thread.sleep(1000);  // Let time elapse before/after the test calls

        var prepare = makeSmallFile("test_file.txt", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var stat = storage.stat("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        Thread.sleep(1000);  // Let time elapse before/after the test calls
        var testFinish = Instant.now();

        var statResult = getResultOf(stat);

        Assertions.assertTrue(statResult.mtime.isAfter(testStart));
        Assertions.assertTrue(statResult.mtime.isBefore(testFinish));
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "ATime testing disabled for Windows / NTFS")
    void testStat_fileATime() throws Exception {

        // For cloud storage implementations, file atime may not be available
        // So, allow implementations to return a null atime
        // If an atime is returned for files, then it must be valid

        // NTFS does not handle atime reliably for this test. From the docs:

        //      NTFS delays updates to the last access time for a file by up to one hour after the last access.
        //      NTFS also permits last access time updates to be disabled.
        //      Last access time is not updated on NTFS volumes by default.

        // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime?redirectedfrom=MSDN

        // On FAT32, atime is limited to an access date, i.e. one-day resolution

        var prepare = makeSmallFile("test_file.txt", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var testStart = Instant.now();
        Thread.sleep(10);  // Let time elapse before/after the test calls

        var readData =
                storage.size("test_file.txt", dataContext).thenCompose(size ->
                storage.readChunk("test_file.txt", 0, (int)(long) size, dataContext));

        waitFor(TEST_TIMEOUT, readData);
        readData.toCompletableFuture().get().close();

        var stat = storage.stat("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        Thread.sleep(10);  // Let time elapse before/after the test calls
        var testFinish = Instant.now();

        var statResult = getResultOf(stat);

        Assertions.assertTrue(statResult.atime == null || statResult.atime.isAfter(testStart));
        Assertions.assertTrue(statResult.atime == null || statResult.atime.isBefore(testFinish));
    }

    @Test
    void testStat_dirOk() throws Exception {

        var prepare = storage.mkdir("some_dir/test_dir", true, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var stat = storage.stat("some_dir/test_dir", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        var statResult = getResultOf(stat);

        Assertions.assertEquals("some_dir/test_dir", statResult.storagePath);
        Assertions.assertEquals("test_dir", statResult.fileName);
        Assertions.assertEquals(FileType.DIRECTORY, statResult.fileType);

        // Size field for directories should always be set to 0
        Assertions.assertEquals(0, statResult.size);
    }

    @Test
    void testStat_dirImplicitOk() throws Exception {

        var prepare = storage.mkdir("some_dir/test_dir", true, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var stat = storage.stat("some_dir", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        var statResult = getResultOf(stat);

        Assertions.assertEquals("some_dir", statResult.storagePath);
        Assertions.assertEquals("some_dir", statResult.fileName);
        Assertions.assertEquals(FileType.DIRECTORY, statResult.fileType);

        // Size field for directories should always be set to 0
        Assertions.assertEquals(0, statResult.size);
    }

    @Test
    void testStat_dirMTime() throws Exception {

        // mtime and atime for dirs is unlikely to be supported in cloud storage buckets
        // So, all of these fields are optional in stat responses for directories

        var prepare1 = storage.mkdir("some_dir/test_dir", true, dataContext);
        waitFor(TEST_TIMEOUT, prepare1);

        // "Modify" the directory by adding a file to it

        var testStart = Instant.now();
        Thread.sleep(10);  // Let time elapse before/after the test calls

        var prepare2 = makeSmallFile("some_dir/test_dir/a_file.txt", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare2);

        var stat = storage.stat("some_dir/test_dir", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        Thread.sleep(10);  // Let time elapse before/after the test calls
        var testFinish = Instant.now();

        var statResult = getResultOf(stat);
        Assertions.assertTrue(statResult.mtime == null || statResult.mtime.isAfter(testStart));
        Assertions.assertTrue(statResult.mtime == null || statResult.mtime.isBefore(testFinish));
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "ATime testing disabled for Windows / NTFS")
    void testStat_dirATime() throws Exception {

        // This test fails intermittently for local storage on Windows, for the same reason as test_stat_file_atime
        // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime?redirectedfrom=MSDN

        // mtime and atime for dirs is unlikely to be supported in cloud storage buckets
        // So, all of these fields are optional in stat responses for directories

        var prepare1 = storage
                .mkdir("some_dir/test_dir", true, dataContext)
                .thenCompose(x -> makeSmallFile("some_dir/test_dir/a_file.txt", storage, dataContext));
        waitFor(TEST_TIMEOUT, prepare1);

        // Access the directory by running "ls" on it

        var testStart = Instant.now();
        Thread.sleep(10);  // Let time elapse before/after the test calls

        var prepare2 = storage.ls("some_dir/test_dir", dataContext);
        waitFor(TEST_TIMEOUT, prepare2);

        var stat = storage.stat("some_dir/test_dir", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        Thread.sleep(10);  // Let time elapse before/after the test calls
        var testFinish = Instant.now();

        var statResult = getResultOf(stat);
        Assertions.assertTrue(statResult.atime == null || statResult.atime.isAfter(testStart));
        Assertions.assertTrue(statResult.atime == null || statResult.atime.isBefore(testFinish));
    }

    @Test
    void testStat_storageRoot() throws Exception {

        var rootStat = storage.stat(".", dataContext);

        waitFor(TEST_TIMEOUT, rootStat);

        var statResult = getResultOf(rootStat);

        Assertions.assertEquals(".", statResult.storagePath);
        Assertions.assertEquals(".", statResult.fileName);
        Assertions.assertEquals(FileType.DIRECTORY, statResult.fileType);

        // Size field for directories should always be set to 0
        Assertions.assertEquals(0, statResult.size);
    }

    @Test
    void testStat_missing() {

        var stat = storage.stat("does_not_exist.dat", dataContext);
        waitFor(TEST_TIMEOUT, stat);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(stat));
    }

    @Test
    void testStat_badPaths() {

        testBadPaths(storage::stat);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testLs_ok() throws Exception {

        // Simple listing, dir containing one file and one sub dir

        var prepare = storage.mkdir("test_dir", false, dataContext)
                .thenCompose(x -> storage.mkdir("test_dir/child_1", false, dataContext))
                .thenCompose(x -> makeSmallFile("test_dir/child_2.txt", storage, dataContext));
        waitFor(TEST_TIMEOUT, prepare);

        var ls = storage.ls("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, ls);

        var listing = getResultOf(ls);

        Assertions.assertEquals(2, listing.size());

        var child1 = listing.stream().filter(e -> e.fileName.equals("child_1")).findFirst();
        var child2 = listing.stream().filter(e -> e.fileName.equals("child_2.txt")).findFirst();

        Assertions.assertTrue(child1.isPresent());
        Assertions.assertEquals("test_dir/child_1", child1.get().storagePath);
        Assertions.assertEquals(FileType.DIRECTORY, child1.get().fileType);

        Assertions.assertTrue(child2.isPresent());
        Assertions.assertEquals("test_dir/child_2.txt", child2.get().storagePath);
        Assertions.assertEquals(FileType.FILE, child2.get().fileType);
    }

    @Test
    void testLs_extensions() throws Exception {

        // Corner case - dir with an extension, file without extension


        var prepare = storage.mkdir("ls_extensions", false, dataContext)
                .thenCompose(x -> storage.mkdir("ls_extensions/child_1.dat", false, dataContext))
                .thenCompose(x -> makeSmallFile("ls_extensions/child_2_file", storage, dataContext));
        waitFor(TEST_TIMEOUT, prepare);

        var ls = storage.ls("ls_extensions", dataContext);
        waitFor(TEST_TIMEOUT, ls);

        var listing = getResultOf(ls);

        Assertions.assertEquals(2, listing.size());

        var child1 = listing.stream().filter(e -> e.fileName.equals("child_1.dat")).findFirst();
        var child2 = listing.stream().filter(e -> e.fileName.equals("child_2_file")).findFirst();

        Assertions.assertTrue(child1.isPresent());
        Assertions.assertEquals("ls_extensions/child_1.dat", child1.get().storagePath);
        Assertions.assertEquals(FileType.DIRECTORY, child1.get().fileType);

        Assertions.assertTrue(child2.isPresent());
        Assertions.assertEquals("ls_extensions/child_2_file", child2.get().storagePath);
        Assertions.assertEquals(FileType.FILE, child2.get().fileType);
    }

    @Test
    void testLs_trailingSlash() throws Exception {

        // Storage path should be accepted with or without trailing slash

        var prepare = storage.mkdir("ls_trailing_slash", false, dataContext)
                .thenCompose(x -> makeSmallFile("ls_trailing_slash/some_file.txt", storage, dataContext));
        waitFor(TEST_TIMEOUT, prepare);

        var ls1 = storage.ls("ls_trailing_slash", dataContext);
        var ls2 = storage.ls("ls_trailing_slash/", dataContext);
        waitFor(TEST_TIMEOUT, ls1, ls2);

        var listing1 = getResultOf(ls1);
        var listing2 = getResultOf(ls2);

        Assertions.assertEquals(1, listing1.size());
        Assertions.assertEquals(1, listing2.size());
    }

    @Test
    void testLs_storageRootAllowed() throws Exception {

        // Ls is one operation that is allowed on the storage root!

        var prepare = storage.mkdir("test_dir", false, dataContext)
                .thenCompose(x -> makeSmallFile("test_file.txt", storage, dataContext));
        waitFor(TEST_TIMEOUT, prepare);

        var ls = storage.ls(".", dataContext);
        waitFor(TEST_TIMEOUT, ls);

        var listing = getResultOf(ls);

        Assertions.assertTrue(listing.size() >= 2);

        var child1 = listing.stream().filter(e -> e.fileName.equals("test_dir")).findFirst();
        var child2 = listing.stream().filter(e -> e.fileName.equals("test_file.txt")).findFirst();

        Assertions.assertTrue(child1.isPresent());
        Assertions.assertEquals("test_dir", child1.get().storagePath);
        Assertions.assertEquals(FileType.DIRECTORY, child1.get().fileType);

        Assertions.assertTrue(child2.isPresent());
        Assertions.assertEquals("test_file.txt", child2.get().storagePath);
        Assertions.assertEquals(FileType.FILE, child2.get().fileType);
    }

    @Test
    void testLs_file() throws Exception {

        // Attempt to call ls on a file is an error

        var prepare = makeSmallFile("test_file", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var ls = storage.ls("test_file", dataContext);
        waitFor(TEST_TIMEOUT, ls);

        var fileLs = getResultOf(ls);

        Assertions.assertEquals(1, fileLs.size());

        var stat = fileLs.get(0);

        Assertions.assertEquals(FileType.FILE, stat.fileType);
        Assertions.assertEquals("test_file", stat.fileName);
        Assertions.assertEquals("test_file", stat.storagePath);
    }

    @Test
    void testLs_missing() {

        // Ls on a missing path is an error condition

        var ls = storage.ls("dir_does_not_exist/", dataContext);
        waitFor(TEST_TIMEOUT, ls);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(ls));
    }

    @Test
    void testLs_badPaths() {

        testBadPaths(storage::ls);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // MKDIR
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testMkdir_ok() throws Exception {

        // Simplest case - create a single directory

        var mkdir = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        Assertions.assertDoesNotThrow(() -> getResultOf(mkdir));

        // Creating a single child dir when the parent already exists

        var childDir = storage.mkdir("test_dir/child", false, dataContext);
        waitFor(TEST_TIMEOUT, childDir);

        Assertions.assertDoesNotThrow(() -> getResultOf(childDir));

        var dirExists = storage.exists("test_dir", dataContext);
        var childExists = storage.exists("test_dir/child", dataContext);
        waitFor(TEST_TIMEOUT, dirExists, childExists);

        Assertions.assertTrue(getResultOf(dirExists));
        Assertions.assertTrue(getResultOf(childExists));
    }

    @Test
    void testMkdir_dirExists() throws Exception {

        // It is not an error to call mkdir on an existing directory

        var prepare = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var exists1 = storage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists1);
        var exists1Result = getResultOf(exists1);

        Assertions.assertTrue(exists1Result);

        var mkdir = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        var exists2 = storage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists2);
        var exists2Result = getResultOf(exists2);

        Assertions.assertTrue(exists2Result);
    }

    @Test
    void testMkdir_fileExists() {

        // mkdir should always fail if requested dir already exists and is a file

        var prepare = makeSmallFile("test_dir", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var mkdir = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(mkdir));
    }

    @Test
    void testMkdir_missingParent() throws Exception {

        // With recursive = false, mkdir with a missing parent should fail
        // Neither parent nor child dir should be created

        var childDir = storage.mkdir("test_dir/child", false, dataContext);
        waitFor(TEST_TIMEOUT, childDir);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(childDir));

        var dirExists = storage.exists("test_dir", dataContext);
        var childExists = storage.exists("test_dir/child", dataContext);
        waitFor(TEST_TIMEOUT, dirExists, childExists);

        Assertions.assertFalse(getResultOf(dirExists));
        Assertions.assertFalse(getResultOf(childExists));
    }

    @Test
    void testMkdir_recursiveOk() throws Exception {

        // mkdir, recursive = true, create parent and child dir in a single call

        var mkdir = storage.mkdir("test_dir/child", true, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        Assertions.assertDoesNotThrow(() -> getResultOf(mkdir));

        var dirExists = storage.exists("test_dir", dataContext);
        var childExists = storage.exists("test_dir/child", dataContext);
        waitFor(TEST_TIMEOUT, dirExists, childExists);

        Assertions.assertTrue(getResultOf(dirExists));
        Assertions.assertTrue(getResultOf(childExists));
    }

    @Test
    void testMkdir_recursiveDirExists() throws Exception {

        // mkdir, when recursive = true it is not an error if the target dir already exists

        var prepare = storage.mkdir("test_dir/child", true, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var mkdir = storage.mkdir("test_dir/child", true, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        Assertions.assertDoesNotThrow(() -> getResultOf(mkdir));

        var dirExists = storage.exists("test_dir", dataContext);
        var childExists = storage.exists("test_dir/child", dataContext);
        waitFor(TEST_TIMEOUT, dirExists, childExists);

        Assertions.assertTrue(getResultOf(dirExists));
        Assertions.assertTrue(getResultOf(childExists));
    }

    @Test
    void testMkdir_recursiveFileExists() {

        // mkdir should always fail if requested dir already exists and is a file

        var prepare = storage
                .mkdir("test_dir", false, dataContext)
                .thenCompose(x -> makeSmallFile("test_dir/child", storage, dataContext));

        waitFor(TEST_TIMEOUT, prepare);

        var mkdir = storage.mkdir("test_dir/child", false, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(mkdir));
    }

    @Test
    void testMkdir_badPaths() {

        testBadPaths((storagePath, execCtx)  -> storage.mkdir(storagePath, false, execCtx));
        testBadPaths((storagePath, execCtx)  -> storage.mkdir(storagePath, true, execCtx));
    }

    @Test
    void testMkdir_storageRoot() {

        failForStorageRoot((storagePath, execCtx) -> storage.mkdir(storagePath, false, execCtx));
        failForStorageRoot((storagePath, execCtx)  -> storage.mkdir(storagePath, true, execCtx));
    }

    @Test
    void testMkdir_unicode() throws Exception {

        var mkdir = storage.mkdir("你好/你好", true, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        Assertions.assertDoesNotThrow(() -> getResultOf(mkdir));

        var dirExists = storage.exists("你好", dataContext);
        var childExists = storage.exists("你好/你好", dataContext);
        waitFor(TEST_TIMEOUT, dirExists, childExists);

        Assertions.assertTrue(getResultOf(dirExists));
        Assertions.assertTrue(getResultOf(childExists));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // RM
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testRm_ok() throws Exception {

        // Simplest case - create one file and delete it

        var prepare = makeSmallFile("test_file.txt", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var rm = storage.rm("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, rm);

        Assertions.assertDoesNotThrow(() -> getResultOf(rm));

        // File should be gone

        var exists = storage.exists("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertFalse(getResultOf(exists));
    }

    @Test
    void testRm_inSubdirOk() throws Exception {

        // Simplest case - create one file and delete it

        var prepare = makeSmallFile("sub_dir/test_file.txt", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var rm = storage.rm("sub_dir/test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, rm);

        Assertions.assertDoesNotThrow(() -> getResultOf(rm));

        // File should be gone

        var exists = storage.exists("sub_dir/test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertFalse(getResultOf(exists));
    }

    @Test
    void testRm_onDir() throws Exception {

        // Calling rm on a directory is a bad request, even if the dir is empty

        var prepare = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var rm = storage.rm("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, rm);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(rm));

        // Dir should still exist because rm has failed

        var exists = storage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertTrue(getResultOf(exists));
    }

    @Test
    void testRm_missing() {

        // Try to delete a path that does not exist

        var rm = storage.rm("missing_path.dat", dataContext);
        waitFor(TEST_TIMEOUT, rm);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(rm));
    }

    @Test
    void testRm_badPaths() {

        testBadPaths(storage::rm);
    }

    @Test
    void testRm_storageRoot() {

        failForStorageRoot(storage::rm);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // RMDIR
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testRmdir_ok() throws Exception {

        var prepare = storage.mkdir("test_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var exists1 = storage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists1);
        Assertions.assertTrue(getResultOf(exists1));

        var rmdir = storage.rmdir("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, rmdir);
        Assertions.assertDoesNotThrow(() -> getResultOf(rmdir));

        var exists2 = storage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists2);
        Assertions.assertFalse(getResultOf(exists2));
    }

    @Test
    void testRmdir_byPrefix() throws Exception {

        var prepare = storage.mkdir("test_dir/sub_dir", true, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var exists1 = storage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists1);
        Assertions.assertTrue(getResultOf(exists1));

        var rmdir = storage.rmdir("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, rmdir);
        Assertions.assertDoesNotThrow(() -> getResultOf(rmdir));

        var exists2 = storage.exists("test_dir", dataContext);
        waitFor(TEST_TIMEOUT, exists2);
        Assertions.assertFalse(getResultOf(exists2));
    }

    @Test
    void testRmdir_withContent() throws Exception {

        // Delete one whole dir tree
        // Sibling dir tree should be unaffected

        var prepare = storage
                .mkdir("test_dir/child_1", true, dataContext)
                .thenCompose(x -> storage.mkdir("test_dir/child_1/sub", false, dataContext))
                .thenCompose(x -> makeSmallFile("test_dir/child_1/file_a.txt", storage, dataContext))
                .thenCompose(x -> makeSmallFile("test_dir/child_1/file_b.txt", storage, dataContext))
                .thenCompose(x -> storage.mkdir("test_dir/child_2", true, dataContext))
                .thenCompose(x -> makeSmallFile("test_dir/child_2/file_a.txt", storage, dataContext));

        waitFor(TEST_TIMEOUT.multipliedBy(2), prepare);  // Allow extra time for multiple operations

        var rmdir = storage.rmdir("test_dir/child_1", dataContext);
        waitFor(TEST_TIMEOUT, rmdir);
        Assertions.assertDoesNotThrow(() -> getResultOf(rmdir));

        var exists1 = storage.exists("test_dir/child_1", dataContext);
        var exists2 = storage.exists("test_dir/child_2", dataContext);
        var size2a = storage.size("test_dir/child_2/file_a.txt", dataContext);
        waitFor(TEST_TIMEOUT, exists1, exists2, size2a);

        Assertions.assertFalse(getResultOf(exists1));
        Assertions.assertTrue(getResultOf(exists2));
        Assertions.assertTrue(getResultOf(size2a) > 0);
    }

    @Test
    void testRmdir_onFile() throws Exception {

        // Calling rmdir on a file is a bad request

        var prepare = makeSmallFile("test_file.txt", storage, dataContext);
        waitFor(TEST_TIMEOUT, prepare);

        var rmdir = storage.rmdir("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, rmdir);
        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(rmdir));

        // File should still exist because rm has failed

        var exists = storage.exists("test_file.txt", dataContext);
        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertTrue(getResultOf(exists));
    }

    @Test
    void testRmdir_Missing() {

        // Try to delete a path that does not exist

        var rmdir = storage.rmdir("missing_path", dataContext);
        waitFor(TEST_TIMEOUT, rmdir);
        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(rmdir));
    }

    @Test
    void testRmdir_badPaths() {

        testBadPaths(storage::rmdir);
    }

    @Test
    void testRmdir_storageRoot() {

        failForStorageRoot(storage::rmdir);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // READ CHUNK
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testReadChunk_ok() throws Exception {

        var fileSize = 10 * 1024;
        var bytes = new byte[fileSize];
        var random = new Random();
        random.nextBytes(bytes);

        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());
        var prepare = makeFile("test_file.dat", content, storage, dataContext);

        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var readChunk = storage.readChunk("test_file.dat", 4096, 4096, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);

        try (var chunk = getResultOf(readChunk)) {

            var expectedBytes = Arrays.copyOfRange(bytes, 4096, 8192);
            var chunkBytes = new byte[4096];
            chunk.readBytes(chunkBytes);

            Assertions.assertArrayEquals(expectedBytes, chunkBytes);
        }
    }

    @Test
    void testReadChunk_first() throws Exception {

        var fileSize = 10 * 1024;
        var bytes = new byte[fileSize];
        var random = new Random();
        random.nextBytes(bytes);

        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());
        var prepare = makeFile("test_file.dat", content, storage, dataContext);

        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var readChunk = storage.readChunk("test_file.dat", 0, 4096, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);

        try (var chunk = getResultOf(readChunk)) {

            var expectedBytes = Arrays.copyOfRange(bytes, 0, 4096);
            var chunkBytes = new byte[4096];
            chunk.readBytes(chunkBytes);

            Assertions.assertArrayEquals(expectedBytes, chunkBytes);
        }
    }

    @Test
    void testReadChunk_last() throws Exception {

        var fileSize = 10 * 1024;
        var bytes = new byte[fileSize];
        var random = new Random();
        random.nextBytes(bytes);

        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());
        var prepare = makeFile("test_file.dat", content, storage, dataContext);

        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var readChunk = storage.readChunk("test_file.dat", 8192, 2048, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);

        try (var chunk = getResultOf(readChunk)) {

            var expectedBytes = Arrays.copyOfRange(bytes, 8192, 10240);
            var chunkBytes = new byte[2048];
            chunk.readBytes(chunkBytes);

            Assertions.assertArrayEquals(expectedBytes, chunkBytes);
        }
    }

    @Test
    void testReadChunk_all() throws Exception {

        var fileSize = 10 * 1024;
        var bytes = new byte[fileSize];
        var random = new Random();
        random.nextBytes(bytes);

        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());
        var prepare = makeFile("test_file.dat", content, storage, dataContext);

        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var readChunk = storage.readChunk("test_file.dat", 0, 10240, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);

        try (var chunk = getResultOf(readChunk)) {

            var expectedBytes = Arrays.copyOfRange(bytes, 0, 10240);
            var chunkBytes = new byte[10240];
            chunk.readBytes(chunkBytes);

            Assertions.assertArrayEquals(expectedBytes, chunkBytes);
        }
    }

    @Test
    void testReadChunk_badSize() throws Exception {

        var fileSize = 10 * 1024;
        var bytes = new byte[fileSize];
        var random = new Random();
        random.nextBytes(bytes);

        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());
        var prepare = makeFile("test_file.dat", content, storage, dataContext);

        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var readChunk = storage.readChunk("test_file.dat", 1024, 10240, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(readChunk));

        var readChunk2 = storage.readChunk("test_file.dat", 10241, 1024, dataContext);
        waitFor(TEST_TIMEOUT, readChunk2);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(readChunk2));
    }

    @Test
    void testReadChunk_onDir() throws Exception {

        // Calling readChunk on a directory is a storage error

        var prepare = storage.mkdir("test_file.dat", false, dataContext);
        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        var readChunk = storage.readChunk("test_file.dat", 0, 1024, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);

        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(readChunk));
    }

    @Test
    void testReadChunk_missing() {

        // Calling readChunk on a missing file is a storage error

        var readChunk = storage.readChunk("missing_file.dat", 0, 1024, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);
        Assertions.assertThrows(EStorageRequest.class, () -> getResultOf(readChunk));
    }

    @Test
    void testReadChunk_invalidParams() throws Exception {

        var fileSize = 10 * 1024;
        var bytes = new byte[fileSize];
        var random = new Random();
        random.nextBytes(bytes);

        var content = Bytes.copyToBuffer(bytes, dataContext.arrowAllocator());
        var prepare = makeFile("test_file.dat", content, storage, dataContext);

        waitFor(TEST_TIMEOUT, prepare);
        getResultOf(prepare);

        // Zero size (empty chunk)

        var readChunk = storage.readChunk("test_file.dat", 1024, 0, dataContext);
        waitFor(TEST_TIMEOUT, readChunk);
        Assertions.assertThrows(EValidationGap.class, () -> getResultOf(readChunk));

        // Negative size

        var readChunk3 = storage.readChunk("test_file.dat", 0, -1024, dataContext);
        waitFor(TEST_TIMEOUT, readChunk3);
        Assertions.assertThrows(EValidationGap.class, () -> getResultOf(readChunk3));

        // Negative offset

        var readChunk2 = storage.readChunk("test_file.dat", -1, 1024, dataContext);
        waitFor(TEST_TIMEOUT, readChunk2);
        Assertions.assertThrows(EValidationGap.class, () -> getResultOf(readChunk2));
    }

    @Test
    void testReadChunk_badPaths() {

        testBadPaths((path, ctx) -> storage.readChunk(path, 0, 1024, dataContext));
    }

    @Test
    void testReadChunk_storageRoot() {

        failForStorageRoot((path, ctx) -> storage.readChunk(path, 0, 1024, dataContext));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // COMMON TESTS (tests applied to several storage calls)
    // -----------------------------------------------------------------------------------------------------------------


    <T> void failForStorageRoot(BiFunction<String, IExecutionContext, CompletionStage<T>> testMethod) {

        // TRAC should not allow write-operations with path "." to reach the storage layer
        // Storage implementations should report this as a validation gap

        var storageRootResult = testMethod.apply(".", dataContext);

        waitFor(TEST_TIMEOUT, storageRootResult);

        Assertions.assertThrows(EValidationGap.class, () -> getResultOf(storageRootResult));
    }

    <T> void testBadPaths(BiFunction<String, IExecutionContext, CompletionStage<T>> testMethod) {

        var escapingPathResult = testMethod.apply("../", dataContext);

        var absolutePathResult = OS.WINDOWS.isCurrentOs()
                ? testMethod.apply("C:\\Windows", dataContext)
                : testMethod.apply("/bin", dataContext);

        // \0 and / are the two characters that are always illegal in posix filenames
        // But / will be interpreted as a separator
        // There are several illegal characters for filenames on Windows!

        var invalidPathResult = OS.WINDOWS.isCurrentOs()
                ? testMethod.apply("@$ N'`$>.)_\"+\n%", dataContext)
                : testMethod.apply("nul\0char", dataContext);

        waitFor(TEST_TIMEOUT,
            escapingPathResult,
            absolutePathResult,
            invalidPathResult);

        Assertions.assertThrows(ETracInternal.class, () -> getResultOf(escapingPathResult));
        Assertions.assertThrows(ETracInternal.class, () -> getResultOf(absolutePathResult));
        Assertions.assertThrows(ETracInternal.class, () -> getResultOf(invalidPathResult));
    }

}
