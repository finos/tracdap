/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.storage;

import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.storage.local.LocalFileStorage;
import com.accenture.trac.common.util.Concurrent;

import io.netty.buffer.*;
import io.netty.util.concurrent.UnorderedThreadPoolEventExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static com.accenture.trac.common.storage.StorageTestHelpers.*;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


public class StorageTestSuite_FileReadWrite {

    /* >>> Generic tests for IFileStorage - read/write operations

    These tests are implemented purely in terms of the IFileStorage interface. E.g. to test the "exists" method,
    a directory is created using IFileStorage.exists(). This test suite can be run for any storage implementation.
    Valid storage implementations must pass this test suite.

    Storage implementations may also wish to supply their own tests that use native APIs to set up and control
    tests. This can allow for finer grained control, particularly when testing corner cases and error conditions.
     */

    public static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    IFileStorage storage;
    IExecutionContext execContext;

    @TempDir
    Path storageDir;

    @BeforeEach
    void setupStorage() {

        // TODO: Abstract mechanism for obtaining storage impl using config

        storage = new LocalFileStorage("TEST_STORAGE", storageDir.toString());
        execContext = () -> new UnorderedThreadPoolEventExecutor(1);
    }

    @Test
    void roundTrip_ok() throws Exception {

        var storagePath = "haiku.txt";

        var haiku =
                "The data goes in;\n" +
                "For a short while it persists,\n" +
                "then returns unscathed!";

        var original = ByteBufUtil.encodeString(
                ByteBufAllocator.DEFAULT,
                CharBuffer.wrap(haiku),
                StandardCharsets.UTF_8);

        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, execContext);
        Concurrent.publish(Stream.of(original)).subscribe(writer);

        waitFor(TEST_TIMEOUT, writeSignal);

        // Make sure the write operation did not report an error before trying to read
        Assertions.assertDoesNotThrow(() -> result(writeSignal));

        var reader = storage.reader(storagePath, execContext);
        var readResult = Concurrent.fold(
                reader, Unpooled::wrappedBuffer,
                (ByteBuf) new EmptyByteBuf(ByteBufAllocator.DEFAULT));

        waitFor(TEST_TIMEOUT, readResult);

        var roundTrip = result(readResult);
        var roundTripHaiku = roundTrip.readCharSequence(
                roundTrip.readableBytes(),
                StandardCharsets.UTF_8);

        Assertions.assertEquals(haiku, roundTripHaiku);
    }

    @Test
    void roundTrip_large() {

        Assertions.fail();
    }

    @Test
    void roundTrip_empty() {

        Assertions.fail();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Functional error cases
    // -----------------------------------------------------------------------------------------------------------------

    // Functional error tests can be set up and verified entirely using the storage API
    // All back ends should behave consistently for these tests

    @Test
    void testWrite_missingDir() {

        Assertions.fail();
    }

    @Test
    void testWrite_alreadyExists() {

        Assertions.fail();
    }

    @Test
    void roundWrite_badPaths() {

        Assertions.fail();
    }

    @Test
    void testWrite_storageRoot() {

        Assertions.fail();
    }

    @Test
    void testWrite_outsideRoot() {

        Assertions.fail();
    }

    @Test
    void testRead_missing() {

        Assertions.fail();
    }

    @Test
    void testRead_badPaths() {

        Assertions.fail();
    }

    @Test
    void testRead_storageRoot() {

        Assertions.fail();
    }

    @Test
    void testRead_outsideRoot() {

        Assertions.fail();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Interrupted operations
    // -----------------------------------------------------------------------------------------------------------------

    /*
        Simulate error conditions in client code that interrupt read/write operations
        Try to check that errors are reported correctly and resources are cleaned up
        That means all handles/streams/locks are closed and for write operation partially written files are removed
        It is difficult to verify this using the abstracted storage API!
        These tests look for common symptoms of resource leaks that may catch some common errors

        Using the storage API it is not possible to simulate errors that occur in the storage back end
        E.g. loss of connection to a storage service or disk full during a write operation

        Individual storage backends should implement tests using native calls to set up and verify tests
        This will allow testing error conditions in the storage back end,
        and more thorough validation of error handling behavior for error conditions in client code
     */

    @Test
    void testWrite_notStarted() {

        Assertions.fail();
    }

    @Test
    void testWrite_failImmediately() {

        Assertions.fail();
    }

    @Test
    void testWrite_failAfterWriting() {

        Assertions.fail();
    }

    @Test
    void testWrite_failAndRetry() {

        Assertions.fail();
    }

    @Test
    void testRead_notStarted() {

        Assertions.fail();
    }

    @Test
    void testRead_cancelAndRetry() {

        Assertions.fail();
    }

    @Test
    void testRead_cancelAndDelete() {

        Assertions.fail();
    }

}
