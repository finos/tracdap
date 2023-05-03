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
import org.finos.tracdap.common.exception.EStorageRequest;
import org.finos.tracdap.common.exception.EValidationGap;
import org.finos.tracdap.common.concurrent.Flows;

import io.netty.buffer.*;
import io.netty.util.ReferenceCountUtil;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.OS;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.resultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.finos.tracdap.test.storage.StorageTestHelpers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public abstract class StorageReadWriteTestSuite {

    /* >>> Test suite for IFileStorage - read/write operations, functional and stability tests

    These tests are implemented purely in terms of the IFileStorage interface. The test suite can be run for
    any storage implementation and a valid storage implementations must pass this test suite.

    NOTE: To test a new storage implementation, setupStorage() must be replaced
    with a method to provide a storage implementation based on a supplied test config.

    Storage implementations may also wish to supply their own tests that use native APIs to set up and control
    tests. This can allow for finer grained control, particularly when testing corner cases and error conditions.
     */

    // Unit test implementation for local storage is in LocalStorageReadWriteTest

    public static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration ASYNC_DELAY = Duration.ofMillis(100);

    protected IFileStorage storage;
    protected IExecutionContext execContext;
    protected IDataContext dataContext;


    // -----------------------------------------------------------------------------------------------------------------
    // Basic round trip
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void roundTrip_basic() throws Exception {

        var storagePath = "haiku.txt";

        var haiku =
                "The data goes in;\n" +
                "For a short while it persists,\n" +
                "then returns unscathed!";

        var haikuBytes = haiku.getBytes(StandardCharsets.UTF_8);

        roundTripTest(storagePath, List.of(haikuBytes), storage, dataContext);
    }

    @Test
    void roundTrip_large() throws Exception {

        var storagePath = "test_file.dat";

        var bytes = new byte[10 * 1024 * 1024];  // One 10 M chunk

        var random = new Random();
        random.nextBytes(bytes);

        StorageReadWriteTestSuite.roundTripTest(
                storagePath, List.of(bytes),
                storage, dataContext);
    }

    @Test
    void roundTrip_heterogeneous() throws Exception {

        var storagePath = "test_file.dat";

        var bytes = List.of(  // Selection of different size chunks
                new byte[3],
                new byte[10000],
                new byte[42],
                new byte[4097],
                new byte[1],
                new byte[2000]);

        var random = new Random();
        bytes.forEach(random::nextBytes);

        StorageReadWriteTestSuite.roundTripTest(
                storagePath, bytes,
                storage, dataContext);
    }

    @Test
    void roundTrip_empty() throws Exception {

        var storagePath = "test_file.dat";
        var emptyBytes = new byte[0];

        StorageReadWriteTestSuite.roundTripTest(
                storagePath, List.of(emptyBytes),
                storage, dataContext);
    }

    @Test
    void roundTrip_unicode() throws Exception {

        var anOdeToTheGoose =
            "鹅、鹅、鹅，\n" +
            "曲项向天歌。\n" +
            "白毛浮绿水，\n" +
            "红掌拨清波";

        var storagePath = "咏鹅.txt";
        var storageBytes = anOdeToTheGoose.getBytes(StandardCharsets.UTF_8);

        StorageReadWriteTestSuite.roundTripTest(
                storagePath, List.of(storageBytes),
                storage, dataContext);
    }

    static void roundTripTest(
            String storagePath, List<byte[]> originalBytes,
            IFileStorage storage, IDataContext dataContext) throws Exception {

        var originalBuffers = originalBytes.stream().map(bytes ->
                ByteBufAllocator.DEFAULT
                .directBuffer(bytes.length)
                .writeBytes(bytes));

        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, dataContext);
        Flows.publish(originalBuffers).subscribe(writer);

        waitFor(Duration.ofHours(1), writeSignal);

        // Make sure the write operation did not report an error before trying to read
        Assertions.assertDoesNotThrow(() -> resultOf(writeSignal));

        var reader = storage.reader(storagePath, dataContext);
        var readResult = Flows.fold(
                reader, (composite, buf) -> composite.addComponent(true, buf),
                ByteBufAllocator.DEFAULT.compositeBuffer());

        waitFor(TEST_TIMEOUT, readResult);

        var roundTripBuffer = resultOf(readResult);
        var roundTripContent = copyBytes(roundTripBuffer);

        // Create a new ByteBuf for the original data
        // The writer will have released the buffers it received
        var originalBuffer = Unpooled.wrappedBuffer(originalBytes.toArray(byte[][]::new));
        var originalContent = copyBytes(originalBuffer);

        Assertions.assertArrayEquals(originalContent, roundTripContent);

        originalBuffer.release();
        roundTripBuffer.release();
    }

    private static byte[] copyBytes(ByteBuf buf) {

        byte[] bs = new byte[buf.readableBytes()];
        buf.readBytes(bs);
        return bs;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Functional error cases
    // -----------------------------------------------------------------------------------------------------------------

    // Functional error tests can be set up and verified entirely using the storage API
    // All back ends should behave consistently for these tests

    // Errors are checked using resultOf(), which unwraps stream state errors
    // This exposes the functional cause of errors in the stream, which will be EStorage (or ETrac) errors

    @Test
    void testWrite_missingDir() throws Exception {

        // Basic test without creating the parent dir first
        // Should be automatically created by the writer

        var storagePath = "parent_dir/haiku.txt";

        var haiku =
                "The data goes in;\n" +
                "For a short while it persists,\n" +
                "then returns unscathed!";

        var haikuBytes = haiku.getBytes(StandardCharsets.UTF_8);

        roundTripTest(storagePath, List.of(haikuBytes), storage, dataContext);
    }

    @Test
    void testWrite_fileAlreadyExists() throws Exception {

        //  Writing a file always overwrites any existing content
        // This is in line with cloud bucket semantics

        var storagePath = "some_file.txt";

        var prepare = makeSmallFile(storagePath, storage, execContext);
        waitFor(TEST_TIMEOUT, prepare);

        var exists1 = storage.exists(storagePath, execContext);
        waitFor(TEST_TIMEOUT, exists1);
        var exists1Result = resultOf(exists1);
        Assertions.assertTrue(exists1Result);

        var newContent = "small";
        var newContentBytes = newContent.getBytes(StandardCharsets.UTF_8);

        roundTripTest(storagePath, List.of(newContentBytes), storage, dataContext);
    }

    @Test
    void testWrite_dirAlreadyExists() throws Exception {

        // File storage should not allow a file to be written if a dir exists with the same name
        // TRAC prohibits this even though it is allowed in pure bucket semantics

        var storagePath = "some_file.txt";

        var prepare = storage.mkdir(storagePath, false, execContext);
        waitFor(TEST_TIMEOUT, prepare);

        var exists1 = storage.exists(storagePath, execContext);
        waitFor(TEST_TIMEOUT, exists1);
        var exists1Result = resultOf(exists1);
        Assertions.assertTrue(exists1Result);

        var content = ByteBufUtil.encodeString(
                ByteBufAllocator.DEFAULT,
                CharBuffer.wrap("Some content"),
                StandardCharsets.UTF_8);

        var contentStream = Flows.publish(Stream.of(content));
        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, dataContext);
        contentStream.subscribe(writer);

        waitFor(TEST_TIMEOUT, writeSignal);

        var exists = storage.exists(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, exists);

        Assertions.assertTrue(resultOf(exists));

        var contentStream2 = Flows.publish(Stream.of(content));
        var writeSignal2 = new CompletableFuture<Long>();
        var writer2 = storage.writer(storagePath, writeSignal2, dataContext);

        Assertions.assertThrows(EStorageRequest.class, () -> {

            contentStream2.subscribe(writer2);
            waitFor(TEST_TIMEOUT, writeSignal2);
            resultOf(writeSignal2);
        });
    }

    @Test
    void testWrite_badPaths() {

        var absolutePath = OS.WINDOWS.isCurrentOs()
                ? "C:\\Temp\\blah.txt"
                : "/tmp/blah.txt";

        // \0 and / are the two characters that are always illegal in posix filenames
        // But / will be interpreted as a separator
        // There are several illegal characters for filenames on Windows!

        var invalidPath = OS.WINDOWS.isCurrentOs()
                ? "£$ N'`¬$£>.)_£\"+\n%"
                : "nul\0char";

        var writeSignal1 = new CompletableFuture<Long>();
        Assertions.assertThrows(EValidationGap.class, () ->
                storage.writer(absolutePath, writeSignal1, dataContext));

        var writeSignal2 = new CompletableFuture<Long>();
        Assertions.assertThrows(EValidationGap.class, () ->
                storage.writer(invalidPath, writeSignal2, dataContext));
    }

    @Test
    void testWrite_storageRoot() {

        var storagePath = ".";

        var writeSignal = new CompletableFuture<Long>();

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.writer(storagePath, writeSignal, dataContext));
    }

    @Test
    void testWrite_outsideRoot() {

        var storagePath = "../any_file.txt";

        var writeSignal = new CompletableFuture<Long>();

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.writer(storagePath, writeSignal, dataContext));
    }

    @Test
    void testRead_missing() {

        var storagePath = "missing_file.txt";

        var reader = storage.reader(storagePath, dataContext);

        Assertions.assertThrows(EStorageRequest.class, () -> {

            var readResult = Flows.fold(
                    reader, (composite, buf) -> composite.addComponent(true, buf),
                    ByteBufAllocator.DEFAULT.compositeBuffer());

            waitFor(TEST_TIMEOUT, readResult);
            resultOf(readResult);
        });
    }

    @Test
    void testRead_badPaths() {

        var absolutePath = OS.WINDOWS.isCurrentOs()
                ? "C:\\Temp\\blah.txt"
                : "/tmp/blah.txt";

        // \0 and / are the two characters that are always illegal in posix filenames
        // But / will be interpreted as a separator
        // There are several illegal characters for filenames on Windows!

        var invalidPath = OS.WINDOWS.isCurrentOs()
                ? "£$ N'`¬$£>.)_£\"+\n%"
                : "nul\0char";

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.reader(absolutePath, dataContext));

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.reader(invalidPath, dataContext));
    }

    @Test
    void testRead_storageRoot() {

        var storagePath = ".";

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.reader(storagePath, dataContext));
    }

    @Test
    void testRead_outsideRoot() {

        var storagePath = "../some_file.txt";

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.reader(storagePath, dataContext));
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

        Error states in the streams are checked directly by mocking/spying on the Java Flow API
        E.g. Illegal state and cancellation exceptions are checked explicitly
        This is different from the functional tests, which unwrap stream state errors to look for EStorage errors

        Using the storage API it is not possible to simulate errors that occur in the storage back end
        E.g. loss of connection to a storage service or disk full during a write operation

        Individual storage backends should implement tests using native calls to set up and verify tests
        This will allow testing error conditions in the storage back end,
        and more thorough validation of error handling behavior for error conditions in client code
     */

    @Test
    void testWrite_chunksReleased() throws Exception {

        // Writer takes ownership of chunks when it receives them
        // This test makes sure they are being released after they have been written

        var storagePath = "test_file.dat";

        var bytes = List.of(
                new byte[3],
                new byte[10000],
                new byte[42],
                new byte[4097],
                new byte[1],
                new byte[2000]);

        var random = new Random();
        bytes.forEach(random::nextBytes);

        var chunks = bytes.stream().map(bs ->
                ByteBufAllocator.DEFAULT
                .directBuffer(bs.length)
                .writeBytes(bs))
                .collect(Collectors.toList());

        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, dataContext);
        Flows.publish(chunks).subscribe(writer);

        waitFor(TEST_TIMEOUT, writeSignal);

        Assertions.assertDoesNotThrow(() -> resultOf(writeSignal));

        // Allow time for background cleanup
        Thread.sleep(ASYNC_DELAY.toMillis());

        for (var chunk : chunks)
            Assertions.assertEquals(0, chunk.refCnt());
    }

    @Test
    void testWrite_subscribeNever() throws Exception {

        // Set up a dir in storage

        var mkdir = storage.mkdir("some_dir", false, dataContext);
        waitFor(TEST_TIMEOUT, mkdir);

        var dirExists = storage.exists("some_dir", dataContext);
        waitFor(TEST_TIMEOUT, dirExists);
        Assertions.assertTrue(resultOf(dirExists));

        // Prepare a writer to write a file inside the new dir

        var storagePath = "some_dir/some_file.txt";
        var writeSignal = new CompletableFuture<Long>();
        storage.writer(storagePath, writeSignal, dataContext);

        // Allow some time in case the writer is going to do anything
        Thread.sleep(ASYNC_DELAY.toMillis());

        // File should not have been created as onSubscribe has not been called
        var fileExists = storage.exists(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, fileExists);
        Assertions.assertFalse(resultOf(fileExists));
    }

    @Test
    void testWrite_subscribeTwice() throws Exception {

        var storagePath = "test_file.dat";

        var bytes = new byte[10000];
        new Random().nextBytes(bytes);
        var chunk = Unpooled.wrappedBuffer(bytes);

        var writerSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writerSignal, dataContext);

        // First subscription to the writer, everything should proceed normally

        var subscription1 = mock(Flow.Subscription.class);
        writer.onSubscribe(subscription1);
        verify(subscription1, timeout(TEST_TIMEOUT.toMillis())).request(anyLong());

        // Second subscription to the writer, should throw illegal state

        var subscription2 = mock(Flow.Subscription.class);
        Assertions.assertThrows(IllegalStateException.class, () -> writer.onSubscribe(subscription2));

        // First subscription should be unaffected, write operation should complete normally on subscription 1

        writer.onNext(chunk);
        writer.onComplete();
        waitFor(TEST_TIMEOUT, writerSignal);
        Assertions.assertDoesNotThrow(() -> resultOf(writerSignal));

        // File should now be visible in storage
        var size = storage.size(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, size);
        Assertions.assertEquals(bytes.length, resultOf(size));
    }

    @Test
    void testWrite_errorImmediately() throws Exception {

        var storagePath = "test_file.dat";

        var writerSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writerSignal, dataContext);

        // Subscribe to the writer, it should call subscription.request()
        // Request call may be immediate or async

        var subscription = mock(Flow.Subscription.class);
        writer.onSubscribe(subscription);

        verify(subscription, timeout(TEST_TIMEOUT.toMillis())).request(anyLong());

        // Send an error without calling onNext
        // The writer should clean up and notify failure using the writer signal

        writer.onError(new TestException());
        waitFor(TEST_TIMEOUT, writerSignal);

        // After onError, writer should not have called cancel() (this would be redundant)
        verify(subscription, never()).cancel();

        // For errors received externally via onError(),
        // The writer should wrap the error with a completion error and use the wrapped error as the result signal
        Assertions.assertThrows(CompletionException.class, () -> resultOf(writerSignal, false));

        // The wrapped exception should be what was received in onError
        try {
            resultOf(writerSignal, false);
        }
        catch (CompletionException e) {
            Assertions.assertTrue(e.getCause() instanceof TestException);
        }

        // If there is a partially written file,
        // the writer should remove it as part of the error cleanup

        var exists = storage.exists(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, exists);

        Assertions.assertFalse(resultOf(exists));
    }

    @Test
    void testWrite_errorAfterChunk() throws Exception {

        var storagePath = "test_file.dat";

        var bytes0 = new byte[10000];
        new Random().nextBytes(bytes0);
        var chunk0 = Unpooled.wrappedBuffer(bytes0);

        var writerSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writerSignal, dataContext);

        // Subscribe to the writer, it should call subscription.request()
        // Request call may be immediate or async

        var subscription = mock(Flow.Subscription.class);
        writer.onSubscribe(subscription);

        verify(subscription, timeout(TEST_TIMEOUT.toMillis())).request(anyLong());

        // Send one chunk to the writer

        writer.onNext(chunk0);
        Thread.sleep(ASYNC_DELAY.toMillis());

        // Send an error
        // The writer should clean up and notify failure using the writer signal

        writer.onError(new TestException());
        waitFor(TEST_TIMEOUT, writerSignal);

        // After onError, writer should not have called cancel() (this would be redundant)
        verify(subscription, never()).cancel();

        // For errors received externally via onError(),
        // The writer should wrap the error with a completion error and use the wrapped error as the result signal
        Assertions.assertThrows(CompletionException.class, () -> resultOf(writerSignal, false));

        // The wrapped exception should be what was received in onError
        try {
            resultOf(writerSignal, false);
        }
        catch (CompletionException e) {
            Assertions.assertTrue(e.getCause() instanceof TestException);
        }

        // If there is a partially written file,
        // the writer should remove it as part of the error cleanup

        var exists = storage.exists(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, exists);

        Assertions.assertFalse(resultOf(exists));
    }

    @Test
    void testWrite_errorThenRetry() throws Exception {

        var storagePath = "test_file.dat";
        var dataSize = 10000;

        var bytes = new byte[dataSize];
        new Random().nextBytes(bytes);

        // Set up a writer and send it a chunk
        var writerSignal1 = new CompletableFuture<Long>();
        var writer1 = storage.writer(storagePath, writerSignal1, dataContext);
        var subscription1 = mock(Flow.Subscription.class);
        var chunk1 = Unpooled.wrappedBuffer(bytes);

        writer1.onSubscribe(subscription1);
        verify(subscription1, timeout(TEST_TIMEOUT.toMillis())).request(anyLong());
        writer1.onNext(chunk1);

        // Give the writer some time for the chunk to be written to disk
        Thread.sleep(ASYNC_DELAY.toMillis());

        // Send the onError() message and make sure the writer signal reports the failure
        writer1.onError(new TestException());
        waitFor(TEST_TIMEOUT, writerSignal1);
        Assertions.assertThrows(CompletionException.class, () -> resultOf(writerSignal1, false));

        // File should not exist in storage after an aborted write
        var exists1 = storage.exists(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, exists1);
        Assertions.assertFalse(resultOf(exists1));

        // Set up a second writer to retry the same operation
        // This time, send the chunk and an onComplete() message
        var writerSignal2 = new CompletableFuture<Long>();
        var writer2 = storage.writer(storagePath, writerSignal2, dataContext);
        var subscription2 = mock(Flow.Subscription.class);
        var chunk2 = Unpooled.wrappedBuffer(bytes);

        writer2.onSubscribe(subscription2);
        verify(subscription2, timeout(TEST_TIMEOUT.toMillis())).request(anyLong());
        writer2.onNext(chunk2);
        writer2.onComplete();

        // Wait for the second operation, which should succeed
        waitFor(TEST_TIMEOUT, writerSignal2);
        Assertions.assertDoesNotThrow(() -> resultOf(writerSignal2));

        // File should now be visible in storage
        var size2 = storage.size(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, size2);
        Assertions.assertEquals(dataSize, resultOf(size2));
    }

    @Test
    void testRead_requestUpfront() {

        var storagePath = "some_file.dat";

        // Create a file big enough that it needs many chunks to read

        var originalBytes = new byte[10 * 1024 * 1024];
        var originalContent = ByteBufAllocator.DEFAULT.directBuffer(10000);

        var random = new Random();
        random.nextBytes(originalBytes);
        originalContent.writeBytes(originalBytes);

        var write = makeFile(storagePath, originalContent, storage, dataContext);
        waitFor(TEST_TIMEOUT, write);

        // request a million chunks - way more than needed
        // Actual chunks received will depend on the storage implementation
        // These are intended as some common sense bounds

        var CHUNKS_TO_REQUEST = 1000000;
        var MIN_CHUNKS_EXPECTED = 10;          // implies a min chunk size of 1 MiB
        var MAX_CHUNKS_EXPECTED = 40 * 1024;   // implies a max chunk size of 256 B

        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        AtomicLong bytesRead = new AtomicLong(0);

        // Request all the chunks in one go as soon as onSubscribe is received
        doAnswer(invocation -> {
            Flow.Subscription subscription = invocation.getArgument(0);
            subscription.request(CHUNKS_TO_REQUEST);
            return null;
        }).when(subscriber).onSubscribe(any(Flow.Subscription.class));

        // Count up the bytes received and release buffers
        doAnswer(invocation -> {
            ByteBuf chunk = invocation.getArgument(0);
            bytesRead.addAndGet(chunk.readableBytes());
            chunk.release();
            return null;
        }).when(subscriber).onNext(any(ByteBuf.class));

        // Create a reader and read using the mocked subscriber

        var reader = storage.reader(storagePath, dataContext);
        reader.subscribe(subscriber);

        // onSubscribe should be received
        verify(subscriber, timeout(TEST_TIMEOUT.toMillis())).onSubscribe(any(Flow.Subscription.class));

        // The stream should be read until complete
        verify(subscriber, timeout(TEST_TIMEOUT.toMillis())).onComplete();

        // Chunks received - there should be more than one
        verify(subscriber, atLeast(MIN_CHUNKS_EXPECTED)).onNext(any(ByteBuf.class));
        verify(subscriber, atMost(MAX_CHUNKS_EXPECTED)).onNext(any(ByteBuf.class));

        // No errors
        verify(subscriber, never()).onError(any());
    }

    @Test
    void testRead_subscribeLate() throws Exception {

        var storagePath = "some_file.txt";
        var writeSignal = makeSmallFile(storagePath, storage, dataContext);
        waitFor(TEST_TIMEOUT, writeSignal);

        // Create a reader but do not subscribe to it
        // Reader should not try to access the file

        var reader = storage.reader(storagePath, dataContext);

        // Use another reader to read the file - should be ok
        var content = readFile(storagePath, storage, dataContext);
        waitFor(TEST_TIMEOUT, content);
        Assertions.assertTrue(resultOf(content).readableBytes() > 0);
        resultOf(content).release();

        // Delete the file
        var rm = storage.rm(storagePath, dataContext);
        waitFor(TEST_TIMEOUT, rm);

        // Now try subscribing to the reader - should result in a storage request error
        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        reader.subscribe(subscriber);

        verify(subscriber, timeout(TEST_TIMEOUT.toMillis())).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber, timeout(TEST_TIMEOUT.toMillis())).onError(any(EStorageRequest.class));
    }

    @Test
    void testRead_subscribeTwice() throws Exception {

        var storagePath = "some_file.txt";
        var writeSignal = makeSmallFile(storagePath, storage, dataContext);
        waitFor(TEST_TIMEOUT, writeSignal);

        // Two subscribers that just record their subscriptions

        CompletableFuture<Flow.Subscription> subscription1 = new CompletableFuture<>();
        Flow.Subscriber<ByteBuf> subscriber1 = unchecked(mock(Flow.Subscriber.class));
        doAnswer(invocation -> subscription1.complete(invocation.getArgument(0)))
            .when(subscriber1).onSubscribe(any(Flow.Subscription.class));
        // Also discard bytes from onNext, since some will be read
        doAnswer(invocation -> ReferenceCountUtil.release(invocation.getArgument(0)))
            .when(subscriber1).onNext(any(ByteBuf.class));

        CompletableFuture<Flow.Subscription> subscription2 = new CompletableFuture<>();
        Flow.Subscriber<ByteBuf> subscriber2 = unchecked(mock(Flow.Subscriber.class));
        doAnswer(invocation -> subscription2.complete(invocation.getArgument(0)))
            .when(subscriber2).onSubscribe(any(Flow.Subscription.class));

        var reader = storage.reader(storagePath, dataContext);
        reader.subscribe(subscriber1);
        reader.subscribe(subscriber2);

        // First subscription should receive onSubscribe as normal
        // Second should receive onError with an illegal state exception
        // As per: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/Flow.Publisher.html#subscribe(java.util.concurrent.Flow.Subscriber)

        verify(subscriber1, timeout(TEST_TIMEOUT.toMillis())).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber2, timeout(TEST_TIMEOUT.toMillis())).onError(any(IllegalStateException.class));

        // Subscription 1 should still work as normal when data is requested

        subscription1.get().request(2);

        verify(subscriber1, timeout(TEST_TIMEOUT.toMillis()).times(1)).onNext(any(ByteBuf.class));
        verify(subscriber1, timeout(TEST_TIMEOUT.toMillis()).times(1)).onComplete();

        // Subscription 2 should not receive any further signals

        verify(subscriber2, never()).onSubscribe(any());
        verify(subscriber2, never()).onNext(any());
        verify(subscriber2, never()).onComplete();
    }

    @Test
    void testRead_cancelImmediately() throws Exception {

        var storagePath = "some_file.txt";
        var writeSignal = makeSmallFile(storagePath, storage, dataContext);
        waitFor(TEST_TIMEOUT, writeSignal);

        // A subscriber that cancels as soon as it receives onSubscribe

        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        doAnswer(invocation -> {

            var subscription = (Flow.Subscription) invocation.getArgument(0);
            subscription.cancel();

            return null;

        }).when(subscriber).onSubscribe(any(Flow.Subscription.class));

        // Read using the mocked subscriber
        // No error should be thrown as a result of the cancel

        var reader = storage.reader(storagePath, dataContext);
        reader.subscribe(subscriber);

        // Expected sequence of calls into the subscriber
        //  - One call into onSubscribe, when the subscriber was subscribed
        //  - Nothing else, because we cancelled before making any requests

        verify(subscriber, timeout(TEST_TIMEOUT.toMillis()).times(1)).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber, never()).onNext(any());
        verify(subscriber, never()).onComplete();
        verify(subscriber, never()).onError(any());
    }

    @Test
    void testRead_cancelAndRetry() throws Exception {

        var storagePath = "some_file.dat";

        // Create a file big enough that it can't be read in a single chunk

        var originalBytes = new byte[10000];
        var originalContent = ByteBufAllocator.DEFAULT.directBuffer(10000);

        var random = new Random();
        random.nextBytes(originalBytes);
        originalContent.writeBytes(originalBytes);

        var writeSignal = makeFile(storagePath, originalContent, storage, dataContext);
        waitFor(TEST_TIMEOUT, writeSignal);

        // A subscriber that will read one chunk and then cancel the subscription

        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        doAnswer(invocation -> {

            // Request 4 chunks, but we'll cancel when we get the first one
            subscription.complete(invocation.getArgument(0));
            subscription.get().request(4);
            return null;

        }).when(subscriber).onSubscribe(any(Flow.Subscription.class));
        doAnswer(invocation -> {

            ReferenceCountUtil.release(invocation.getArgument(0));
            subscription.get().cancel();
            return null;

        }).when(subscriber).onNext(any(ByteBuf.class));

        // Create a reader and read using the mocked subscriber

        var reader = storage.reader(storagePath, dataContext);
        reader.subscribe(subscriber);

        // Expected sequence of calls into the subscriber
        //  - One call into onSubscribe, we request 2 chunks
        //  - Allow the reader to send at most one chunk after cancelling (in fact, Java Publisher spec allows more)
        //  - No calls to onComplete or onError

        verify(subscriber, timeout(TEST_TIMEOUT.toMillis()).times(1)).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber, atMost(2)).onNext(any());
        verify(subscriber, never()).onComplete();
        verify(subscriber, never()).onError(any());

        // Now do a regular read and make sure the whole content comes back

        var retryRead = readFile(storagePath, storage, dataContext);

        waitFor(TEST_TIMEOUT, retryRead);
        var content = resultOf(retryRead);

        var bytes = new byte[content.readableBytes()];
        content.readBytes(bytes);
        content.release();

        Assertions.assertArrayEquals(originalBytes, bytes);
    }

    @Test
    void testRead_cancelAndDelete() throws Exception {

        var storagePath = "some_file.dat";

        // Create a file big enough that it can't be read in a single chunk

        var originalBytes = new byte[10000];
        var originalContent = ByteBufAllocator.DEFAULT.directBuffer(10000);

        var random = new Random();
        random.nextBytes(originalBytes);
        originalContent.writeBytes(originalBytes);

        var writeSignal = makeFile(storagePath, originalContent, storage, dataContext);
        waitFor(TEST_TIMEOUT, writeSignal);

        // A subscriber that will read one chunk and then cancel the subscription

        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        doAnswer(invocation -> {

            // Request 4 chunks, but we'll cancel when we get the first one
            subscription.complete(invocation.getArgument(0));
            subscription.get().request(4);
            return null;

        }).when(subscriber).onSubscribe(any(Flow.Subscription.class));
        doAnswer(invocation -> {

            ReferenceCountUtil.release(invocation.getArgument(0));
            subscription.get().cancel();
            return null;

        }).when(subscriber).onNext(any(ByteBuf.class));

        // Create a reader and read using the mocked subscriber

        var reader = storage.reader(storagePath, dataContext);
        reader.subscribe(subscriber);

        // Expected sequence of calls into the subscriber
        //  - One call into onSubscribe, we request 2 chunks
        //  - One call into onNext, then we cancel the subscription
        //  - Allow the reader to send at most one chunk after cancelling (in fact, Java Publisher spec allows more)
        //  - No calls to onComplete or onError

        verify(subscriber, timeout(TEST_TIMEOUT.toMillis()).times(1)).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber, atMost(2)).onNext(any());
        verify(subscriber, never()).onComplete();
        verify(subscriber, never()).onError(any());

        // Now delete the file

        var rm = storage.rm(storagePath, dataContext);

        waitFor(TEST_TIMEOUT, rm);

        var exists = storage.exists(storagePath, dataContext);

        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertFalse(resultOf(exists));
    }

    @SuppressWarnings("unchecked")
    private <T> T unchecked(Object unchecked) {

        return (T) unchecked;
    }

    private static class TestException extends RuntimeException {

        TestException() { super("Test error handling"); }
    }

}
