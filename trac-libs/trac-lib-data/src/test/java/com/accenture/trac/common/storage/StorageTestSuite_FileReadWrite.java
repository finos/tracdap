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

import com.accenture.trac.common.eventloop.ExecutionContext;
import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.exception.EStorageRequest;
import com.accenture.trac.common.exception.EValidationGap;
import com.accenture.trac.common.storage.local.LocalFileStorage;
import com.accenture.trac.common.util.Concurrent;

import io.netty.buffer.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static com.accenture.trac.test.storage.StorageTestHelpers.*;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
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
    public static final Duration ASYNC_DELAY = Duration.ofSeconds(1);

    IFileStorage storage;
    IExecutionContext execContext;

    @TempDir
    Path storageDir;

    @BeforeEach
    void setupStorage() {

        // TODO: Abstract mechanism for obtaining storage impl using config

        storage = new LocalFileStorage("TEST_STORAGE", storageDir.toString());
        execContext = new ExecutionContext(new DefaultEventExecutor(new DefaultThreadFactory("t-events")));
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
        Assertions.assertDoesNotThrow(() -> resultOf(writeSignal));

        var reader = storage.reader(storagePath, execContext);
        var readResult = Concurrent.fold(
                reader, Unpooled::wrappedBuffer,
                (ByteBuf) new EmptyByteBuf(ByteBufAllocator.DEFAULT));

        waitFor(TEST_TIMEOUT, readResult);

        var roundTrip = resultOf(readResult);
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
    void roundTrip_heterogeneous() {

        Assertions.fail();
    }

    @Test
    void roundTrip_empty() throws Exception {

        var storagePath = "empty.dat";
        var original = new EmptyByteBuf(ByteBufAllocator.DEFAULT);

        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, execContext);
        Concurrent.publish(Stream.of(original)).subscribe(writer);

        waitFor(TEST_TIMEOUT, writeSignal);

        // Make sure the write operation did not report an error before trying to read
        Assertions.assertDoesNotThrow(() -> resultOf(writeSignal));

        var reader = storage.reader(storagePath, execContext);
        var readResult = Concurrent.fold(
                reader, Unpooled::wrappedBuffer,
                (ByteBuf) new EmptyByteBuf(ByteBufAllocator.DEFAULT));

        waitFor(TEST_TIMEOUT, readResult);

        var roundTrip = resultOf(readResult);

        Assertions.assertEquals(0, roundTrip.readableBytes());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Functional error cases
    // -----------------------------------------------------------------------------------------------------------------

    // Functional error tests can be set up and verified entirely using the storage API
    // All back ends should behave consistently for these tests

    // Errors are checked using resultOf(), which unwraps stream state errors
    // This exposes the functional cause of errors in the stream, which will be EStorage (or ETrac) errors

    @Test
    void testWrite_missingDir() {

        var storagePath = "missing_dir/some_file.txt";
        var content = ByteBufUtil.encodeString(
                ByteBufAllocator.DEFAULT,
                CharBuffer.wrap("Some content"),
                StandardCharsets.UTF_8);

        var contentStream = Concurrent.publish(Stream.of(content));

        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, execContext);

        Assertions.assertThrows(EStorageRequest.class, () -> {

            contentStream.subscribe(writer);
            waitFor(TEST_TIMEOUT, writeSignal);
            resultOf(writeSignal);
        });
    }

    @Test
    void testWrite_alreadyExists() throws Exception {

        var storagePath = "some_file.txt";
        var content = ByteBufUtil.encodeString(
                ByteBufAllocator.DEFAULT,
                CharBuffer.wrap("Some content"),
                StandardCharsets.UTF_8);

        var contentStream = Concurrent.publish(Stream.of(content));
        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, execContext);
        contentStream.subscribe(writer);

        waitFor(TEST_TIMEOUT, writeSignal);

        var exists = storage.exists(storagePath);
        waitFor(TEST_TIMEOUT, exists);

        Assertions.assertTrue(resultOf(exists));

        var contentStream2 = Concurrent.publish(Stream.of(content));
        var writeSignal2 = new CompletableFuture<Long>();
        var writer2 = storage.writer(storagePath, writeSignal2, execContext);

        Assertions.assertThrows(EStorageRequest.class, () -> {

            contentStream2.subscribe(writer2);
            waitFor(TEST_TIMEOUT, writeSignal);
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
                storage.writer(absolutePath, writeSignal1, execContext));

        var writeSignal2 = new CompletableFuture<Long>();
        Assertions.assertThrows(EValidationGap.class, () ->
                storage.writer(invalidPath, writeSignal2, execContext));
    }

    @Test
    void testWrite_storageRoot() {

        var storagePath = ".";

        var writeSignal = new CompletableFuture<Long>();

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.writer(storagePath, writeSignal, execContext));
    }

    @Test
    void testWrite_outsideRoot() {

        var storagePath = "../any_file.txt";

        var writeSignal = new CompletableFuture<Long>();

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.writer(storagePath, writeSignal, execContext));
    }

    @Test
    void testRead_missing() {

        var storagePath = "missing_file.txt";

        var reader = storage.reader(storagePath, execContext);

        Assertions.assertThrows(EStorageRequest.class, () -> {

            var readResult = Concurrent.fold(
                    reader, Unpooled::wrappedBuffer,
                    (ByteBuf) new EmptyByteBuf(ByteBufAllocator.DEFAULT));

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
                storage.reader(absolutePath, execContext));

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.reader(invalidPath, execContext));
    }

    @Test
    void testRead_storageRoot() {

        var storagePath = ".";

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.reader(storagePath, execContext));
    }

    @Test
    void testRead_outsideRoot() {

        var storagePath = "../some_file.txt";

        Assertions.assertThrows(EValidationGap.class, () ->
                storage.reader(storagePath, execContext));
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
    void testWrite_notStarted() throws Exception {

        var storagePath = "some_file.dat";

        var writeSignal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, writeSignal, execContext);

        var subscription = new Flow.Subscription() {

            @Override
            public void request(long n) {

            }

            @Override
            public void cancel() {

            }
        };

        writer.onSubscribe(subscription);
        writer.onError(new RuntimeException("Dummy error"));
        waitFor(TEST_TIMEOUT, writeSignal);

        // writeSignal.can

        Assertions.assertThrows(CancellationException.class, () -> resultOf(writeSignal));

        var exists = storage.exists(storagePath);
        waitFor(TEST_TIMEOUT, exists);

        Assertions.assertFalse(resultOf(exists));
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
    void testRead_requestUpfront() {

        var storagePath = "some_file.dat";

        // Create a file big enough that it needs many chunks to read

        var originalBytes = new byte[10 * 1024 * 1024];
        var originalContent = ByteBufAllocator.DEFAULT.directBuffer(10000);

        var random = new Random();
        random.nextBytes(originalBytes);
        originalContent.writeBytes(originalBytes);

        var write = makeFile(storagePath, originalContent, storage, execContext);
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

        var reader = storage.reader(storagePath, execContext);
        reader.subscribe(subscriber);

        // onSubscribe should be received right away
        verify(subscriber, times(1)).onSubscribe(any(Flow.Subscription.class));

        // The stream should be read until complete
        verify(subscriber, timeout(TEST_TIMEOUT.toMillis())).onComplete();

        // Chunks received - there should be more than one
        verify(subscriber, atLeast(MIN_CHUNKS_EXPECTED)).onNext(any(ByteBuf.class));
        verify(subscriber, atMost(MAX_CHUNKS_EXPECTED)).onNext(any(ByteBuf.class));

        // No errors
        verify(subscriber, never()).onError(any());
    }

    @Test
    void testRead_neverSubscribe() throws Exception {

        var storagePath = "some_file.txt";
        makeSmallFile(storagePath, storage, execContext);

        // Create a reader but do not subscribe to it
        // Reader should not try to access the file

        var reader = storage.reader(storagePath, execContext);

        // Use another reader to read the file - should be ok
        var content = readFile(storagePath, storage, execContext);
        waitFor(TEST_TIMEOUT, content);
        Assertions.assertTrue(resultOf(content).readableBytes() > 0);
        resultOf(content).release();

        // Delete the file
        var rm = storage.rm(storagePath, false);
        waitFor(TEST_TIMEOUT, rm);

        // Now try subscribing to the reader - should result in an illegal state exception
        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        reader.subscribe(subscriber);

        verify(subscriber, never()).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber, times(1)).onError(any(IllegalStateException.class));
    }

    @Test
    void testRead_subscribeTwice() throws Exception {

        var storagePath = "some_file.txt";
        makeSmallFile(storagePath, storage, execContext);

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

        var reader = storage.reader(storagePath, execContext);
        reader.subscribe(subscriber1);
        reader.subscribe(subscriber2);

        // First subscription should receive onSubscribe as normal
        // Second should receive onError with an illegal state exception
        // As per: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/Flow.Publisher.html#subscribe(java.util.concurrent.Flow.Subscriber)

        verify(subscriber1, times(1)).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber2, times(1)).onError(any(IllegalStateException.class));

        // Subscription 1 should still work as normal when data is requested

        subscription1.get().request(2);
        Thread.sleep(ASYNC_DELAY.toMillis());

        verify(subscriber1, times(1)).onNext(any(ByteBuf.class));
        verify(subscriber1, times(1)).onComplete();

        // Subscription 2 should not receive any further signals

        verify(subscriber2, never()).onSubscribe(any());
        verify(subscriber2, never()).onNext(any());
        verify(subscriber2, never()).onComplete();
    }

    @Test
    void testRead_cancelImmediately() throws Exception {

        var storagePath = "some_file.txt";
        makeSmallFile(storagePath, storage, execContext);

        // A subscriber that cancels as soon as it receives onSubscribe

        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        doAnswer(invocation -> {

            var subscription = (Flow.Subscription) invocation.getArgument(0);
            subscription.cancel();

            return null;

        }).when(subscriber).onSubscribe(any(Flow.Subscription.class));

        // Read using the mocked subscriber
        // No error should be thrown as a result of the cancel

        var reader = storage.reader(storagePath, execContext);
        reader.subscribe(subscriber);

        // Expected sequence of calls into the subscriber
        //  - One call into onSubscribe, when the subscriber was subscribed
        //  - Nothing else, because we cancelled before making any requests

        Thread.sleep(ASYNC_DELAY.toMillis());

        verify(subscriber, times(1)).onSubscribe(any(Flow.Subscription.class));
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

        makeFile(storagePath, originalContent, storage, execContext);

        // A subscriber that will read one chunk and then cancel the subscription

        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        doAnswer(invocation -> {

            // Request 2 chunks, but we'll cancel when we get the first one
            subscription.complete(invocation.getArgument(0));
            subscription.get().request(2);
            return null;

        }).when(subscriber).onSubscribe(any(Flow.Subscription.class));
        doAnswer(invocation -> {

            ReferenceCountUtil.release(invocation.getArgument(0));
            subscription.get().cancel();
            return null;

        }).when(subscriber).onNext(any(ByteBuf.class));

        // Create a reader and read using the mocked subscriber

        var reader = storage.reader(storagePath, execContext);
        reader.subscribe(subscriber);

        // Expected sequence of calls into the subscriber
        //  - One call into onSubscribe, we request 2 chunks
        //  - One call into onNext, then we cancel the subscription
        //  - No further calls to onNext, no calls to onComplete or onError

        Thread.sleep(ASYNC_DELAY.toMillis());

        verify(subscriber, times(1)).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber, times(1)).onNext(any());
        verify(subscriber, never()).onComplete();
        verify(subscriber, never()).onError(any());

        // Now do a regular read and make sure the whole content comes back

        var retryRead = readFile(storagePath, storage, execContext);

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

        makeFile(storagePath, originalContent, storage, execContext);

        // A subscriber that will read one chunk and then cancel the subscription

        Flow.Subscriber<ByteBuf> subscriber = unchecked(mock(Flow.Subscriber.class));
        CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        doAnswer(invocation -> {

            // Request 2 chunks, but we'll cancel when we get the first one
            subscription.complete(invocation.getArgument(0));
            subscription.get().request(2);
            return null;

        }).when(subscriber).onSubscribe(any(Flow.Subscription.class));
        doAnswer(invocation -> {

            ReferenceCountUtil.release(invocation.getArgument(0));
            subscription.get().cancel();
            return null;

        }).when(subscriber).onNext(any(ByteBuf.class));

        // Create a reader and read using the mocked subscriber

        var reader = storage.reader(storagePath, execContext);
        reader.subscribe(subscriber);

        // Expected sequence of calls into the subscriber
        //  - One call into onSubscribe, we request 2 chunks
        //  - One call into onNext, then we cancel the subscription
        //  - No further calls to onNext, no calls to onComplete or onError

        Thread.sleep(ASYNC_DELAY.toMillis());

        verify(subscriber, times(1)).onSubscribe(any(Flow.Subscription.class));
        verify(subscriber, times(1)).onNext(any());
        verify(subscriber, never()).onComplete();
        verify(subscriber, never()).onError(any());

        // Now delete the file

        var rm = storage.rm(storagePath, false);

        waitFor(TEST_TIMEOUT, rm);

        var exists = storage.exists(storagePath);

        waitFor(TEST_TIMEOUT, exists);
        Assertions.assertFalse(resultOf(exists));
    }

    @SuppressWarnings("unchecked")
    private <T> T unchecked(Object unchecked) {

        return (T) unchecked;
    }

}
