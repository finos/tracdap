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

import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.storage.StorageErrors;
import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.util.concurrent.OrderedEventExecutor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;
import static org.finos.tracdap.common.storage.local.LocalFileStorage.READ_OPERATION;
import static java.nio.file.StandardOpenOption.*;


public class LocalFileReader implements Flow.Publisher<ArrowBuf> {

    private static final int DEFAULT_CHUNK_SIZE = 4096;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storagePath;
    private final Path absolutePath;
    private final BufferAllocator allocator;
    private final OrderedEventExecutor executor;
    private final StorageErrors errors;

    private final AtomicBoolean subscriberSet;
    private Flow.Subscriber<? super ArrowBuf> subscriber;

    private AsynchronousFileChannel channel;
    private ChunkReadHandler readHandler;

    private int chunksPending;
    private long bytesRead;
    private boolean chunkInProgress;
    private boolean gotComplete;
    private boolean gotCancel;
    private boolean gotError;

    LocalFileReader(
            String storagePath, Path absolutePath,
            IDataContext dataContext,
            StorageErrors errors) {

        this.storagePath = storagePath;
        this.absolutePath = absolutePath;
        this.allocator = dataContext.arrowAllocator();
        this.executor = dataContext.eventLoopExecutor();
        this.errors = errors;

        this.subscriberSet = new AtomicBoolean(false);
        this.subscriber = null;

        chunksPending = 0;
        bytesRead = 0;
        gotComplete = false;
        gotCancel = false;
        gotError = false;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PUBLISHER INTERFACE
    // -----------------------------------------------------------------------------------------------------------------

    // These public methods can all be called externally and may be called on different threads


    @Override
    public void subscribe(Flow.Subscriber<? super ArrowBuf> subscriber) {

        // Avoid concurrency issues - use atomic boolean CAS to ensure the method is only called once
        // No other processing has started at this point

        var subscribeOk = subscriberSet.compareAndSet(false, true);

        if (!subscribeOk) {

            // According to Java API docs, errors in subscribe() should be reported as IllegalStateException
            // https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/Flow.Publisher.html#subscribe(java.util.concurrent.Flow.Subscriber)

            var eStorage = errors.explicitError(READ_OPERATION, storagePath, DUPLICATE_SUBSCRIPTION);
            var eFlowState = new IllegalStateException(eStorage.getMessage(), eStorage);
            subscriber.onError(eFlowState);
            return;
        }

        this.subscriber = subscriber;

        // Make sure the doStart action goes into the event loop before calling subscriber.onSubscribe()
        // This makes sure that doStart is called before any requests from the subscription get processed

        executor.submit(this::doStart);

        // Now activate the subscription, before doStart gets executed
        // This approach allows errors to be reported normally during onStart (e.g. file not found)
        // Otherwise, if the subscription is not yet active, errors should be reported with IllegalStateException
        // File not found is an expected error, reporting it with EStorage makes for cleaner error handling

        subscriber.onSubscribe(new ReadSubscription());
    }

    private class ReadSubscription implements Flow.Subscription {

        @Override
        public void request(long n) {

            // Avoid concurrency issues - ensure all calls are processed in the ordered event loop
            executor.submit(() -> doRequest(n));
        }

        @Override
        public void cancel() {

            // Avoid concurrency issues - ensure all calls are processed in the ordered event loop
            executor.submit(LocalFileReader.this::doCancel);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PRIVATE IMPLEMENTATION
    // -----------------------------------------------------------------------------------------------------------------

    // These methods are only ever called on the ordered event executor

    private void doStart() {

        // When doStart is called, subscription is already active
        // So, it is fine to report errors normally if they occur

        try {

            this.channel = AsynchronousFileChannel.open(absolutePath, Set.of(READ), executor);
            this.readHandler = new ChunkReadHandler();

            log.info("File channel open for reading: [{}]", absolutePath);
        }
        catch (Exception e) {

            log.error("File channel could not be opened: {} [{}]", e.getMessage(), absolutePath, e);

            gotError = true;
            var eStorage = errors.handleException(READ_OPERATION, storagePath, e);
            subscriber.onError(eStorage);
        }
    }

    private void doComplete() {

        try {

            channel.close();

            log.info("File channel closed: [{}]", absolutePath);

            subscriber.onComplete();
        }
        catch (Exception e) {

            log.error("File channel was not closed cleanly: {} [{}]", e.getMessage(), absolutePath, e);

            gotError = true;
            var eStorage = errors.handleException(READ_OPERATION, storagePath, e);
            subscriber.onError(eStorage);
        }
    }

    private void doRequest(long n) {

        // Do not accept the request if the read operation has finished for any reason
        if (gotComplete || gotError || gotCancel)
            return;

        chunksPending += n;

        if (!chunkInProgress)
            readChunk();
    }

    private void doCancel() {

        // Do not process cancellation if the read operation has finished for any reason
        if (gotComplete || gotError || gotCancel)
            return;

        try {
            gotCancel = true;

            log.info("Read operation cancelled: [{}]",  absolutePath);

            channel.close();

            log.info("File channel closed: [{}]", absolutePath);

            // Do not send any signal to the subscriber for a clean cancel
        }
        catch (Exception e) {

            log.error("File channel was not closed cleanly: {} [{}]", e.getMessage(), absolutePath, e);

            gotError = true;

            // If the cancel results in an error closing the file, do not send the onError message
        }
    }

    private void readChunk() {

        try {

            // This should never happen - chunkInProgress is always checked or set before calling readChunk()
            // If this condition check ever fails, a race condition has occurred and the reader is in unknown state

            if (chunkInProgress)
                throw new EUnexpected();

            var chunk = allocator.buffer(DEFAULT_CHUNK_SIZE);
            var nioChunk = chunk.nioBuffer(0, DEFAULT_CHUNK_SIZE);

            channel.read(nioChunk, bytesRead, chunk, readHandler);

            chunkInProgress = true;
        }
        catch (Exception e) {

            gotError = true;
            handleError(e);
        }
    }

    private void readChunkComplete(Integer nBytes, ArrowBuf chunk) {

        // Update counts

        if (nBytes > 0)
            bytesRead += nBytes;

        chunksPending -= 1;
        chunkInProgress = false;

        // Check if the read is already failed or cancelled
        // If so, release the buffer and do not send any further signals
        if (gotError || gotCancel) {

            releaseBuffer(chunk);
        }

        // nBytes read < 0 indicates the read is complete
        else if (nBytes < 0) {

            // Buffer contains no data so can be released immediately
            releaseBuffer(chunk);

            // Make sure not to send multiple onComplete signals
            // E.g. if multiple chunks have been requested past the end of the file

            var alreadyComplete = gotComplete;
            gotComplete = true;

            if (!(gotError || gotCancel || alreadyComplete))
                doComplete();
        }

        // Otherwise, this is a normal read
        // Count the bytes and send them on their journey!
        else {

            gotChunk(chunk, nBytes);
        }
    }

    private void readChunkFailed(Throwable error, ArrowBuf chunk) {

        chunksPending -= 1;
        chunkInProgress = false;

        // Buffer contains no data so can be released immediately
        releaseBuffer(chunk);

        // Async close exception is sent for operations that were in progress when the channel was closed
        // If the close happened because of a complete or cancel event,
        // then it is safe to ignore the error

        if (gotCancel && error instanceof AsynchronousCloseException)
            return;

        // If a chunk read fails after the operation has finished for any other reason, just log a warning
        // It won't be possible to signal the subscriber, because a final status is already sent

        if (gotComplete || gotCancel || gotError) {

            log.warn("Read operation is terminated but further errors occurred: [{}]", absolutePath, error);
            return;
        }

        gotError = true;
        handleError(error);
    }

    private void gotChunk(ArrowBuf chunk, int nBytes) {

        try {

            // If the operation is already failed / cancelled, don't pass this chunk on

            if (gotCancel || gotError) {
                releaseBuffer(chunk);
                return;
            }

            // Trigger the next read operation immediately
            // Possibly the subscriber is going to do processing in onNext

            if (chunksPending > 0)
                readChunk();

            // The channel wrote into the underlying nio ByteBuffer
            // Update the Netty ByteBuf to match the number of bytes received

            chunk.writerIndex(nBytes);

            // Signal the subscriber

            subscriber.onNext(chunk);
        }
        catch (Exception e) {

            releaseBuffer(chunk);

            gotError = true;
            handleError(e);
        }
    }

    private void handleError(Throwable throwable) {

        var error = throwable instanceof Exception
                ? (Exception) throwable
                : new ExecutionException(throwable);

        try {

            log.error("Read operation failed: {} [{}]", throwable.getMessage(), absolutePath, throwable);

            channel.close();

            log.info("File channel closed: [{}]", absolutePath);

            var eStorage = errors.handleException(READ_OPERATION, storagePath, error);
            subscriber.onError(eStorage);
        }
        catch (Exception e) {

            log.error("File channel was not closed cleanly: {} [{}]", e.getMessage(), absolutePath, e);

            // Report the original error back up the chain, not the secondary error that occurred on close

            var eStorage = errors.handleException(READ_OPERATION, storagePath, e);
            subscriber.onError(eStorage);
        }
    }

    private void releaseBuffer(ArrowBuf buffer) {

        buffer.close();

        if (buffer.refCnt() != 0 && buffer.capacity() > 0)
            log.warn("Chunk buffer was not released (this could indicate a memory leak)");
    }

    private class ChunkReadHandler implements CompletionHandler<Integer, ArrowBuf> {

        @Override
        public void completed(Integer nBytes, ArrowBuf chunk) {

            readChunkComplete(nBytes, chunk);
        }

        @Override
        public void failed(Throwable error, ArrowBuf chunk) {

            readChunkFailed(error, chunk);
        }
    }
}
