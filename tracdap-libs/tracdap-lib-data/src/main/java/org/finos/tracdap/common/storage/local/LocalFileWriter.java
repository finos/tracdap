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

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.OrderedEventExecutor;
import org.finos.tracdap.common.storage.StorageErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;
import static org.finos.tracdap.common.storage.local.LocalFileStorage.WRITE_OPERATION;
import static java.nio.file.StandardOpenOption.*;


public class LocalFileWriter implements Flow.Subscriber<ByteBuf> {

    private static final int CHUNK_BUFFER_CAPACITY = 32;
    private static final int CHUNK_BUFFER_MIN_REQUESTS = 8;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final StorageErrors errors;
    private final String storagePath;

    private final Path absolutePath;
    private final CompletableFuture<Long> signal;
    private final OrderedEventExecutor executor;

    private final AtomicBoolean subscriptionSet;
    private Flow.Subscription subscription;
    private AsynchronousFileChannel channel;
    private ChunkWriteHandler writeHandler;

    private final Queue<ByteBuf> chunkBuffer;
    private int chunksRequested;
    private boolean chunkInProgress;
    private long bytesReceived;
    private long bytesWritten;
    private boolean gotComplete;
    private boolean gotError;
    private final boolean gotCancel;

    LocalFileWriter(
            String storageKey, String storagePath,
            Path absolutePath, CompletableFuture<Long> signal, OrderedEventExecutor executor) {

        this.errors = new LocalStorageErrors(storageKey, log);
        this.storagePath = storagePath;

        this.absolutePath = absolutePath;
        this.signal = signal;
        this.executor = executor;

        this.subscriptionSet = new AtomicBoolean(false);
        this.subscription = null;

        this.chunkBuffer = new ArrayBlockingQueue<>(CHUNK_BUFFER_CAPACITY);
        bytesReceived = 0;
        bytesWritten = 0;
        gotComplete = false;
        gotError = false;
        gotCancel = false;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SUBSCRIBER INTERFACE
    // -----------------------------------------------------------------------------------------------------------------

    // These public methods can all be called externally and may be called on different threads


    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        // Avoid concurrency issues - use atomic boolean CAS to ensure the method is only called once
        // No other processing has started at this point

        var subscribeOk = subscriptionSet.compareAndSet(false, true);

        if (!subscribeOk) {

            // According to Java API docs, errors in subscribe() should be reported as IllegalStateException
            // The completion signal is already being used to report on the existing, valid subscription
            // So, the duplicate subscription can't be reported there
            // Our only option is to throw directly from this method

            // According to the Java API docs, behavior is not guaranteed when onSubscribe throws
            // Still, it seems likely to break the pipeline for the new subscription, which is what we want

            // https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/Flow.Publisher.html#subscribe(java.util.concurrent.Flow.Subscriber)

            var eStorage = errors.explicitError(DUPLICATE_SUBSCRIPTION, storagePath, WRITE_OPERATION);
            throw new IllegalStateException(eStorage.getMessage(), eStorage);
        }

        this.subscription = subscription;

        executor.submit(this::doStart);
    }

    @Override
    public void onNext(ByteBuf chunk) {

        // Avoid concurrency issues - ensure all calls are processed in the ordered event loop
        executor.submit(() -> this.doNext(chunk));
    }

    @Override
    public void onComplete() {

        // Avoid concurrency issues - ensure all calls are processed in the ordered event loop
        executor.submit(this::doComplete);
    }

    @Override
    public void onError(Throwable error) {

        // Avoid concurrency issues - ensure all calls are processed in the ordered event loop
        executor.submit(() -> this.doError(error));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PRIVATE IMPLEMENTATION
    // -----------------------------------------------------------------------------------------------------------------

    // These methods are only ever called on the ordered event executor

    // Note: All errors must be passed to handleError
    // Errors that escape on the executor thread will be lost

    private void doStart() {

        try {

            this.channel = AsynchronousFileChannel.open(absolutePath, Set.of(WRITE, CREATE_NEW), executor);
            this.writeHandler = new ChunkWriteHandler();

            log.info("File channel open for writing: [{}]", absolutePath);

            // We want to keep at least MIN_REQUESTS chunks in the buffer, so start with double that
            var initialRequest = CHUNK_BUFFER_MIN_REQUESTS * 2;

            subscription.request(initialRequest);
            chunksRequested += initialRequest;
        }
        catch (Exception e) {

            subscription.cancel();

            var eStorage = errors.handleException(e, storagePath, WRITE_OPERATION);
            signal.completeExceptionally(eStorage);
        }
    }

    private void doRequestMore() {

        // Do not request more if the operation is already finished for any reason
        if (gotComplete || gotError || gotCancel)
            return;

        try {

            if (chunksRequested < CHUNK_BUFFER_MIN_REQUESTS) {
                var requestSize = CHUNK_BUFFER_MIN_REQUESTS;
                chunksRequested += requestSize;
                subscription.request(requestSize);
            }
        }
        catch (Exception e) {

            gotError = true;
            handleError(e, true);
        }
    }

    private void doNext(ByteBuf chunk) {

        // Do not accept the onNext message if the operation is already finished for any reason

        if (gotComplete || gotError || gotCancel) {
            releaseBuffer(chunk);
            return;
        }

        var bufferedOk = chunkBuffer.offer(chunk);

        // Buffer overflow will only happen if more chunks are sent than have been requested
        if (!bufferedOk) {
            var err = "Buffer overflow (data was received more quickly than it could be written)";
            log.error(err);
            handleError(new ETracInternal(err), true);
        }

        if (!chunkInProgress)
            writeNextChunk();
    }

    private void doComplete() {

        // Do not accept the onComplete message if the operation is already finished for any reason

        if (gotComplete || gotError || gotCancel)
            return;

        gotComplete = true;

        if (chunkBuffer.isEmpty() && !chunkInProgress)
            handleComplete();
    }

    private void doError(Throwable error) {

        // Do not accept the onError message if the operation is already finished for any reason

        if (gotComplete || gotError || gotCancel)
            return;

        gotError = true;
        handleError(error, false);
    }

    private void writeNextChunk() {

        ByteBuf chunk = null;

        try {

            // This should never happen - chunkInProgress is always checked or set before calling writeChunk()
            // If this condition check ever fails, a race condition has occurred and the writer is in unknown state

            if (chunkInProgress || chunkBuffer.isEmpty())
                throw new EUnexpected();

            chunkInProgress = true;
            chunksRequested -= 1;
            chunk = chunkBuffer.remove();

            var offset = bytesReceived;
            bytesReceived += chunk.readableBytes();

            if (chunk.nioBufferCount() < 1)
                throw new EUnexpected();

            var nioChunk = chunk.nioBuffer();

            channel.write(nioChunk, offset, chunk, writeHandler);
        }
        catch (Exception e) {

            if (chunk != null)
                releaseBuffer(chunk);

            gotError = true;
            handleError(e, true);
        }
    }

    public void writeChunkComplete(Integer nBytes, ByteBuf chunk) {

        // Update counts
        bytesWritten += nBytes;
        chunkInProgress = false;

        // The number of bytes in the buffer is the expected number of bytes written
        var bytesExpected = chunk.readableBytes();

        // Chunk can be released whether the operation succeeds or fails - it is no longer needed
        releaseBuffer(chunk);

        // Check if the write operation is already failed or cancelled

        if (gotError || gotCancel) {

            if (log.isDebugEnabled())
                log.debug("Ignoring chunks, write operation is failed or terminated: [{}]", absolutePath);

            return;
        }

        // If the full content of the buffer was not written, this is an error
        if (nBytes != bytesExpected) {

            gotError = true;

            var error = errors.chunkNotFullyWritten(bytesExpected, nBytes);
            handleError(error, true);
        }

        // If onComplete has been sent and there are no chunks pending, then the operation is complete
        else if (gotComplete && chunkBuffer.isEmpty()) {

            handleComplete();
        }

        // Otherwise, this is a normal write
        else {

            if (!chunkBuffer.isEmpty())
                writeNextChunk();

            doRequestMore();
        }
    }

    public void writeChunkFailed(Throwable error, ByteBuf chunk) {

        // Update counts
        chunkInProgress = false;

        // Chunk can be released whether the operation succeeds or fails - it is no longer needed
        releaseBuffer(chunk);

        // Async close exception is sent for operations that were in progress when the channel was closed
        // If the close happened because of a complete or cancel event,
        // then it is safe to ignore the error

        if (error instanceof AsynchronousCloseException)
            return;

        // If a chunk write fails after the operation has finished for any other reason, just log a warning
        // It won't be possible to send the signal, because a signal is already sent

        if (gotComplete || gotError || gotCancel) {

            log.warn("Write operation is terminated but further errors occurred: [{}]", absolutePath, error);
            return;
        }

        gotError = true;
        handleError(error, true);
    }

    private void handleComplete() {

        try {

            log.info("Write operation complete: {} bytes written [{}]", bytesWritten, absolutePath);

            channel.close();

            log.info("File channel closed: [{}]", absolutePath);

            signal.complete(bytesWritten);
        }
        catch (Exception e) {

            log.error("File channel was not closed cleanly: {} [{}]", e.getMessage(), absolutePath, e);

            var eStorage = errors.handleException(e, storagePath, WRITE_OPERATION);
            signal.completeExceptionally(eStorage);
        }
    }

    private void handleError(Throwable error, boolean internalError) {

        var eWrapped = internalError
                ? errors.handleException(error, storagePath, WRITE_OPERATION)
                : wrapExternalError(error);

        try {

            if (internalError) {
                log.error("Write operation failed: {} [{}]", error.getMessage(), absolutePath, eWrapped);
                subscription.cancel();
            }
            else
                // Do not log stack trace for errors in the source data stream
                // Stack trace will be logged at the original error site, and again in the final outbound handler
                log.error("Write operation stopped due to an error: {} [{}]", error.getMessage(), absolutePath);

            while (!chunkBuffer.isEmpty()) {
                var chunk = chunkBuffer.remove();
                releaseBuffer(chunk);
            }

            channel.close();

            // Try to remove the partially written file
            Files.deleteIfExists(absolutePath);

            log.info("File channel closed: [{}]", absolutePath);

            signal.completeExceptionally(eWrapped);
        }
        catch (Exception e) {

            log.error("File channel was not closed cleanly: {} [{}]", e.getMessage(), absolutePath, e);

            // Report the original error back up the chain, not the secondary error that occurred on close

            signal.completeExceptionally(eWrapped);
        }
    }

    private Exception wrapExternalError(Throwable error) {

        if (error instanceof CompletionException)
            return (CompletionException) error;
        else
            return new CompletionException(error.getMessage(), error);
    }

    private void releaseBuffer(ByteBuf buffer) {

        var releaseOk = buffer.release();

        if (!releaseOk && buffer.capacity() > 0)
            log.warn("Chunk buffer was not released (this could indicate a memory leak)");
    }

    private class ChunkWriteHandler implements CompletionHandler<Integer, ByteBuf> {

        @Override
        public void completed(Integer nBytes, ByteBuf chunk) {

            writeChunkComplete(nBytes, chunk);
        }

        @Override
        public void failed(Throwable error, ByteBuf chunk) {

            writeChunkFailed(error, chunk);
        }
    }
}
