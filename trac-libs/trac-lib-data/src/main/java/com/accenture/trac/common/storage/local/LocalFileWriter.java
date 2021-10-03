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

package com.accenture.trac.common.storage.local;

import com.accenture.trac.common.exception.EUnexpected;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.OrderedEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.accenture.trac.common.storage.local.LocalFileErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;
import static com.accenture.trac.common.storage.local.LocalFileStorage.WRITE_OPERATION;
import static java.nio.file.StandardOpenOption.*;


public class LocalFileWriter implements Flow.Subscriber<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final LocalFileErrors errors;
    private final String storagePath;

    private final Path absolutePath;
    private final CompletableFuture<Long> signal;
    private final OrderedEventExecutor executor;

    private final AtomicBoolean subscriptionSet;
    private Flow.Subscription subscription;
    private AsynchronousFileChannel channel;
    private ChunkWriteHandler writeHandler;

    private int chunksPending;
    private long bytesReceived;
    private long bytesWritten;
    private boolean gotComplete;
    private boolean gotError;
    private final boolean gotCancel;

    LocalFileWriter(
            String storageKey, String storagePath,
            Path absolutePath, CompletableFuture<Long> signal, OrderedEventExecutor executor) {

        this.errors = new LocalFileErrors(log, storageKey);
        this.storagePath = storagePath;

        this.absolutePath = absolutePath;
        this.signal = signal;
        this.executor = executor;

        this.subscriptionSet = new AtomicBoolean(false);
        this.subscription = null;

        chunksPending = 0;
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

        // At this point it is certain there is only one subscription
        // The method is still running on the calling thread though
        // That is ok so long as the publisher is well-behaved and doesn't send anything until onSubscribe() completes

        try {

            this.subscription = subscription;

            this.channel = AsynchronousFileChannel.open(absolutePath, Set.of(WRITE, CREATE_NEW), executor);
            this.writeHandler = new ChunkWriteHandler();

            log.info("File channel open for writing: [{}]", absolutePath);

            subscription.request(1);
        }
        catch (Exception e) {

            subscription.cancel();

            var eStorage = errors.handleException(e, storagePath, WRITE_OPERATION);
            signal.completeExceptionally(eStorage);
        }
    }

    @Override
    public void onNext(ByteBuf chunk) {

        // Avoid concurrency issues - ensure all calls are processed in the ordered event loop

        if (!executor.inEventLoop()) {
            executor.submit(() -> this.onNext(chunk));
            return;
        }

        chunksPending += 1;

        if (!(gotComplete || gotError || gotCancel))
            writeChunk(chunk);

        else
            releaseBuffer(chunk);
    }

    @Override
    public void onComplete() {

        // Avoid concurrency issues - ensure all calls are processed in the ordered event loop

        if (!executor.inEventLoop()) {
            executor.submit(this::onComplete);
            return;
        }

        var alreadyComplete = gotComplete;
        gotComplete = true;

        if (chunksPending == 0 && !(gotError || alreadyComplete))
            doComplete();
    }

    @Override
    public void onError(Throwable error) {

        // Avoid concurrency issues - ensure all calls are processed in the ordered event loop

        if (!executor.inEventLoop()) {
            executor.submit(() -> this.onError(error));
            return;
        }

        var alreadyFailed = gotError;
        gotError = true;

        if (!(gotComplete || gotCancel || alreadyFailed))
            handleError(error, false);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // PRIVATE IMPLEMENTATION
    // -----------------------------------------------------------------------------------------------------------------

    // These methods are only ever called on the ordered event executor


    private class ChunkWriteHandler implements CompletionHandler<Integer, ByteBuf> {

        @Override
        public void completed(Integer nBytes, ByteBuf chunk) {

            // Update counts
            chunksPending -= 1;
            bytesWritten += nBytes;

            // The number of bytes in the buffer is the expected number of bytes written
            var bytesExpected = chunk.readableBytes();

            // Chunk can be released whether the operation succeeds or fails - it is no longer needed
            releaseBuffer(chunk);

            // Check if the write operation is already failed or cancelled
            // If so, do not send any further signals
            if (gotError || gotCancel) {

                if (log.isDebugEnabled())
                    log.debug("Ignoring chunks, write operation is failed or terminated: [{}]", absolutePath);
            }

            // If the full content of the buffer was not written, this is an error
            else if (nBytes != bytesExpected) {

                gotError = true;

                var error = errors.chunkNotFullyWritten(bytesExpected, nBytes);
                handleError(error, true);
            }

            // If onComplete has been sent and there are no chunks pending, then the operation is complete
            else if (gotComplete && chunksPending == 0) {

                doComplete();
            }

            // Otherwise, this is a normal write
            else {

                nextChunk();
            }
        }

        @Override
        public void failed(Throwable error, ByteBuf chunk) {

            // Update counts
            chunksPending -= 1;

            // Chunk can be released whether the operation succeeds or fails - it is no longer needed
            releaseBuffer(chunk);

            // Async close exception is sent for operations that were in progress when the channel was closed
            // If the close happened because of a complete or cancel event,
            // then it is safe to ignore these errors

            if (error instanceof AsynchronousCloseException)
                if (gotComplete || gotCancel)
                    return;

            // Make sure not to send multiple onError signals

            var alreadyFailed = gotError;
            gotError = true;

            if (!gotComplete || gotCancel || alreadyFailed)
                handleError(error, true);

            else
                // For errors after termination, put a warning in the log
                log.warn("Write operation is terminated but further errors occurred: [{}]", absolutePath, error);
        }
    }

    private void writeChunk(ByteBuf chunk) {

        try {

            var offset = bytesReceived;
            bytesReceived += chunk.readableBytes();

            if (chunk.nioBufferCount() < 1)
                throw new EUnexpected();

            var nioChunk = chunk.nioBuffer();

            channel.write(nioChunk, offset, chunk, writeHandler);
        }
        catch (Exception e) {

            releaseBuffer(chunk);

            var alreadyGotError = gotError;
            gotError = true;

            // Only report errors to the subscriber if there is no previous termination signal
            if (!(gotComplete || gotCancel || alreadyGotError))
                handleError(e, true);

            else
                // For errors after termination, put a warning in the log
                log.warn("Write operation is terminated but further errors occurred: [{}]", absolutePath, e);
        }
    }

    private void nextChunk() {

        try {

            subscription.request(1);
        }
        catch (Exception e) {

            var alreadyGotError = gotError;
            gotError = true;

            // Only report errors to the subscriber if there is no previous termination signal
            if (!(gotComplete || gotCancel || alreadyGotError))
                handleError(e, true);

            else
                // For errors after termination, put a warning in the log
                log.warn("Write operation is terminated but further errors occurred: [{}]", absolutePath, e);
        }
    }

    private void doComplete() {

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
                log.error("Write operation stopped due to an error: {} [{}]", error.getMessage(), absolutePath, eWrapped);

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
}
