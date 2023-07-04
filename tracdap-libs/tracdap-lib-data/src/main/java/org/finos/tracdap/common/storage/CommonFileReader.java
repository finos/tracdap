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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.finos.tracdap.common.storage.CommonFileStorage.READ_OPERATION;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.DUPLICATE_SUBSCRIPTION;


public abstract class CommonFileReader implements Flow.Publisher<ArrowBuf> {

    private static final long DEFAULT_CHUNK_SIZE = 2 * 1048576;  // 2 MB
    private static final int DEFAULT_CHUNK_BUFFER_TARGET = 2;
    private static final int DEFAULT_CLIENT_BUFFER_TARGET = 32;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final IDataContext dataContext;
    private final BufferAllocator allocator;
    private final StorageErrors errors;

    private final String storageKey;
    private final String storagePath;

    private final long chunkSize;
    private final int chunkBufferTarget;
    private final int clientBufferTarget;

    private final AtomicBoolean subscriberSet;
    private Flow.Subscriber<? super ArrowBuf> subscriber;

    private final Queue<ArrowBuf> pendingChunks;
    private ArrowBuf currentChunk;

    private long nRequested;
    private long nDelivered;
    private long clientRequested;
    private long clientReceived;
    private long bytesReceived;
    private boolean gotComplete;
    private boolean gotCancel;
    private boolean gotError;

    protected abstract void clientStart();
    protected abstract void clientRequest(long n);
    protected abstract void clientCancel();


    protected CommonFileReader(
            IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath,
            long chunkSize, int chunkBufferTarget, int clientBufferTarget) {

        this.dataContext = dataContext;
        this.allocator = dataContext.arrowAllocator();
        this.errors = errors;

        this.storageKey = storageKey;
        this.storagePath = storagePath;

        this.chunkSize = chunkSize;
        this.chunkBufferTarget = chunkBufferTarget;
        this.clientBufferTarget = clientBufferTarget;

        this.subscriberSet = new AtomicBoolean(false);
        this.pendingChunks = new ArrayDeque<>();
    }

    protected CommonFileReader(
            IDataContext dataContext, StorageErrors errors,
            String storageKey, String storagePath) {

        this(dataContext, errors, storageKey, storagePath,
                DEFAULT_CHUNK_SIZE,
                DEFAULT_CHUNK_BUFFER_TARGET,
                DEFAULT_CLIENT_BUFFER_TARGET);
    }

    @Override
    public final void subscribe(Flow.Subscriber<? super ArrowBuf> subscriber) {

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

        dataContext.eventLoopExecutor().submit(this::start);

        // Now activate the subscription, before doStart gets executed
        // This approach allows errors to be reported normally during onStart (e.g. file not found)
        // Otherwise, if the subscription is not yet active, errors should be reported with IllegalStateException
        // File not found is an expected error, reporting it with EStorage makes for cleaner error handling

        subscriber.onSubscribe(new Subscription());
    }

    private class Subscription implements Flow.Subscription {

        @Override
        public void request(long n) {
            dataContext.eventLoopExecutor().submit(() -> request(n));
        }

        @Override
        public void cancel() {
            dataContext.eventLoopExecutor().submit(this::cancel);
        }
    }

    private void start() {

        try {

            clientStart();

            var initialRequest = (long) 2 * clientBufferTarget;

            clientRequested = initialRequest;
            clientRequest(initialRequest);
        }
        catch (Exception e) {

        }
    }

    private void request(long n) {

        // Do not accept the request if the read operation has finished for any reason
        if (gotComplete || gotError || gotCancel)
            return;

        try {

            nRequested += n;

            sendPendingChunks();

            if (pendingChunks.size() < chunkBufferTarget) {
                if (clientRequested - clientReceived < clientBufferTarget) {
                    clientRequested += clientBufferTarget;
                    clientRequest(clientBufferTarget);
                }
            }
        }
        catch (Exception e) {

        }
    }

    private void cancel() {

        // Do not process cancellation if the read operation has finished for any reason
        if (gotComplete || gotError || gotCancel)
            return;

        try {

            log.info("READ CANCELLED: [{}]", storagePath);

            gotCancel = true;

            clientCancel();

            // Do not send any signal to the subscriber for a clean cancel
        }
        catch (Exception e) {

            log.error("There was an error cancelling the read operation: {} [{}]", e.getMessage(), storagePath, e);

            gotError = true;

            // If the cancel results in an error closing the file, do not send the onError message
        }
        finally {

            releasePendingChunks();
        }
    }

    protected final boolean isDone() {

        return gotError || gotCancel || gotComplete;
    }

    protected final ArrowBuf allocateChunk(long size) {

        return allocator.buffer(size);
    }

    protected final void onChunk(ByteBuffer chunk) {

        try {

            clientReceived += 1;
            bytesReceived += chunk.remaining();

            while (chunk.remaining() > 0) {

                if (currentChunk == null)
                    currentChunk = allocateChunk(chunkSize);

                var nBytes = (int) Math.min(chunk.remaining(), currentChunk.writableBytes());
                var newPosition = chunk.position() + nBytes;

                currentChunk.setBytes(currentChunk.writerIndex(), chunk, chunk.position(), nBytes);
                currentChunk.writerIndex(currentChunk.writerIndex() + nBytes);

                chunk.position(newPosition);

                if (currentChunk.writableBytes() == 0) {
                    sendChunk(currentChunk);
                    currentChunk = null;
                }
            }
        }
        catch (Exception e) {

        }
    }

    protected final void onChunk(ArrowBuf chunk) {

        try {

            clientReceived += 1;
            bytesReceived += chunk.readableBytes();

            sendChunk(chunk);
        }
        catch (Exception e) {

        }
    }

    protected final void onComplete() {

        if (gotError || gotCancel || gotComplete)
            return;

        if (currentChunk != null) {
            sendChunk(currentChunk);
            currentChunk = null;
        }

        if (pendingChunks.isEmpty())
            subscriber.onComplete();
        else
            gotComplete = true;
    }

    protected final void onError(Throwable error) {

        try {

            var tracError = errors.handleException(READ_OPERATION, storagePath, error);

            if (gotError) {
                log.warn("{} {} [{}]: Read operation already failed, then another error occurred",
                        READ_OPERATION, storageKey, storagePath, tracError);
            }
            else if (gotCancel) {
                log.warn("{} {} [{}]: Read operation was cancelled, then an error occurred",
                        READ_OPERATION, storageKey, storagePath, tracError);
            }
            else {
                log.error("{} {} [{}]: {}",
                        READ_OPERATION, storageKey, storagePath, tracError.getMessage(), tracError);

                gotError = true;
                subscriber.onError(tracError);
            }
        }
        finally {
            releasePendingChunks();
        }
    }

    private void sendChunk(ArrowBuf chunk) {

        if (nDelivered < nRequested && pendingChunks.isEmpty()) {
            nDelivered += 1;
            subscriber.onNext(chunk);
        }
        else {
            pendingChunks.add(chunk);
        }
    }

    private void sendPendingChunks() {

        while (nDelivered < nRequested && !pendingChunks.isEmpty()) {
            nDelivered += 1;
            subscriber.onNext(pendingChunks.remove());
        }

        if (pendingChunks.isEmpty() && gotComplete) {
            // Clear the flag, in case sendPendingChunks() is queued multiple times on the executor
            gotComplete = false;
            subscriber.onComplete();
        }
    }

    private void releasePendingChunks() {

        while (!pendingChunks.isEmpty())
            pendingChunks.remove().close();

        if (currentChunk != null) {
            currentChunk.close();
            currentChunk = null;
        }
    }
}
