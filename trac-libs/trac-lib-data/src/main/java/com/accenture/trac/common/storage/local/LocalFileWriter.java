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

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.*;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;

import static com.accenture.trac.common.storage.local.LocalFileStorage.WRITE_OPERATION;
import static java.nio.file.StandardOpenOption.*;


public class LocalFileWriter implements Flow.Subscriber<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final LocalFileErrors errors;
    private final String storagePath;

    private final Path absolutePath;
    private final CompletableFuture<Long> signal;
    private final ExecutorService executor;

    private Flow.Subscription subscription;
    private AsynchronousFileChannel channel;
    private ChunkWriteHandler writeHandler;

    private int chunksPending;
    private long bytesWritten;
    private boolean gotComplete;
    private boolean gotError;

    LocalFileWriter(
            String storageKey, String storagePath,
            Path absolutePath, CompletableFuture<Long> signal, ExecutorService executor) {

        this.errors = new LocalFileErrors(log, storageKey);
        this.storagePath = storagePath;

        this.absolutePath = absolutePath;
        this.signal = signal;
        this.executor = executor;

        chunksPending = 0;
        bytesWritten = 0;
        gotComplete = false;
        gotError = false;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        try {

            this.subscription = subscription;

            this.channel = AsynchronousFileChannel.open(absolutePath, Set.of(WRITE, CREATE_NEW), executor);
            this.writeHandler = new ChunkWriteHandler();

            log.info("File channel open for writing: [{}]", absolutePath);

            subscription.request(2);
        }
        catch (Exception e) {

            subscription.cancel();

            var eStorage = errors.handleException(e, storagePath, WRITE_OPERATION);
            signal.completeExceptionally(eStorage);
        }
    }

    @Override
    public void onNext(ByteBuf chunk) {

        try {

            chunksPending += 1;
            channel.write(chunk.nioBuffer(), 0, chunk, writeHandler);
        }
        catch (Exception e) {

            subscription.cancel();

            var eStorage = errors.handleException(e, storagePath, WRITE_OPERATION);
            signal.completeExceptionally(eStorage);
        }
    }

    @Override
    public void onError(Throwable error) {

        gotError = true;

        doOnError(error);
    }

    @Override
    public void onComplete() {

        gotComplete = true;

        if (chunksPending == 0 && !gotError)
            doOnComplete();
    }

    private class ChunkWriteHandler implements CompletionHandler<Integer, ByteBuf> {

        @Override
        public void completed(Integer nBytes, ByteBuf chunk) {

            var releaseOk = chunk.release();

            if (!releaseOk && chunk.capacity() > 0)
                log.warn("Chunk buffer was not released (this could indicate a memory leak)");

            chunksPending -= 1;
            bytesWritten += nBytes;

            if (gotComplete) {
                if (chunksPending == 0 && !gotError)
                    doOnComplete();
            }
            else
                if (!gotError)
                    subscription.request(1);
        }

        @Override
        public void failed(Throwable error, ByteBuf chunk) {

            var releaseOk = chunk.release();

            if (!releaseOk && chunk.capacity() > 0)
                log.warn("Chunk buffer was not released (this could indicate a memory leak)");

            chunksPending -= 1;
            gotError = true;

            doOnError(error);
        }
    }

    private void doOnComplete() {

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

    private void doOnError(Throwable throwable) {

        var error = throwable instanceof Exception
                ? (Exception) throwable
                : new ExecutionException(throwable);

        try {

            log.error("Write operation failed: {} [{}]", throwable.getMessage(), absolutePath, throwable);

            subscription.cancel();
            channel.close();

            log.info("File channel closed: [{}]", absolutePath);

            var eStorage = errors.handleException(error, storagePath, WRITE_OPERATION);
            signal.completeExceptionally(eStorage);
        }
        catch (Exception e) {

            log.error("File channel was not closed cleanly: {} [{}]", e.getMessage(), absolutePath, e);

            // Report the original error back up the chain, not the secondary error that occurred on close

            var eStorage = errors.handleException(error, storagePath, WRITE_OPERATION);
            signal.completeExceptionally(eStorage);
        }
    }
}
