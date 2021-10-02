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

import java.io.IOException;
import java.nio.channels.*;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

            channel = AsynchronousFileChannel.open(absolutePath, Set.of(WRITE, CREATE_NEW), executor);

            subscription.request(2);
        }
        catch (IOException e) {

            throw errors.handleException(e, storagePath, WRITE_OPERATION);
        }
    }

    @Override
    public void onNext(ByteBuf chunk) {

        chunksPending += 1;

        channel.write(chunk.nioBuffer(), 0, chunk, new CompletionHandler<>() {

            @Override
            public void completed(Integer nBytes, ByteBuf chunk) {

                chunk.release();
                chunksPending -= 1;
                bytesWritten += nBytes;

                // TODO: proper logic!

                if (gotComplete && chunksPending == 0)
                    signal.complete(bytesWritten);
                else
                    subscription.request(1);
            }

            @Override
            public void failed(Throwable error, ByteBuf chunk) {

                chunk.release();
                chunksPending -= 1;
                gotError = true;

                subscription.cancel();
                signal.completeExceptionally(error);  // TODO: Wrap error
            }
        });
    }

    @Override
    public void onError(Throwable error) {

        gotError = true;

        subscription.cancel();
        signal.completeExceptionally(error);  // TODO: Wrap error
    }

    @Override
    public void onComplete() {

        gotComplete = true;

        if (chunksPending == 0 && !gotError)
            signal.complete(bytesWritten);
    }
}
