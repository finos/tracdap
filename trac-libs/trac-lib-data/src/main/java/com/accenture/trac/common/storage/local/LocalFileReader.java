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

import com.accenture.trac.common.exception.EStorage;
import com.accenture.trac.common.exception.EUnexpected;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;

import static java.nio.file.StandardOpenOption.*;


public class LocalFileReader implements Flow.Publisher<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Path absolutePath;
    private final ByteBufAllocator allocator;
    private final ExecutorService executor;

    private final ReadSubscription subscription;
    private final ReadHandler handler;

    private AsynchronousFileChannel channel;
    private Flow.Subscriber<? super ByteBuf> subscriber;

    private int chunkSize = 4096;  // TODO: Size
    private int chunksPending;
    private long bytesRead;
    private boolean gotComplete;
    private boolean gotError;

    LocalFileReader(Path absolutePath, ByteBufAllocator allocator, ExecutorService executor) {

        this.absolutePath = absolutePath;
        this.allocator = allocator;
        this.executor = executor;

        this.subscription = new ReadSubscription();
        this.handler = new ReadHandler();

        chunksPending = 0;
        bytesRead = 0;
        gotComplete = false;
        gotError = false;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuf> subscriber) {

        try {

            channel = AsynchronousFileChannel.open(absolutePath, Set.of(READ), executor);

            this.subscriber = subscriber;
            subscriber.onSubscribe(subscription);
        }
        catch (IOException e) {

            throw new EStorage("", e);  // TODO: Error
        }
    }

    private class ReadSubscription implements Flow.Subscription {

        @Override
        public void request(long n) {

            var buffer = allocator.ioBuffer(chunkSize);

            if (buffer.nioBufferCount() != 1)
                throw new EUnexpected();

            var nio = buffer.nioBuffer(0, chunkSize);

            channel.read(nio, bytesRead, buffer, handler);
        }

        @Override
        public void cancel() {

            doClose();
        }
    }

    private class ReadHandler implements CompletionHandler<Integer, ByteBuf> {

        @Override
        public void completed(Integer nBytes, ByteBuf buffer) {

            if (nBytes >= 0) {
                bytesRead += nBytes;
                buffer.writerIndex(nBytes);
                subscriber.onNext(buffer);
            }
            else {
                doClose();
                subscriber.onComplete();
            }
        }

        @Override
        public void failed(Throwable error, ByteBuf buffer) {

            try {
                doClose();
                //buffer.
            }
            finally {
                subscriber.onError(error);
            }
        }
    }

    private void doClose() {

        try {
            channel.close();
        }
        catch (IOException e) {
            // TODO
        }
    }

}
