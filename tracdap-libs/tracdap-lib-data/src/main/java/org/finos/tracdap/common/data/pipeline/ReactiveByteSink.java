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

package org.finos.tracdap.common.data.pipeline;

import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Flow;

public class ReactiveByteSink extends BaseSinkStage implements DataPipeline.ByteStreamConsumer {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final ByteBuf EOS = Unpooled.EMPTY_BUFFER;

    private final DataPipelineImpl pipeline;
    private final Flow.Subscriber<ByteBuf> sink;

    private final Queue<ByteBuf> chunkBuffer;
    private int chunksRequested;
    private int chunksDelivered;

    private Flow.Subscription subscription;

    ReactiveByteSink(DataPipelineImpl pipeline, Flow.Subscriber<ByteBuf> sink) {

        super(pipeline);

        this.pipeline = pipeline;
        this.sink = sink;

        this.chunkBuffer = new ArrayDeque<>();
        this.chunksRequested = 0;
        this.chunksDelivered = 0;
    }

    @Override
    public void start() {

        if (subscription != null)
            throw new EUnexpected();

        this.subscription = new Subscription();
        sink.onSubscribe(subscription);
    }

    @Override
    public boolean poll() {

        return chunkBuffer.isEmpty() && chunksRequested > chunksDelivered;
    }

    @Override
    public void onStart() {

        // No-op, already subscribed
    }

    @Override
    public void onNext(ByteBuf chunk) {

        try {

            var chunksPending = chunksRequested - chunksDelivered;

            if (chunksPending > 0 && chunkBuffer.isEmpty()) {
                chunksDelivered += 1;
                sink.onNext(chunk);
            } else {
                chunkBuffer.add(chunk);
                deliverPendingChunks();
            }
        }
        catch (Throwable error) {

            emitFailed(error);
        }
    }

    @Override
    public void onComplete() {

        try {

            if (chunkBuffer.isEmpty()) {
                sink.onComplete();
                pipeline.markComplete();
                close();
            } else {
                chunkBuffer.add(EOS);
                deliverPendingChunks();
            }
        }
        catch (Throwable error) {

            emitFailed(error);
        }
    }

    @Override
    public void onError(Throwable error) {

        emitFailed(error);
    }

    @Override
    public void emitComplete() {

        try {
            sink.onComplete();
            super.emitComplete();
        }
        finally {
            close();
        }

    }

    @Override
    public void emitFailed(Throwable error) {

        try {

            if (subscription != null)
                sink.onError(error);

            close();
        }
        finally {
            super.emitFailed(error);
        }
    }

    private class Subscription implements Flow.Subscription {

        @Override
        public void request(long n) {

            chunksRequested += n;
            deliverPendingChunks();

            if (chunksRequested > chunksDelivered)
                pipeline.feedData();
        }

        @Override
        public void cancel() {

            try {
                pipeline.cancel();
            }
            finally {
                close();
            }
        }
    }

    @Override
    public void close() {

        while(!chunkBuffer.isEmpty()) {
            chunkBuffer.remove().release();
        }
    }

    private void deliverPendingChunks() {

        var chunksPending = chunksRequested - chunksDelivered;

        while (chunksPending > 0 && !chunkBuffer.isEmpty()) {

            var chunk = chunkBuffer.remove();

            if (chunk == EOS) {
                emitComplete();
                return;
            }

            chunksDelivered += 1;
            sink.onNext(chunk);

            chunksPending = chunksRequested - chunksDelivered;
        }
    }
}
