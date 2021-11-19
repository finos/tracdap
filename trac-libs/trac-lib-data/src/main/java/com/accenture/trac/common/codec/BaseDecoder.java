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

package com.accenture.trac.common.codec;

import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.exception.EDataCorruption;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletionException;


public abstract class BaseDecoder extends BaseProcessor<ByteBuf, DataBlock> implements ICodec.Decoder {

    protected static final boolean STREAMING_DECODER = true;
    protected static final boolean BUFFERED_DECODER = false;

    private static final DataBlock END_OF_STREAM = DataBlock.forRecords(null);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final boolean isStreaming;
    private final CompositeByteBuf buffer;
    private final Queue<DataBlock> outQueue;

    private boolean started = false;
    private long nBytes;
    private long nRows;
    private int nBatches;

    protected abstract void decodeStart();
    protected abstract void decodeChunk(ByteBuf chunk);
    protected abstract void decodeEnd();

    protected BaseDecoder(boolean isStreaming) {

        super(ReferenceCounted::release);

        this.isStreaming = isStreaming;
        this.buffer = ByteBufAllocator.DEFAULT.compositeBuffer();
        this.outQueue = new ArrayDeque<>();
    }

    protected final void emitBlock(DataBlock block) {

        outQueue.add(block);

        if (block.arrowRecords != null) {
            nRows += block.arrowRecords.getLength();
            nBatches += 1;
        }
    }

    @Override
    protected final void handleTargetSubscribe() {

        // No-op
    }

    @Override
    protected final void handleTargetRequest() {

        deliverPendingBlocks();

        if (nTargetRequested() > nTargetDelivered() && nSourceRequested() <= nSourceDelivered())
            doSourceRequest(1);
    }

    @Override
    protected final void handleTargetCancel() {

        log.warn("DECODE CANCELLED");

        doSourceCancel();
    }

    @Override
    protected final void handleSourceSubscribe() {

        // No-op
    }

    @Override
    protected final void handleSourceNext(ByteBuf chunk) {

        var chunkDelivered = false;

        try {

            checkStarted();

            if (chunk.readableBytes() > 0) {

                nBytes += chunk.readableBytes();
                chunkDelivered = true;

                if (isStreaming)
                    decodeChunk(chunk);
                else
                    buffer.addComponent(true, chunk);
            }

            deliverPendingBlocks();

            if (nTargetRequested() > nTargetDelivered() && nSourceRequested() <= nSourceDelivered())
                doSourceRequest(1);
        }
        catch (Throwable error) {

            log.error("DECODE FAILED: " + error.getMessage());

            if (!chunkDelivered)
                chunk.release();

            doTargetError(error);
        }
    }

    @Override
    protected final void handleSourceComplete() {

        try {

            checkStarted();

            if (nBytes == 0) {
                log.error("Data stream contains zero bytes");
                throw new EDataCorruption("Data stream contains zero bytes");
            }

            if (!isStreaming)
                decodeChunk(buffer.retain());

            decodeEnd();
            outQueue.add(END_OF_STREAM);

            log.info("DECODE SUCCEEDED, {} rows in {} batches", nRows, nBatches);

            deliverPendingBlocks();
        }
        catch (Throwable error) {

            log.error("DECODE FAILED: " + error.getMessage());

            doTargetError(error);
        }
    }

    @Override
    protected final void handleSourceError(Throwable error) {

        // Stack trac is logged at original error site and again in outbound gRPC handler
        // Do not log the same stack trace multiple times

        log.error("DECODE FAILED: Error in source data stream: " + error.getMessage());

        var completionError = error instanceof CompletionException
                ? error
                : new CompletionException(error.getMessage(), error);

        doTargetError(completionError);
    }

    @Override
    public void close() {

        // Release chunk buffer
        var releaseOk = buffer.release();

        if (!releaseOk && buffer.capacity() > 0)
            log.warn("Decode buffer was not released (this could indicate a memory leak)");

        // Release any blocks still in the out queue

        var block = outQueue.poll();

        while (block != null) {

            if (block.arrowRecords != null)
                block.arrowRecords.close();

            if (block.arrowDictionary != null)
                block.arrowDictionary.close();

            block = outQueue.poll();
        }
    }

    private void checkStarted() {

        if (!started) {
            log.info("DECODE START");
            started = true;
            decodeStart();
        }
    }

    private void deliverPendingBlocks() {

        while (nTargetDelivered() < nTargetRequested()) {

            var block = outQueue.poll();

            if (block == END_OF_STREAM)
                doTargetComplete();

            else if (block != null)
                doTargetNext(block);

            else
                return;
        }
    }
}
