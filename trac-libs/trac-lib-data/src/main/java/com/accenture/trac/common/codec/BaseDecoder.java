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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;


public abstract class BaseDecoder extends BaseProcessor<ByteBuf, DataBlock> implements ICodec.Decoder {

    protected static final boolean STREAMING_DECODER = true;
    protected static final boolean BUFFERED_DECODER = false;

    protected abstract void decodeStart();
    protected abstract void decodeChunk(ByteBuf chunk);
    protected abstract void decodeLastChunk();

    private static final DataBlock END_OF_STREAM = DataBlock.eos();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final boolean isStreaming;
    private final CompositeByteBuf buffer;
    private final Queue<DataBlock> outQueue;

    private boolean started = false;
    private boolean released = false;
    private long nBytes;
    private long nRows;
    private int nBatches;

    protected BaseDecoder(boolean isStreaming) {

        this.isStreaming = isStreaming;
        this.buffer = ByteBufAllocator.DEFAULT.compositeBuffer();  // todo: allocator needed for composites or no?
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
    protected final void handleTargetRequest() {

        try {
            deliverPendingBlocks();

            if (nTargetRequested() > nTargetDelivered() && nSourceRequested() <= nSourceDelivered())
                doSourceRequest(1);
        }
        catch (Throwable e) {
            releaseBuffer();
            releaseOutQueue();
            throw e;
        }
    }

    @Override
    protected final void handleTargetCancel() {

        try {
            doSourceCancel();
        }
        finally {
            releaseBuffer();
            releaseOutQueue();
        }
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
        catch (Throwable e) {

            if (!chunkDelivered)
                chunk.release();

            releaseOutQueue();
            releaseBuffer();
            throw e;
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

            decodeLastChunk();
            outQueue.add(END_OF_STREAM);

            log.info("DECODE SUCCEEDED, {} rows in {} batches", nRows, nBatches);

            deliverPendingBlocks();
        }
        catch (Throwable e) {
            releaseOutQueue();
            throw e;
        }
        finally {
            releaseBuffer();
        }
    }

    @Override
    protected final void handleSourceError(Throwable error) {

        try {
            log.error(error.getMessage(), error);

            doTargetError(error);  // todo
        }
        finally {
            releaseOutQueue();
            releaseBuffer();
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

    private void releaseBuffer() {

        if (!released) {

            var releaseOk = buffer.release();
            released = true;

            if (!releaseOk && buffer.capacity() > 0)
                log.warn("CSV decode buffer was not released (this could indicate a memory leak)");
        }
    }

    private void releaseOutQueue() {

        var block = outQueue.poll();

        while (block != null) {

            if (block.arrowRecords != null)
                block.arrowRecords.close();

            if (block.arrowDictionary != null)
                block.arrowDictionary.close();

            block = outQueue.poll();
        }
    }
}
