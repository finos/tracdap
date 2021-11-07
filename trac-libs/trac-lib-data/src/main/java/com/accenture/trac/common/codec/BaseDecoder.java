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

import com.accenture.trac.common.concurrent.flow.CommonBaseProcessor;
import com.accenture.trac.common.data.DataBlock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;


public abstract class BaseDecoder extends CommonBaseProcessor<ByteBuf, DataBlock> implements ICodec.Decoder {

    private static final DataBlock END_OF_STREAM = DataBlock.eos();

    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final CompositeByteBuf buffer;
    protected final Queue<DataBlock> outQueue;
    private boolean started = false;

    protected abstract void decodeFirstChunk();
    protected abstract void decodeChunk();
    protected abstract void decodeLastChunk();

    protected BaseDecoder() {

        this.buffer = ByteBufAllocator.DEFAULT.compositeBuffer();  // todo: allocator needed for composites or no?
        this.outQueue = new ArrayDeque<>();
    }

    @Override
    protected void handleTargetRequest() {

        try {
            deliverPendingBlocks();

            if (nTargetRequested() > nTargetDelivered() && nSourceRequested() <= nSourceDelivered())
                doSourceRequest(1);
        }
        catch (Throwable e) {
            releaseBuffer();
            releasePendingChunks();
            throw e;
        }
    }

    @Override
    protected void handleTargetCancel() {

        try {
            doSourceCancel();
        }
        finally {
            releaseBuffer();
            releasePendingChunks();
        }
    }

    @Override
    protected void handleSourceNext(ByteBuf chunk) {

        try {
            buffer.addComponent(true, chunk);
            doSourceRequest(1);

            if (!started) {
                started = true;
                decodeFirstChunk();
            }
            else
                decodeChunk();
        }
        catch (Throwable e) {
            releaseBuffer();
            releasePendingChunks();
            throw e;
        }
    }

    @Override
    protected void handleSourceComplete() {

        try {

            decodeLastChunk();
            outQueue.add(END_OF_STREAM);

            deliverPendingBlocks();
        }
        catch (Throwable e) {
            releasePendingChunks();
        }
        finally {
            releaseBuffer();
        }
    }

    @Override
    protected void handleSourceError(Throwable error) {

        try {
            log.error(error.getMessage(), error);

            doTargetError(error);  // todo
        }
        finally {
            releaseBuffer();
            releasePendingChunks();
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

        var releaseOk = buffer.release();

        if (!releaseOk && buffer.capacity() > 0)
            log.warn("CSV decode buffer was not released (this could indicate a memory leak)");
    }

    private void releasePendingChunks() {

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
