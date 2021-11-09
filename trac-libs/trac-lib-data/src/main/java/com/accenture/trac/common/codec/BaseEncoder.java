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
import com.accenture.trac.common.exception.EUnexpected;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletionException;


public abstract class BaseEncoder extends CommonBaseProcessor<DataBlock, ByteBuf> implements ICodec.Encoder {

    private static final ByteBuf END_OF_STREAM = new EmptyByteBuf(ByteBufAllocator.DEFAULT);

    private final Queue<ByteBuf> outQueue;

    protected abstract void encodeSchema(Schema arrowSchema);
    protected abstract void encodeRecords(ArrowRecordBatch batch);
    protected abstract void encodeDictionary(ArrowDictionaryBatch batch);
    protected abstract void encodeEos();

    protected BaseEncoder() {

        this.outQueue = new ArrayDeque<>();
    }

    protected void emitChunk(ByteBuf chunk) {
        outQueue.add(chunk);
    }

    @Override
    protected final void handleTargetRequest() {

        deliverPendingChunks();

        if (nTargetRequested() > nTargetDelivered() && nSourceRequested() <= nSourceDelivered())
            doSourceRequest(1);
    }

    @Override
    protected final void handleTargetCancel() {

        try {
            doSourceCancel();
        }
        finally {
            releaseOutQueue();
        }
    }

    @Override
    protected final void handleSourceNext(DataBlock block) {

        if (block.arrowSchema != null)
            encodeSchema(block.arrowSchema);

        else if (block.arrowRecords != null)
            encodeRecords(block.arrowRecords);

        else
            throw new EUnexpected();  // TODO: Error

        doSourceRequest(1);
        deliverPendingChunks();
    }

    @Override
    protected final void handleSourceComplete() {

        encodeEos();
        outQueue.add(END_OF_STREAM);

        deliverPendingChunks();
    }

    @Override
    protected final void handleSourceError(Throwable error) {

        try {
            var completionError = error instanceof CompletionException
                    ? error
                    : new CompletionException(error.getMessage(), error);

            doTargetError(completionError);
        }
        finally {
            releaseOutQueue();
        }
    }

    private void deliverPendingChunks() {

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

    private void releaseOutQueue() {

        while (!outQueue.isEmpty()) {

            var chunk = outQueue.poll();

            if (chunk != null)
                chunk.release();
        }
    }
}
