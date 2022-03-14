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

package org.finos.tracdap.common.codec;

import org.finos.tracdap.common.data.DataBlock;
import org.finos.tracdap.common.exception.EDataCorruption;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletionException;


public abstract class BaseEncoder extends BaseProcessor<DataBlock, ByteBuf> implements ICodec.Encoder {

    private static final ByteBuf END_OF_STREAM = new EmptyByteBuf(ByteBufAllocator.DEFAULT);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Queue<ByteBuf> outQueue;
    private boolean schemaReceived;
    private long nRows;
    private long nBatches;

    protected abstract void encodeSchema(Schema arrowSchema);
    protected abstract void encodeRecords(ArrowRecordBatch batch);
    protected abstract void encodeDictionary(ArrowDictionaryBatch batch);
    protected abstract void encodeEos();

    protected BaseEncoder() {

        super(DataBlock::close);

        this.outQueue = new ArrayDeque<>();
    }

    protected final void emitChunk(ByteBuf chunk) {
        outQueue.add(chunk);
    }

    @Override
    protected final void handleTargetSubscribe() {

        // No-op
    }

    @Override
    protected final void handleTargetRequest() {

        deliverPendingChunks();

        if (nTargetRequested() > nTargetDelivered() && nSourceRequested() <= nSourceDelivered())
            doSourceRequest(1);
    }

    @Override
    protected final void handleTargetCancel() {

        log.warn("ENCODE CANCELLED");

        doSourceCancel();
    }

    @Override
    protected final void handleSourceSubscribe() {

        // No-op
    }

    @Override
    protected final void handleSourceNext(DataBlock block) {

        var blockDelivered = false;

        try {

            if (!schemaReceived) {

                if (block.arrowSchema != null) {

                    log.info("ENCODE START");

                    schemaReceived = true;
                    encodeSchema(block.arrowSchema);
                }
                else {

                    var err = "Invalid data stream, schema is missing";
                    log.error(err);
                    throw new EDataCorruption(err);
                }
            }
            else {

                if (block.arrowRecords != null) {

                    blockDelivered = true;
                    encodeRecords(block.arrowRecords);

                    nRows += block.arrowRecords.getLength();
                    nBatches += 1;
                }
                else if (block.arrowDictionary != null) {

                    blockDelivered = true;
                    encodeDictionary(block.arrowDictionary);
                }
                else if (block.arrowSchema != null) {

                    var err = "Invalid data stream, duplicate schema";
                    log.error(err);
                    throw new EDataCorruption(err);
                }
                else  {

                    var err = "Invalid data stream, empty block";
                    log.error(err);
                    throw new EDataCorruption(err);
                }
            }

            doSourceRequest(1);
            deliverPendingChunks();
        }
        catch (Exception error) {

            log.error("ENCODE FAILED: " + error.getMessage());

            if (!blockDelivered)
                block.close();

            doTargetError(error);
        }
    }

    @Override
    protected final void handleSourceComplete() {

        try {

            // Data stream must contain at least the schema,
            // but can contain zero record or dictionary blocks

            if (!schemaReceived) {

                var err = "Invalid data stream, schema is missing";
                log.error(err);
                throw new EDataCorruption(err);
            }

            encodeEos();
            outQueue.add(END_OF_STREAM);

            log.info("ENCODE SUCCEEDED: {} rows, {} batches", nRows, nBatches);

            deliverPendingChunks();
        }
        catch (Exception error) {

            log.error("DECODE FAILED: " + error.getMessage());

            doTargetError(error);
        }
    }

    @Override
    protected final void handleSourceError(Throwable error) {

        log.error("ENCODE FAILED: Error in source data stream: " + error.getMessage());

        var completionError = error instanceof CompletionException
                ? error
                : new CompletionException(error.getMessage(), error);

        doTargetError(completionError);
    }

    @Override
    public void close() {

        while (!outQueue.isEmpty()) {

            var chunk = outQueue.poll();

            if (chunk != null)
                chunk.release();
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
}
