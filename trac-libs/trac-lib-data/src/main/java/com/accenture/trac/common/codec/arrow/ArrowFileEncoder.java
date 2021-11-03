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

package com.accenture.trac.common.codec.arrow;

import com.accenture.trac.common.concurrent.flow.CommonBaseProcessor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowMessage;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;


public class ArrowFileEncoder extends CommonBaseProcessor<ArrowRecordBatch, ByteBuf> {

    private final Queue<ByteBuf> outQueue;
    private final WritableByteChannel outChannel;

    private final VectorSchemaRoot root;
    private final VectorLoader loader;
    private final ArrowFileWriter arrowWriter;


    public ArrowFileEncoder() {

        outQueue = new ArrayDeque<>();
        outChannel = new ByteBufChannel();

        root = new VectorSchemaRoot(List.of(), List.of());  // TODO: schema and empty vectors
        loader = new VectorLoader(root);
        arrowWriter = new ArrowFileWriter(root, null, outChannel);
    }

    @Override
    protected void handleTargetRequest() {

        while (nTargetDelivered() < nTargetRequested() && !outQueue.isEmpty()) {
            var chunk = outQueue.remove();
            doTargetOnNext(chunk);
        }

        if (nTargetDelivered() < nTargetRequested()) {

            if (outChannel.isOpen())
                doSourceRequest(1);  // TODO: How many batches?

            else
                doTargetOnComplete();
        }
    }

    @Override
    protected void handleSourceNext(ArrowRecordBatch batch) {

        try {

            loader.load(batch);
            arrowWriter.writeBatch();

            root.clear();
            batch.close();

            while (nTargetDelivered() < nTargetRequested() && !outQueue.isEmpty()) {
                var chunk = outQueue.remove();
                doTargetOnNext(chunk);
            }

            // if (nTargetDelivered() < nTargetRequested())
        }
        catch (IOException e) {

            // TODO
        }
    }

    @Override
    protected void handleSourceComplete() {

        try {
            arrowWriter.end();

            while (nTargetDelivered() < nTargetRequested() && !outQueue.isEmpty()) {
                var chunk = outQueue.remove();
                doTargetOnNext(chunk);
            }

            if (outQueue.isEmpty())
                doTargetOnComplete();
        }
        catch (IOException e) {

            // TODO
        }
    }

    private class ByteBufChannel implements WritableByteChannel {

        @Override
        public int write(ByteBuffer src) throws IOException {

            var outBuf = Unpooled.wrappedBuffer(src);
            outQueue.add(outBuf);

            return outBuf.readableBytes();
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {

        }
    }
}
