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

package org.finos.tracdap.common.codec.arrow;

import org.apache.arrow.vector.ipc.message.MessageStreamReader;
import org.finos.tracdap.common.codec.StreamingDecoder;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETracInternal;

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;


public class ArrowStreamDecoder extends StreamingDecoder implements DataPipeline.StreamApi {

    // Pure streaming implementation of the arrow stream codec
    // Uses MessageStreamReader to process incoming bytes and determine when messages are available
    // Once messages arrive, defer to the regular ArrowStreamReader provided by the core Arrow libraries

    private final BufferAllocator allocator;

    private MessageStreamReader messageReader;
    private ArrowReader arrowReader;
    private VectorSchemaRoot root;

    public ArrowStreamDecoder(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void onStart() {
        messageReader = new MessageStreamReader(allocator);
        arrowReader = new ArrowStreamReader(messageReader, allocator);
    }

    @Override
    public void onNext(ArrowBuf chunk) {

        try {

            if (messageReader == null)
                throw new IllegalStateException();

            messageReader.feedBytes(chunk);
            sendBatches();
        }
        catch (Throwable e) {
            var error = ArrowErrorMapping.mapDecodingError(e);
            onError(error);
        }
    }

    @Override
    public void onComplete() {

        try {

            if (messageReader == null)
                throw new IllegalStateException();

            messageReader.feedEos();
            sendBatches();
        }
        catch (Throwable e) {
            var error = ArrowErrorMapping.mapDecodingError(e);
            onError(error);
        }
    }

    @Override
    public void onError(Throwable error) {

        try {
            markAsDone();
            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    @Override
    public void pump() {

        try {
            if (arrowReader != null)
                sendBatches();
        }
        catch (Throwable e) {
            var error = ArrowErrorMapping.mapDecodingError(e);
            onError(error);
        }
    }

    private void sendBatches() throws IOException {

        if (!consumerReady() || isDone())
            return;

        if (root == null) {

            if (messageReader.hasMessage()) {
                root = arrowReader.getVectorSchemaRoot();
                consumer().onStart(root);
            }
            else if (messageReader.hasEos()) {
                var error = new EDataCorruption("Arrow decoding failed, data stream is empty");
                onError(error);
                return;
            }
            else {
                return;
            }
        }

        while (consumerReady() && messageReader.hasMessage(MessageHeader.RecordBatch)) {
            arrowReader.loadNextBatch();
            consumer().onBatch();
        }

        if (consumerReady() && messageReader.hasEos()) {

            if (messageReader.hasMessage()) {
                var error = new EDataCorruption("Arrow decoding failed, unexpected messages after EOS marker");
                onError(error);
                return;
            }

            markAsDone();
            consumer().onComplete();
        }
    }

    @Override
    public void close() {

        try {

            if (arrowReader != null) {
                arrowReader.close();
                arrowReader = null;
            }

            if (messageReader != null) {
                messageReader.close();
                messageReader = null;
            }

            if (root != null) {
                root.close();
                root = null;
            }
        }
        catch (Exception e) {
            throw new  ETracInternal("Failed to close Arrow stream decoder", e);
        }
    }
}
