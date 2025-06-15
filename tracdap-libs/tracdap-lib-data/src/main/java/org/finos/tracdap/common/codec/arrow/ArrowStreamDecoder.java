/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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

import org.finos.tracdap.common.codec.StreamingDecoder;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETracInternal;

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.MessageStreamReader;

import java.io.IOException;


public class ArrowStreamDecoder extends StreamingDecoder implements DataPipeline.StreamApi {

    // Pure streaming implementation of the arrow stream codec
    // Uses MessageStreamReader to process incoming bytes and determine when messages are available
    // Once messages arrive, defer to the regular ArrowStreamReader provided by the core Arrow libraries

    private final BufferAllocator allocator;

    private MessageStreamReader messageReader;
    private ArrowReader arrowReader;
    private ArrowVsrContext context;

    public ArrowStreamDecoder(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void onStart() {

        try {

            messageReader = new MessageStreamReader(allocator);
            arrowReader = new ArrowStreamReader(messageReader, allocator);
        }
        catch (Throwable e) {
            var error = ArrowErrorMapping.mapDecodingError(e);
            onError(error);
        }
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

        if (isDone())
            return;

        if (context == null) {

            if (messageReader.hasMessage()) {
                context = ArrowVsrContext.forSource(arrowReader.getVectorSchemaRoot(), arrowReader, allocator);
                consumer().onStart(context);
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

        if (context.readyToFlip())
            context.flip();

        if (context.readyToLoad()) {

            if (messageReader.hasMessage(MessageHeader.RecordBatch)) {

                arrowReader.loadNextBatch();
                context.setLoaded();

                if (context.readyToFlip())
                    context.flip();
            }
            else if (messageReader.hasEos()) {

                if (messageReader.hasMessage()) {
                    var error = new EDataCorruption("Arrow decoding failed, unexpected messages after EOS marker");
                    onError(error);
                    return;
                }

                if (!isDone())
                    markAsDone();
            }
        }

        if (context.readyToUnload() && consumerReady())
            consumer().onBatch();

        if (isDone() && !context.readyToUnload())
            consumer().onComplete();
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

            if (context != null) {
                context.close();
                context = null;
            }
        }
        catch (Exception e) {
            throw new  ETracInternal("Failed to close Arrow stream decoder", e);
        }
    }
}
