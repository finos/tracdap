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
import org.finos.tracdap.common.exception.EData;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETracInternal;

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;


public class ArrowStreamDecoder extends StreamingDecoder implements DataPipeline.StreamApi {

    // Safeguard max allowed size for first (schema) message - 16 MiB should be ample
    private static final int CONTINUATION_MARKER = 0xffffffff;
    private static final int MAX_FIRST_MESSAGE_SIZE = 16 * 1024 * 1024;

    private final MessageStreamReader messageReader;
    private final ArrowReader arrowReader;
    private VectorSchemaRoot root;

    public ArrowStreamDecoder(BufferAllocator arrowAllocator) {
        this.messageReader = new MessageStreamReader(arrowAllocator);
        this.arrowReader = new ArrowStreamReader(messageReader, arrowAllocator);
    }

    @Override
    public void onStart() {

        // No-op, don't do anything before the data stream starts
    }

    @Override
    public void onNext(ArrowBuf chunk) {

        try {
            messageReader.feedBytes(chunk);
            sendBatches();
        }
        catch (Throwable e) {
            var error = new EData("Arrow stream decoding failed: " + e.getMessage(), e);
            onError(error);
        }
    }

    @Override
    public void onComplete() {

        try {
            messageReader.feedEos();
            sendBatches();
        }
        catch (Throwable e) {
            var error = new EData("Arrow stream decoding failed: " + e.getMessage(), e);
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
            sendBatches();
        }
        catch (Throwable e) {
            var error = new EData("Arrow stream decoding failed: " + e.getMessage(), e);
            onError(error);
        }
    }

    private void sendBatches() throws IOException {

        if (root == null) {

            if (messageReader.hasMessage(MessageHeader.Schema)) {
                root = arrowReader.getVectorSchemaRoot();
                consumer().onStart(root);
            }
            else {
                return;
            }
        }

        while (consumerReady() && messageReader.hasMessage(MessageHeader.RecordBatch)) {

            if (arrowReader.loadNextBatch()) {
                consumer().onBatch();
            }
            else {
                markAsDone();
                consumer().onComplete();
            }
        }
    }

    @Override
    public void close() {

        try {
            messageReader.close();
            arrowReader.close();
            root.close();
        }
        catch (Exception e) {
            throw new  ETracInternal("Failed to close Arrow stream decoder", e);
        }
    }

    private void validateStartOfStream(SeekableByteChannel channel) throws IOException {

        // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format

        // Arrow streams are a series of messages followed by optional body data
        // Each message is preceded by a continuation marker and message size
        // Just sanity checking these two values should catch some serious decode failures
        // E.g. if a data stream contains a totally different format

        // Record the stream position, so it can be restored after the validation
        long pos = channel.position();

        var headerBytes = new byte[8];
        var headerBuf = ByteBuffer.wrap(headerBytes);
        headerBuf.order(ByteOrder.LITTLE_ENDIAN);

        channel.position(0);

        var prefaceLength = channel.read(headerBuf);
        var continuation = headerBuf.getInt(0);
        var messageSize = headerBuf.getInt(4);

        if (prefaceLength != 8 || continuation != CONTINUATION_MARKER ||
            messageSize <=0 || messageSize > MAX_FIRST_MESSAGE_SIZE)

            throw new EDataCorruption("Data is corrupt, or not an Arrow stream");

        // Restore original stream position
        channel.position(pos);
    }
}
