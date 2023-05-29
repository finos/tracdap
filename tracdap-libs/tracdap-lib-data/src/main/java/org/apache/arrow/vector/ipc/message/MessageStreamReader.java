/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.apache.arrow.vector.ipc.message;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.InvalidArrowFileException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;


public class MessageStreamReader extends MessageChannelReader {

    // This class is an alternative implementation of Arrow's MessageChannelReader
    // It is meant for use with ArrowStreamReader, so the reader's functionality is preserved
    // We could just implement a new version of ArrowStreamReader instead, which could be done in the tracdap namespace
    // However we would still need essentially the same functionality and separation
    // This approach avoids duplicating the code in ArrowStreamReader itself

    // Bytes and the EOS signal are fed in using feedBytes() and feedEos()
    // Available messages can be checked by calling hasMessage() before doing loadNextBatch() on the reader
    // If loadNextBatch() is called before a message is available, an IO exception is thrown

    private static final int CONTINUATION_MARKER = 0xffffffff;

    private final BufferAllocator allocator;
    private final Deque<ArrowBuf> byteQueue;
    private final Deque<MessageResult> messageQueue;

    private enum Expectation {
        CONTINUATION,
        LENGTH,
        MESSAGE,
        BODY,
        EOS
    }

    private Expectation expectation;
    private long bytesExpected;
    private long bytesReceived;
    private long bytesConsumed;
    private boolean gotEos;

    private Message currentMessage;

    public MessageStreamReader(BufferAllocator allocator) {

        // Supply null channel and allocator
        // We are treating the base class as an interface and overriding the whole implementation
        super(null, null);

        this.allocator = allocator;
        this.byteQueue = new ArrayDeque<>();
        this.messageQueue = new ArrayDeque<>();

        expectation = Expectation.CONTINUATION;
        bytesExpected = 4;
    }

    public void feedBytes(ArrowBuf chunk) throws IOException {

        if (gotEos || expectation == Expectation.EOS) {
            throw new IOException("More data received after Arrow EOS marker");
        }

        byteQueue.addLast(chunk);
        bytesReceived += chunk.readableBytes();

        while (bytesExpected <= bytesReceived - bytesConsumed) {

            switch (expectation) {

            case CONTINUATION:

                var continuationMarker = consumeInt();

                if (continuationMarker == CONTINUATION_MARKER) {
                    expectation = Expectation.LENGTH;
                    bytesExpected = 4;
                    break;
                }
                else
                    // Arrow does not currently have an equivalent InvalidArrowStreamException
                    throw new InvalidArrowFileException("Invalid message in the Arrow data stream");

            case LENGTH:

                var messageLength = consumeInt();

                if (messageLength > 0) {
                    expectation = Expectation.MESSAGE;
                    bytesExpected = messageLength;
                }
                else if (messageLength == 0) {
                    expectation = Expectation.EOS;
                    bytesExpected = 0;
                }
                else
                    throw new InvalidArrowFileException("Invalid message in the Arrow data stream");

                break;

            case MESSAGE:

                var messageBuffer = consumeByteBuffer((int) bytesExpected);
                var message = Message.getRootAsMessage(messageBuffer);

                if (message.bodyLength() > 0) {
                    expectation = Expectation.BODY;
                    bytesExpected = message.bodyLength();
                    currentMessage = message;
                }
                else {
                    var result = new MessageResult(message, null);
                    messageQueue.addLast(result);
                    expectation = Expectation.CONTINUATION;
                    bytesExpected = 4;
                }

                break;

            case BODY:

                var body = (ArrowBuf) consumeArrowBuf(bytesExpected);
                var result = new MessageResult(currentMessage, body);
                messageQueue.addLast(result);

                expectation = Expectation.CONTINUATION;
                bytesExpected = 4;

                break;

            case EOS:

                if (bytesReceived > bytesConsumed)
                    throw new IOException("More data received after Arrow EOS marker");

                // Do not set gotEos = true yet, wait for the explicit signal
                // The EOS message is still a chunk in the data stream
                // The explicit signal should come from onComplete() or similar

                return;
            }
        }
    }

    public void feedEos() {
        gotEos = true;
    }

    public boolean hasMessage(byte messageType) {

        for (var message : messageQueue)
            if (message.getMessage().headerType() == messageType)
                return true;

        return false;
    }

    public boolean hasMessage() {
        return ! messageQueue.isEmpty();
    }

    public boolean hasEos() {
        return gotEos;
    }

    @Override
    public MessageResult readNext() throws IOException {

        if (messageQueue.size() > 0)
            return messageQueue.pop();

        if (gotEos)
            return null;

        throw new EOFException("Unexpected end of Arrow data stream");
    }

    @Override
    public long bytesRead() {
        return bytesConsumed;
    }

    @Override
    public void close() throws IOException {

        // Do not call super.close(), so base class does not try to close resources

        byteQueue.forEach(ArrowBuf::close);
        byteQueue.clear();

        while (!messageQueue.isEmpty()) {
            var message = messageQueue.pop();
            var body = message.getBodyBuffer();
            if (body != null)
                body.close();
        }
    }

    private int consumeInt() throws IOException {

        var buffer = consumeByteBuffer(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer.getInt();
    }

    private ByteBuffer consumeByteBuffer(int size) throws IOException {

        if (byteQueue.isEmpty())
            throw new EOFException("Unexpected end of Arrow data stream");

        // FlatBuffers accesses the ByteBuffer directly to supply values of the Message
        // Since the ArrowBuf will be closed, we need to allocate a new ByteBuffer and copy the bytes

        var buffer = ByteBuffer.allocateDirect(size);
        var head = byteQueue.peek();

        // If the required content is all in the current ArrowBuf, we can copy directly

        if (head != null && head.readableBytes() >= size) {

            head.getBytes(head.readerIndex(), buffer);
            head.readerIndex(head.readerIndex() + size);

            bytesConsumed += size;

            // Once a chunk is fully consumed, pop and release
            if (head.readableBytes() == 0)
                byteQueue.pop().close();
        }

        // If the required content spans several chunks, we need to read chunk by chunk
        // Arrow does not have a range-limited getBytes where the target is a ByteBuffer
        // For now, use slices and limit each slice to match the current chunk

        while (head != null && buffer.remaining() > 0) {

            var slice = buffer.slice();

            if (slice.remaining() > head.readableBytes())
                slice.limit((int) head.readableBytes());

            head.getBytes(head.readerIndex(), slice);
            head.readerIndex(head.readerIndex() + slice.position());

            buffer.position(buffer.position() + slice.position());
            bytesConsumed += slice.position();

            // Once a chunk is fully consumed, pop and release
            if (head.readableBytes() == 0) {
                byteQueue.pop().close();
                head = byteQueue.peek();
            }
        }

        buffer.flip();

        if (buffer.remaining() != size)
            throw new EOFException("Unexpected end of Arrow data stream");

        return buffer;
    }

    private ArrowBuf consumeArrowBuf(long size) throws IOException {

        if (byteQueue.isEmpty())
            throw new EOFException("Unexpected end of Arrow data stream");

        var head = byteQueue.peek();

        // If the required content is fully in the current chunk, there is no need to copy
        // Instead use a slice to give a view into the existing buffer

        if (head != null && head.readableBytes() >= size) {

            var buffer = head.slice(head.readerIndex(), size);
            head.readerIndex(head.readerIndex() + size);

            // ArrowBuf.slice() does not increment the ref count, so we need to call retain()
            buffer.getReferenceManager().retain();
            bytesConsumed += size;

            // Once a chunk is fully consumed, pop and release
            if (head.readableBytes() == 0)
                byteQueue.pop().close();

            return buffer;
        }

        // Arrow does not support compound buffers
        // If the required content spans several chunks, we need to allocate a new buffer and copy

        var buffer = allocator.buffer(size);

        while (head != null && buffer.writerIndex() < size) {

            // The maximum bytes that can be copied in a single operation is limited to an integer value
            var nRemaining = size - buffer.writerIndex();
            var nBytes = Math.min(Math.min(nRemaining, head.readableBytes()), Integer.MAX_VALUE);

            head.getBytes(head.readerIndex(), buffer, buffer.writerIndex(), (int) nBytes);
            head.readerIndex(head.readerIndex() + nBytes);

            buffer.writerIndex(buffer.writerIndex() + nBytes);
            bytesConsumed += nBytes;

            // Once a chunk is fully consumed, pop and release
            if (head.readableBytes() == 0) {
                byteQueue.pop().close();
                head = byteQueue.peek();
            }
        }

        if (buffer.writerIndex() != size)
            throw new EOFException("Unexpected end of Arrow data stream");

        return buffer;
    }
}
