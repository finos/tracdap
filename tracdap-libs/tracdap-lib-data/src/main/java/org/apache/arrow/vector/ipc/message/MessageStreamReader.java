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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;


public class MessageStreamReader extends MessageChannelReader {

    // Safeguard max allowed size for first (schema) message - 16 MiB should be ample
    private static final int CONTINUATION_MARKER = 0xffffffff;

    private final BufferAllocator allocator;
    private final Deque<ArrowBuf> byteQueue;
    private final Deque<MessageResult> messageQueue;

    private final boolean allowLegacyFormat = false;

    private long bytesReceived;
    private long bytesConsumed;
    private boolean gotEos;

    private enum Expectation {
        CONTINUATION,
        LENGTH,
        LENGTH_WITHOUT_CONTINUATION,
        MESSAGE,
        BODY,
        EOS
    }

    private Expectation expectation;
    private long bytesExpected;
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

        if (gotEos) {
            throw new IOException();  // todo
        }

        byteQueue.addLast(chunk);
        bytesReceived += chunk.readableBytes();

        while (bytesExpected <= bytesReceived - bytesExpected && !gotEos) {

            switch (expectation) {

            case CONTINUATION:

                var continuationMarker = consumeInt();

                if (continuationMarker == CONTINUATION_MARKER) {
                    expectation = Expectation.LENGTH;
                    bytesExpected = 4;
                    break;
                }
                else if (allowLegacyFormat) {
                    expectation = Expectation.LENGTH_WITHOUT_CONTINUATION;
                    bytesExpected = continuationMarker;
                }
                else
                    // Arrow does not currently have an equivalent InvalidArrowStreamException
                    throw new InvalidArrowFileException("Data corruption in Arrow data stream");

            case LENGTH:

                bytesExpected = consumeInt();

            case LENGTH_WITHOUT_CONTINUATION:

                if (bytesExpected > 0) {
                    expectation = Expectation.MESSAGE;
                }
                else if (bytesExpected == 0) {
                    expectation = Expectation.EOS;
                    gotEos = true;
                }
                else
                    throw new IOException();  // todo error

                break;

            case MESSAGE:

                var messageBuffer = consumeByteBuffer((int) bytesExpected);
                var message = Message.getRootAsMessage(messageBuffer);

                if (message.bodyLength() > 0) {
                    currentMessage = message;
                    expectation = Expectation.BODY;
                    bytesExpected = message.bodyLength();
                }
                else {
                    var result = new MessageResult(message, null);
                    messageQueue.addLast(result);
                    expectation = Expectation.CONTINUATION;
                    bytesExpected = 0;
                }

                break;

            case BODY:

                var body = (ArrowBuf) consumeArrowBuf(bytesExpected);
                var result = new MessageResult(currentMessage, body);
                messageQueue.addLast(result);

                expectation = Expectation.CONTINUATION;
                bytesExpected = 4;

                break;
            }
        }

    }

    public void feedEos() {
        gotEos = true;
    }

    public boolean hasMessage(byte messageType) {

        if (gotEos)
            return true;

        for (var message : messageQueue)
            if (message.getMessage().headerType() == messageType)
                return true;

        return false;
    }

    @Override
    public MessageResult readNext() throws IOException {

        if (messageQueue.size() > 0)
            return messageQueue.pop();

        if (gotEos)
            return null;

        throw new IOException("Unexpected end of stream");
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
            throw new IOException();  // todo

        bytesConsumed += size;

        var head = byteQueue.peek();

        if (head.readableBytes() >= size) {

            var buffer = head.nioBuffer(head.readerIndex(), size);
            head.readerIndex(head.readerIndex() + size);

            return buffer;
        }

        var buffer = ByteBuffer.allocateDirect(size);

        while (buffer.remaining() > 0 && head != null) {

            var slice = buffer.slice();

            if (slice.remaining() > head.readableBytes())
                slice.limit((int) head.readableBytes());

            head.getBytes(head.readerIndex(), slice);
            head.readerIndex(head.readerIndex() + slice.position());

            if (head.readableBytes() == 0) {
                byteQueue.pop().close();
                head = byteQueue.peek();
            }
        }

        buffer.flip();

        if (buffer.remaining() != size)
            throw new IOException();  // todo

        return buffer;
    }

    private ArrowBuf consumeArrowBuf(long size) throws IOException {

        if (byteQueue.isEmpty())
            throw new IOException();  // todo

        bytesConsumed += size;

        var head = byteQueue.peek();

        if (head.readableBytes() >= size) {

            var buffer = head.slice(head.readerIndex(), size);
            head.readerIndex(head.readerIndex() + size);

            if (head.readableBytes() == 0)
                byteQueue.pop().close();

            return buffer;
        }

        var buffer = allocator.buffer(size);

        while (buffer.writableBytes() > 0 && head != null) {

            var nBytes = Math.min(buffer.writableBytes(), head.readableBytes());

            buffer.setBytes(buffer.writerIndex(), head, head.readerIndex(), nBytes);
            buffer.writerIndex(buffer.writerIndex() + nBytes);
            head.readerIndex(head.readerIndex() + nBytes);

            if (head.readableBytes() == 0) {
                byteQueue.pop().close();
                head = byteQueue.peek();
            }
        }

        if (buffer.writerIndex() != size)
            throw new IOException();  // todo

        return buffer;
    }
}
