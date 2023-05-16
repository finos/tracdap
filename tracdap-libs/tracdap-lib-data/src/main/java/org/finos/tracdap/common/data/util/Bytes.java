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

package org.finos.tracdap.common.data.util;

import org.finos.tracdap.common.exception.EDataSize;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;


public class Bytes {

    // Currently, it is not possible to do zero-copy between managed buffers and gRPC
    // https://github.com/grpc/grpc-java/issues/1054

    // Incoming data has already been copied to create the ByteString
    // Outgoing data needs to be copied before giving to gRPC
    // Because writing to the wire is asynchronous and there is no way to callback for release

    public static ArrowBuf writeToStream(
            ByteBuffer src, ArrowBuf target,
            BufferAllocator allocator, int chunkSize,
            Consumer<ArrowBuf> sink) {

        while (src.remaining() > 0) {

            if (target == null)
                target = allocator.buffer(chunkSize);

            var nBytes = (int) Math.min(src.remaining(), target.writableBytes());
            var bytesPosition = src.position() + nBytes;
            var bufferWriterIndex = target.writerIndex() + nBytes;

            target.setBytes(target.writerIndex(), src, src.position(), nBytes);

            target.writerIndex(bufferWriterIndex);
            src.position(bytesPosition);

            if (target.writableBytes() == 0) {
                sink.accept(target);
                target = null;
            }
        }

        return target;
    }

    public static ArrowBuf writeToStream(
            byte[] src, int off, int len, ArrowBuf target,
            BufferAllocator allocator, int chunkSize,
            Consumer<ArrowBuf> sink) {

        var remaining = len - off;

        while (remaining > 0) {

            if (target == null)
                target = allocator.buffer(chunkSize);

            var nBytes = (int) Math.min(remaining, target.writableBytes());

            target.writeBytes(src, off, nBytes);

            off += nBytes;
            remaining -= nBytes;

            if (target.writableBytes() == 0) {
                sink.accept(target);
                target = null;
            }
        }

        return target;
    }

    public static ArrowBuf writeToStream(
            int byte_, ArrowBuf target,
            BufferAllocator allocator, int chunkSize,
            Consumer<ArrowBuf> sink) {

        if (target == null)
            target = allocator.buffer(chunkSize);

        target.writeByte(byte_);

        if (target.writableBytes() == 0) {
            sink.accept(target);
            target = null;
        }

        return target;
    }

    public static ArrowBuf flushStream(ArrowBuf buffer, Consumer<ArrowBuf> sink) {

        if (buffer != null) {
            if (buffer.readableBytes() > 0)
                sink.accept(buffer);
            else
                buffer.close();
        }

        return null;
    }

    public static ArrowBuf closeStream(ArrowBuf buffer) {

        if (buffer != null)
            buffer.close();

        return null;
    }

    public static void readFromStream(ArrowBuf src, Consumer<ByteBuffer> sink) {

        try (src) {

            while (src.readableBytes() > 0) {

                var nBytes = (int) Math.min(src.readableBytes(), Integer.MAX_VALUE);
                var target = ByteBuffer.allocateDirect(nBytes);

                src.getBytes(src.readerIndex(), target);
                src.readerIndex(src.readerIndex() + nBytes);

                target.flip();

                sink.accept(target);
            }
        }
    }

    public static ByteBuffer readFromBuffer(List<ArrowBuf> src) {

        try {

            var bufferSize = readableBytes(src);

            if (bufferSize > Integer.MAX_VALUE)
                throw new EDataSize("Data size exceeded the limit for a buffered read operation");

            // ArrowBuf does not have a getBytes() method with target offset/size to write directly to ByteBuffer
            // Only solution is to write to a heap-allocated buffer and wrap

            var target = new byte[(int) bufferSize];
            var targetOffset = 0;

            for (var chunk : src) {

                var chunkSize = (int) chunk.readableBytes();

                chunk.getBytes(chunk.readerIndex(), target, targetOffset, chunkSize);
                chunk.readerIndex(chunk.readerIndex() + chunkSize);
            }

            return ByteBuffer.wrap(target);
        }
        finally {

            src.forEach(ArrowBuf::close);
            src.clear();
        }
    }

    public static long readableBytes(List<ArrowBuf> src) {

        return src.stream()
                .mapToLong(ArrowBuf::readableBytes)
                .sum();
    }
}
