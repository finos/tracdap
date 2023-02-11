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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;


public class ByteOutputStream extends OutputStream {

    private static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;

    private final ByteBufAllocator allocator;
    private final Consumer<ByteBuf> sink;

    private ByteBuf buffer;

    public ByteOutputStream(Consumer<ByteBuf> sink) {
        this(sink, ByteBufAllocator.DEFAULT);
    }

    public ByteOutputStream(Consumer<ByteBuf> sink, ByteBufAllocator allocator) {

        this.allocator = allocator;
        this.buffer = null;
        this.sink = sink;
    }

    @Override
    public void write(@Nonnull byte[] b, int off, int len) throws IOException {

        var remaining = len - off;

        while (remaining > 0) {

            if (buffer == null)
                buffer = allocator.directBuffer(DEFAULT_CHUNK_SIZE);

            var nBytes = Math.min(remaining, buffer.writableBytes());

            buffer.writeBytes(b, off, nBytes);

            off += nBytes;
            remaining -= nBytes;

            if (buffer.writableBytes() == 0) {
                sink.accept(buffer);
                buffer = null;
            }
        }
    }

    @Override
    public void write(int b) throws IOException {

        if (buffer == null)
            buffer = allocator.directBuffer(DEFAULT_CHUNK_SIZE);

        buffer.writeByte(b);

        if (buffer.writableBytes() == 0) {
            sink.accept(buffer);
            buffer = null;
        }
    }

    @Override
    public void flush() {

        if (buffer != null && buffer.readableBytes() > 0) {
            sink.accept(buffer);
            buffer = null;
        }
    }

    @Override
    public void close() throws IOException {

        flush();

        if (buffer != null) {
            buffer.release();
            buffer = null;
        }
    }
}
