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
import io.netty.buffer.CompositeByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.function.Consumer;


public class ByteOutputChannel implements WritableByteChannel {

    private static final int CHUNK_SIZE = 2 * 1024 * 1024;

    private final ByteBufAllocator allocator;
    private final Consumer<ByteBuf> sink;
    private final boolean useBuffering;

    private CompositeByteBuf buffer;
    private boolean isOpen = true;

    public ByteOutputChannel(Consumer<ByteBuf> sink) {
        this(sink, ByteBufAllocator.DEFAULT);
    }

    public ByteOutputChannel(Consumer<ByteBuf> sink, ByteBufAllocator allocator) {
        this(sink, allocator, true);
    }

    public ByteOutputChannel(Consumer<ByteBuf> sink, ByteBufAllocator allocator, boolean useBuffering) {

        this.allocator = allocator;
        this.sink = sink;
        this.useBuffering = useBuffering;

        this.buffer = null;

    }

    @Override
    public int write(ByteBuffer src) throws IOException {

        if (!isOpen)
            throw new IOException("Channel is already closed");

        var chunkBuf = allocator.directBuffer(src.remaining());
        chunkBuf.writeBytes(src);
        var chunkRemaining = chunkBuf.readableBytes();

        if (!useBuffering) {
            sink.accept(chunkBuf);
            return chunkRemaining;
        }

        var bytesWritten = 0;

        while (chunkRemaining > 0) {

            if (buffer == null)
                buffer = allocator.compositeBuffer();

            var bufferRemaining = CHUNK_SIZE - buffer.readableBytes();
            var sliceSize = Math.min(chunkBuf.readableBytes(), bufferRemaining);
            var slice = chunkBuf.readSlice(sliceSize);
            slice.retain();

            buffer.addComponent(true, slice);
            bytesWritten += sliceSize;
            chunkRemaining = chunkBuf.readableBytes();

            if (buffer.readableBytes() == CHUNK_SIZE) {
                sink.accept(buffer);
                buffer = null;
            }
        }

        chunkBuf.release();

        return bytesWritten;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {

        isOpen = false;

        if (buffer != null) {

            sink.accept(buffer);
            buffer = null;
        }
    }
}
