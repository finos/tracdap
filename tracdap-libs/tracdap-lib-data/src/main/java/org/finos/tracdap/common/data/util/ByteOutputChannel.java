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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.function.Consumer;


public class ByteOutputChannel implements WritableByteChannel {

    private static final int CHUNK_SIZE = 2 * 1024 * 1024;

    private final BufferAllocator allocator;
    private final Consumer<ArrowBuf> sink;

    private ArrowBuf buffer;
    private boolean isOpen;

    public ByteOutputChannel(BufferAllocator allocator, Consumer<ArrowBuf> sink) {

        this.allocator = allocator;
        this.sink = sink;

        this.buffer = null;
        this.isOpen = true;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {

        if (!isOpen)
            throw new IOException("Channel is already closed");

        var startPosition = src.position();

        buffer = Bytes.writeToStream(src, buffer, allocator, CHUNK_SIZE, sink);

        var endPosition = src.position();

        return endPosition - startPosition;
    }

    @Override
    public void close() throws IOException {

        isOpen = false;

        try {
            buffer = Bytes.flushStream(buffer, sink);
        }
        finally {
            buffer = Bytes.closeStream(buffer);
        }
    }
}
