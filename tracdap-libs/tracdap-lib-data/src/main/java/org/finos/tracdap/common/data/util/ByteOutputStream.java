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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;


public class ByteOutputStream extends OutputStream {

    private static final int DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;

    private final BufferAllocator allocator;
    private final Consumer<ArrowBuf> sink;

    private ArrowBuf buffer;

    public ByteOutputStream(BufferAllocator allocator, Consumer<ArrowBuf> sink) {

        this.allocator = allocator;
        this.sink = sink;

        this.buffer = null;
    }

    @Override
    public void write(@Nonnull byte[] b, int off, int len) throws IOException {

        buffer = Bytes.writeToStream(
                b, off, len,
                buffer, allocator,
                DEFAULT_CHUNK_SIZE,
                sink);
    }

    @Override
    public void write(int b) throws IOException {

        buffer = Bytes.writeToStream(
                b, buffer, allocator,
                DEFAULT_CHUNK_SIZE,
                sink);
    }

    @Override
    public void flush() {

        buffer = Bytes.flushStream(buffer, sink);
    }

    @Override
    public void close() throws IOException {

        try {
            buffer = Bytes.flushStream(buffer, sink);
        }
        finally {
            buffer = Bytes.closeStream(buffer);
        }
    }
}
