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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;


public class ByteSeekableChannel implements SeekableByteChannel {

    private final ByteBuf buffer;
    private final long size;

    private boolean isOpen;

    public ByteSeekableChannel(ByteBuf buffer) {

        this.buffer = buffer;
        buffer.readerIndex(0);
        this.size = buffer.readableBytes();

        isOpen = true;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {

        var bytesToRead = Math.min(dst.remaining(), buffer.readableBytes());

        buffer.readBytes(dst);

        return bytesToRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() {
        return buffer.readerIndex();
    }

    @Override
    public SeekableByteChannel position(long newPosition) {

        if (newPosition < 0)
            throw new IllegalArgumentException();

        var position = Math.min(newPosition, size);
        buffer.readerIndex((int) position);

        return this;
    }

    @Override
    public long size() throws IOException {
        return size;
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {

        isOpen = false;
    }
}
