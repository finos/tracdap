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
import org.finos.tracdap.common.exception.EUnexpected;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;


public class ByteSeekableChannel implements SeekableByteChannel {

    private final List<ArrowBuf> chunks;
    private final List<Long> offsets;
    private final long size;

    private long position;
    private int index;
    private boolean isOpen;

    public ByteSeekableChannel(List<ArrowBuf> buffers) {

        this.chunks = buffers;
        this.offsets = new ArrayList<>(chunks.size());

        for (position = 0, index = 0; index < chunks.size(); index++) {
            offsets.add(position);
            position += chunks.get(index).readableBytes();
        }

        size = position;
        position = 0;
        index = 0;
        isOpen = true;
    }

    @Override
    public long size() throws IOException {

        if (!isOpen)
            throw new ClosedChannelException();

        return size;
    }

    @Override
    public long position() throws IOException  {

        if (!isOpen)
            throw new ClosedChannelException();

        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {

        if (!isOpen)
            throw new ClosedChannelException();

        if (newPosition < 0)
            throw new IllegalArgumentException();

        position = newPosition;

        if (index >= offsets.size()) {
            updateIndex(0, offsets.size());
            return this;
        }

        var chunk = chunks.get(index);
        var offset = offsets.get(index);
        var boundary = offset + chunk.readableBytes();

        if (position < offset)
            updateIndex(0, index);

        if (position >= boundary)
            updateIndex(index + 1, offsets.size());

        return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {

        if (!isOpen)
            throw new ClosedChannelException();

        if (position >= size)
            return -1;

        var offset = offsets.get(index);
        var chunk = chunks.get(index);
        var chunkOffset = position - offset;

        var boundary = index + 1 < offsets.size() ? offsets.get(index + 1) : size;
        var nBytes = (int) Math.min(chunk.readableBytes() - chunkOffset, Integer.MAX_VALUE);

        if (nBytes < dst.remaining())
            dst.limit(dst.position() + nBytes);

        chunk.getBytes(chunk.readerIndex() + chunkOffset, dst);

        position += nBytes;

        if (position >= boundary)
            index += 1;

        return nBytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException();
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

    private void updateIndex(int min, int max) {

        if (min == max) {
            index = min;
            return;
        }

        if (position < offsets.get(min))
            throw new EUnexpected();

        for (var i = min; i < max; i++) {

            var offset = offsets.get(i);
            var boundary = i + 1 < offsets.size() ? offsets.get(i + 1) : size;

            if (offset <= position && position < boundary) {
                index = i;
                return;
            }
        }

        if (position == size)
            index = offsets.size();

        throw new EUnexpected();
    }
}
