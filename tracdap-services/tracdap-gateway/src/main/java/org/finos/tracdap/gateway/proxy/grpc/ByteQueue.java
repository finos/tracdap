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

package org.finos.tracdap.gateway.proxy.grpc;

import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.buffer.ByteBuf;

import java.util.ArrayDeque;
import java.util.Deque;


public class ByteQueue {

    private final Deque<ByteBuf> queue;
    private int queueBytes = 0;

    public ByteQueue() {
        this.queue = new ArrayDeque<>();
    }

    public int availableBytes() {
        return queueBytes;
    }

    public boolean hasAnyBytes() {
        return queueBytes > 0;
    }

    public boolean hasBytes(int nBytes) {
        return this.queueBytes >= nBytes;
    }

    public void pushBytes(ByteBuf buf) {
        this.queue.add(buf);
        this.queueBytes += buf.readableBytes();
    }

    public ByteBuf peekBytes(int nBytes) {

        var buf0 = this.queue.peek();

        if (this.queueBytes < nBytes || buf0 == null)
            throw new EUnexpected();

        if (buf0.readableBytes() >= nBytes) {
            return buf0.slice(0, nBytes).retain();
        }

        var buf = popBytes(nBytes);
        queue.push(buf);

        return buf.retain();
    }

    public ByteBuf popBytes(int nBytes) {

        if (this.queueBytes < nBytes)
            throw new EUnexpected();

        var buf0 = popBytesUpto(nBytes);

        if (buf0.readableBytes() == nBytes)
            return buf0;

        var composite = buf0.alloc().compositeBuffer();
        composite.addComponent(true, buf0);

        while(composite.readableBytes() < nBytes) {

            buf0 = popBytesUpto(nBytes - composite.readableBytes());
            composite.addComponent(true, buf0);
        }

        return composite;
    }

    private ByteBuf popBytesUpto(int nBytes) {

        var buf0 = this.queue.pop();

        if (buf0.readableBytes() <= nBytes) {
            queueBytes -= buf0.readableBytes();
            return buf0;
        }

        else {

            var requiredSlice = buf0.retainedSlice(0, nBytes);
            var remainingSlice = buf0.retainedSlice(nBytes, buf0.readableBytes() - nBytes);
            buf0.release();

            this.queue.push(remainingSlice);

            queueBytes -= requiredSlice.readableBytes();
            return requiredSlice;
        }
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public void destroyQueue() {

        while (!queue.isEmpty()) {
            var buf = queue.pop();
            buf.release();
        }
    }

}