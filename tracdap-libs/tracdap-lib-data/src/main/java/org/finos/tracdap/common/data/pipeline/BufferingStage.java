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

package org.finos.tracdap.common.data.pipeline;

import org.finos.tracdap.common.data.DataPipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.finos.tracdap.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BufferingStage extends BaseBufferProducer
        implements DataPipeline.ByteStreamConsumer{

    private final Logger log = LoggerFactory.getLogger(getClass());

    private CompositeByteBuf buffer;

    public BufferingStage() {
        buffer = null;
    }

    @Override
    public void onStart() {

        if (buffer != null)
            throw new EUnexpected();

        buffer = Unpooled.compositeBuffer();
    }

    @Override
    public void onNext(ByteBuf chunk) {

        if (buffer == null)
            throw new EUnexpected();

        buffer.addComponent(chunk);
        buffer.writerIndex(this.buffer.writerIndex() + chunk.readableBytes());
    }

    @Override
    public void onComplete() {

        if (buffer == null)
            throw new EUnexpected();

        log.info("Buffer size: {}", buffer.readableBytes());

        var buffer = this.buffer;
        this.buffer = null;

        emitBuffer(buffer);
    }

    @Override
    public void onError(Throwable error) {

        try {
            // todo
        }
        finally {
            close();
        }
    }

    @Override
    public void close() {

        if (buffer != null) {
            buffer.release();
            buffer = null;
        }
    }
}
