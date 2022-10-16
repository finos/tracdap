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
import org.finos.tracdap.common.exception.EUnexpected;

import io.netty.buffer.ByteBuf;


public abstract class BaseBufferProducer implements DataPipeline.ByteBufferProducer {

    private DataPipeline.ByteBufferConsumer consumer;

    final void bind(DataPipeline.ByteBufferConsumer consumer) {

        if (this.consumer != null)
            throw new EUnexpected();  // todo err

        this.consumer = consumer;
    }

    @Override
    public void emitBuffer(ByteBuf buffer) {

        //log.info("emitBuffer");

        if (this.consumer == null)
            throw new EUnexpected();

        consumer.consumeBuffer(buffer);
    }
}
