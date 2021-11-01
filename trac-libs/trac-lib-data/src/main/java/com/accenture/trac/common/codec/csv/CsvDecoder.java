/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.codec.csv;

import com.accenture.trac.common.concurrent.flow.CommonBaseProcessor;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CsvDecoder extends CommonBaseProcessor<ByteBuf, VectorSchemaRoot> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final CompositeByteBuf buffer;
    private final CsvMapper mapper;

    public CsvDecoder() {
        buffer = ByteBufAllocator.DEFAULT.compositeBuffer();
        mapper = new CsvMapper();
    }

    @Override
    protected void handleTargetRequest() {

        if (nTargetRequested() > 0 && nSourceRequested() <= nSourceDelivered())
            doSourceRequest(1);
    }

    @Override
    protected void handleTargetCancel() {

        releaseBuffer();
    }

    @Override
    protected void handleSourceNext(ByteBuf chunk) {

        buffer.addComponent(chunk);
    }

    @Override
    protected void handleSourceError(Throwable error) {

        releaseBuffer();
    }

    @Override
    protected void handleSourceComplete() {

        releaseBuffer();
    }

    private void releaseBuffer() {

        var releaseOk = buffer.release();

        if (!releaseOk && buffer.capacity() > 0)
            log.warn("CSV decode buffer was not released (this could indicate a memory leak)");
    }
}
