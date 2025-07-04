/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.codec.arrow;

import org.finos.tracdap.common.codec.BufferDecoder;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;


public abstract class ArrowDecoder extends BufferDecoder implements DataPipeline.BufferApi {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator allocator;
    private ArrowVsrContext context;

    private List<ArrowBuf> buffer;
    private ArrowReader reader;

    public ArrowDecoder(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    protected abstract ArrowReader createReader(List<ArrowBuf> buffer) throws IOException;

    @Override
    public void onBuffer(List<ArrowBuf> buffer) {

        if (log.isTraceEnabled())
            log.trace("ARROW DECODER: onBuffer()");

        if (Bytes.readableBytes(buffer) == 0) {
            var error = new EDataCorruption("Arrow data file is empty");
            log.error(error.getMessage(), error);
            throw error;
        }

        handleErrors(() -> {

            this.buffer = buffer;
            this.reader = createReader(buffer);

            this.context = ArrowVsrContext.forSource(
                    reader.getVectorSchemaRoot(),
                    reader, allocator);

            consumer().onStart(context);

            var isComplete = sendBatches();

            if (isComplete) {
                markAsDone();
                consumer().onComplete();
                close();
            }

            return null;
        });
    }

    @Override
    public void onError(Throwable error) {

        try  {

            if (log.isTraceEnabled())
                log.trace("ARROW DECODER: onError()");

            markAsDone();
            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    @Override
    public void pump() {

        handleErrors(() -> {

            // Don't try to pump if the stage isn't active yet
            if (context == null)
                return null;

            var isComplete = sendBatches();

            if (isComplete) {
                markAsDone();
                consumer().onComplete();
                close();
            }

            return null;
        });
    }

    private boolean sendBatches() throws IOException {

        // Data arrives in one big buffer, so don't send it all at once
        // PUsh what the consumer requests, then wait for another call to pump()

        while (context.readyToFlip() || context.readyToLoad()) {

            if (context.readyToFlip())
                context.flip();

            if (context.readyToLoad()) {

                var batchAvailable = reader.loadNextBatch();

                if (!batchAvailable)
                    return true;

                context.setLoaded();

                if (context.readyToFlip())
                    context.flip();
            }

            if (context.readyToUnload() && consumerReady())
                consumer().onBatch();
        }

        return false;
    }

    private void handleErrors(Callable<Void> lambda) {

        try {

            lambda.call();
        }
        catch (Throwable e) {

            var error = ArrowErrorMapping.mapDecodingError(e);
            log.error(error.getMessage(), error);
            throw error;
        }
    }

    @Override
    public void close() {

        try {

            if (context != null) {
                context.close();
                context = null;
            }

            if (reader != null) {
                reader.close();
                reader = null;
            }

            if (buffer != null) {
                buffer.forEach(ArrowBuf::close);
                buffer = null;
            }
        }
        catch (Exception e) {

            log.error("Unexpected error while shutting down Arrow file decoder", e);
            throw new EUnexpected(e);
        }
    }
}
