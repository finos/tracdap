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

package org.finos.tracdap.common.codec.arrow;

import org.finos.tracdap.common.codec.BufferDecoder;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.InvalidArrowFileException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;


public abstract class ArrowDecoder extends BufferDecoder implements DataPipeline.BufferApi {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private List<ArrowBuf> buffer;
    private ArrowReader reader;
    private VectorSchemaRoot root;

    public ArrowDecoder() {

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
            this.root = reader.getVectorSchemaRoot();

            consumer().onStart(root);

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
            if (root == null)
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

        while (consumerReady()) {

            var batchAvailable = reader.loadNextBatch();

            if (batchAvailable) {
                consumer().onBatch();
            }
            else {
                return true;
            }
        }

        return false;
    }

    private void handleErrors(Callable<Void> lambda) {

        try {

            lambda.call();
        }
        catch (ETrac e) {

            // Error has already been handled, propagate as-is

            var errorMessage = "Arrow decoding failed: " + e.getMessage();

            log.error(errorMessage, e);
            throw e;
        }
        catch (InvalidArrowFileException e) {

            // A nice clean validation failure from the Arrow framework
            // E.g. missing / incorrect magic number at the start (or end) of the file

            var errorMessage = "Arrow decoding failed, file is invalid: " + e.getMessage();

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException | IOException  e) {

            // These errors occur if the data stream contains bad values for vector sizes, offsets etc.
            // This may be as a result of a corrupt data stream, or a maliciously crafted message

            // Decoders work on a stream of buffers, "real" IO exceptions should not occur

            var errorMessage = "Arrow decoding failed, content is garbled";

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (Throwable e)  {

            // Ensure unexpected errors are still reported to the Flow API

            log.error("Unexpected error in Arrow decoding", e);
            throw new EUnexpected(e);
        }
    }

    @Override
    public void close() {

        try {

            if (root != null) {
                root.close();
                root = null;
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
