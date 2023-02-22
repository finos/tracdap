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

import org.finos.tracdap.common.codec.StreamingEncoder;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public abstract class ArrowEncoder extends StreamingEncoder implements DataPipeline.ArrowApi {

    // Common base encoder for both files and streams
    // Both receive a data stream and output a byte stream

    // Decoders do not share a common structure
    // This is because the file decoder requires random access to the byte stream

    private final Logger log = LoggerFactory.getLogger(getClass());

    private VectorSchemaRoot root;
    private ArrowWriter writer;

    protected abstract ArrowWriter createWriter(VectorSchemaRoot root);

    public ArrowEncoder() {

    }

    @Override
    public void onStart(VectorSchemaRoot root) {

        try {

            if (log.isTraceEnabled())
                log.trace("ARROW ENCODER: onStart()");

            this.root = root;
            this.writer = createWriter(root);

            writer.start();
            consumer().onStart();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }

    @Override
    public void onBatch() {

        try {  // This will release the batch

            if (log.isTraceEnabled())
                log.trace("ARROW ENCODER: onNext()");

            writer.writeBatch();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }

    @Override
    public void onComplete() {

        try {

            if (log.isTraceEnabled())
                log.trace("ARROW ENCODER: onComplete()");

            markAsDone();

            // Flush and close output

            writer.end();
            writer.close();
            writer = null;

            consumer().onComplete();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
        finally {
            close();
        }
    }

    @Override
    public void onError(Throwable error) {

        try {

            if (log.isTraceEnabled())
                log.trace("ARROW ENCODER: onError()");

            markAsDone();
            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    @Override
    public void close() {

        if (writer != null) {
            writer.close();
            writer = null;
        }

        if (root != null) {
            // Do not close root, we do not own it
            root = null;
        }
    }
}
