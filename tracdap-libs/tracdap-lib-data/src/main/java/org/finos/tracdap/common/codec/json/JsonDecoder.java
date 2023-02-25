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

package org.finos.tracdap.common.codec.json;

import org.finos.tracdap.common.codec.StreamingDecoder;
import org.finos.tracdap.common.data.ArrowSchema;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class JsonDecoder extends StreamingDecoder {

    private static final int BATCH_SIZE = 1024;
    private static final boolean CASE_INSENSITIVE = false;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private VectorSchemaRoot root;
    private JsonStreamParser parser;
    private long bytesConsumed;

    public JsonDecoder(BufferAllocator arrowAllocator, Schema arrowSchema) {

        // Allocate memory once, and reuse it for every batch (i.e. do not clear/allocate per batch)
        // This memory is released in close(), which calls root.close()

        this.root = ArrowSchema.createRoot(arrowSchema, arrowAllocator, BATCH_SIZE);

        for (var vector : root.getFieldVectors())
            vector.allocateNew();
    }

    @Override
    public void onStart() {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON DECODER: onStart()");

            var factory = new JsonFactory();
            var tableHandler = new JsonTableHandler(root, batch -> consumer().onNext(), BATCH_SIZE, CASE_INSENSITIVE);

            this.parser = new JsonStreamParser(factory, tableHandler);

            consumer().onStart(root);
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }

    @Override
    public void onNext(ByteBuf chunk) {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON DECODER: onNext()");

            parser.feedInput(chunk.nioBuffer());
            bytesConsumed += chunk.readableBytes();

            JsonToken token;

            while ((token = parser.nextToken()) != JsonToken.NOT_AVAILABLE)
                parser.acceptToken(token);
        }
        catch (ETrac e) {

            // Error has already been handled, propagate as-is

            var errorMessage = "JSON decoding failed: " + e.getMessage();

            log.error(errorMessage, e);
            throw e;
        }
        catch (JacksonException e) {

            // This exception is a "well-behaved" parse failure, parse location and message should be meaningful

            var errorMessage = String.format("JSON decoding failed on line %d: %s",
                    e.getLocation().getLineNr(),
                    e.getOriginalMessage());

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (IOException e) {

            // Decoders work on a stream of buffers, "real" IO exceptions should not occur
            // IO exceptions here indicate parse failures, not file/socket communication errors
            // This is likely to be a more "badly-behaved" failure, or at least one that was not anticipated

            var errorMessage = "JSON decoding failed, content is garbled: " + e.getMessage();
            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (Throwable e)  {

            // Ensure unexpected errors are still reported to the Flow API
            log.error("Unexpected error during decoding", e);
            throw new EUnexpected(e);
        }
        finally {

            chunk.release();
        }
    }

    @Override
    public void onComplete() {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON DECODER: onComplete()");

            if (bytesConsumed == 0) {
                var error = new EDataCorruption("JSON data is empty");
                log.error(error.getMessage(), error);
                consumer().onError(error);
            }
            else {

                consumer().onComplete();
            }
        }
        finally {
            close();
        }
    }

    @Override
    public void onError(Throwable error) {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON DECODER: onError()");

            // TODO: Should datapipeline handle this?
            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    @Override
    public void close() {

        try {

            if (parser != null) {
                parser.close();
                parser = null;
            }

            if (root != null) {
                root.close();
                root = null;
            }
        }
        catch (IOException e) {

            // Ensure unexpected errors are still reported to the Flow API
            log.error("Unexpected error closing decoder: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }
}
