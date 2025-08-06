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

package org.finos.tracdap.common.codec.text;

import org.finos.tracdap.common.codec.StreamingDecoder;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.ArrowVsrSchema;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.EUnexpected;

import com.fasterxml.jackson.core.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.BiConsumer;


public class BaseTextDecoder extends StreamingDecoder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ArrowVsrSchema schema;
    private final BufferAllocator allocator;
    private final TextFileConfig config;
    private final BiConsumer<JsonParser, ArrowVsrContext> parserSetup;

    private TextFileReader reader;
    private ArrowVsrContext context;

    private long bytesConsumed;
    private boolean done;

    public BaseTextDecoder(
            ArrowVsrSchema schema,
            BufferAllocator allocator,
            TextFileConfig config) {

        this(schema, allocator, config, null);
    }

    public BaseTextDecoder(
            ArrowVsrSchema schema,
            BufferAllocator allocator,
            TextFileConfig config,
            BiConsumer<JsonParser, ArrowVsrContext> parserSetup) {

        this.schema = schema;
        this.allocator = allocator;
        this.config = config;
        this.parserSetup = parserSetup;
    }

    @Override
    public void onStart() {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON DECODER: onStart()");

            this.reader = new TextFileReader(
                    schema.physical(),
                    schema.dictionaryFields(),
                    schema.dictionaries(),
                    allocator, config);

            this.context = ArrowVsrContext.forSource(
                    reader.getVectorSchemaRoot(),
                    reader, allocator);

            if (parserSetup != null)
                parserSetup.accept(reader.getParser(), context);

            consumer().onStart(context);
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }

    @Override
    public void onNext(ArrowBuf chunk) {

        try (chunk) {  // automatically close chunk as a resource

            if (log.isTraceEnabled())
                log.trace("JSON DECODER: onNext()");

            // Empty chunks are allowed in the stream but should be ignored
            if (chunk.readableBytes() > 0) {

                reader.feedInput(chunk.nioBuffer());
                bytesConsumed += chunk.readableBytes();

                done = doParse();
            }
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
            else if (!done) {
                var error = new EDataCorruption("JSON data is incomplete");
                log.error(error.getMessage(), error);
                consumer().onError(error);
            }
            else {

                markAsDone();
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

            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    boolean doParse() throws IOException {

        do {

            if (context.readyToFlip())
                context.flip();

            if (context.readyToUnload() && consumerReady())
                consumer().onBatch();

            if (context.readyToLoad() && reader.hasBatch()) {

                reader.resetBatch(context.getBackBuffer());

                if (reader.readBatch())
                    context.setLoaded();
                else
                    return false;
            }

        } while (context.readyToFlip());

        return ! reader.hasBatch();
    }

    @Override
    public void close() {

        try {

            if (reader != null) {
                reader.close();
                reader = null;
            }

            if (context != null) {
                context.close();
                context = null;
            }
        }
        catch (IOException e) {

            // Ensure unexpected errors are still reported to the Flow API
            log.error("Unexpected error closing decoder: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }
}
