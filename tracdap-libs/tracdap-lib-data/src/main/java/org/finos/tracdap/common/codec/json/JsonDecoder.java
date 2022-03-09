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

import org.finos.tracdap.common.codec.BaseDecoder;
import org.finos.tracdap.common.codec.arrow.ArrowSchema;
import org.finos.tracdap.common.data.DataBlock;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class JsonDecoder extends BaseDecoder {

    private static final int BATCH_SIZE = 1024;
    private static final boolean CASE_INSENSITIVE = false;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private final SchemaDefinition tracSchema;
    private final Schema arrowSchema;

    private JsonStreamParser parser;

    public JsonDecoder(BufferAllocator arrowAllocator, SchemaDefinition tracSchema) {

        super(STREAMING_DECODER);

        this.arrowAllocator = arrowAllocator;
        this.tracSchema = tracSchema;

        // Schema cannot be inferred from JSON, so it must always be set from a TRAC schema
        this.arrowSchema = ArrowSchema.tracToArrow(tracSchema);
    }

    @Override
    protected void decodeStart() {

        try {

            var factory = new JsonFactory();

            var tableHandler = new JsonTableHandler(
                    arrowAllocator, arrowSchema, CASE_INSENSITIVE,
                    this::dispatchBatch, BATCH_SIZE);

            this.parser = new JsonStreamParser(factory, tableHandler);

            emitBlock(DataBlock.forSchema(this.arrowSchema));
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }

    @Override
    protected void decodeChunk(ByteBuf chunk) {

        try {

            var bytes = new byte[chunk.readableBytes()];
            chunk.readBytes(bytes);

            parser.feedInput(bytes, 0, bytes.length);

            JsonToken token;

            while ((token = parser.nextToken()) != JsonToken.NOT_AVAILABLE)
                parser.acceptToken(token);
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
    protected void decodeEnd() {

        // No-op
    }

    @Override
    public void close() {

        try {

            if (parser != null) {
                parser.close();
                parser = null;
            }
        }
        catch (IOException e) {

            // Ensure unexpected errors are still reported to the Flow API
            log.error("Unexpected error closing decoder: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
        finally {

            super.close();
        }
    }

    private void dispatchBatch(ArrowRecordBatch batch) {

        emitBlock(DataBlock.forRecords(batch));
    }
}
