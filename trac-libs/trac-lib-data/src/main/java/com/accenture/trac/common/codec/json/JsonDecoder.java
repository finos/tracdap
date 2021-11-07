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

package com.accenture.trac.common.codec.json;

import com.accenture.trac.common.codec.BaseDecoder;
import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;


public class JsonDecoder extends BaseDecoder {

    private static final int BATCH_SIZE = 1024;

    private final BufferAllocator arrowAllocator;
    private final SchemaDefinition tracSchema;

    private final Schema arrowSchema;
    private VectorSchemaRoot root;
    private VectorUnloader unloader;
    private ByteArrayFeeder feeder;
    private JsonParser parser;

    public JsonDecoder(BufferAllocator arrowAllocator, SchemaDefinition tracSchema) {

        this.arrowAllocator = arrowAllocator;
        this.tracSchema = tracSchema;

        // Schema cannot be inferred from JSON, so it must always be set from a TRAC schema
        this.arrowSchema = ArrowSchema.tracToArrow(tracSchema);
    }

    @Override
    protected void decodeStart() {

        try {

            this.root = ArrowSchema.createRoot(arrowSchema, arrowAllocator, BATCH_SIZE);
            this.unloader = new VectorUnloader(root);  // TODO: No compression support atm

            var factory = new JsonFactory();
            this.parser = factory.createNonBlockingByteArrayParser();
            this.feeder = (ByteArrayFeeder) parser.getNonBlockingInputFeeder();
        }
        catch (IOException e) {

            throw new EUnexpected(e);  // TODO
        }
    }

    @Override
    protected void decodeChunk() {

        try {

            if (!feeder.needMoreInput() && buffer.readableBytes() > 0)
                throw new EUnexpected(); // TODO: EDataInvalidStream

            var bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            feeder.feedInput(bytes, 0, bytes.length);

            JsonToken token;

            while ((token = parser.nextToken()) != JsonToken.NOT_AVAILABLE) {

                // TODO: Parser impl
            }
        }
        catch (IOException e) {

            throw new EUnexpected(e);  // TODO
        }
        finally {
            buffer.release();
        }
    }

    @Override
    protected void decodeLastChunk() {

    }
}
