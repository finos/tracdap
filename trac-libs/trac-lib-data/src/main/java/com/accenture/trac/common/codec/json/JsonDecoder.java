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
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class JsonDecoder extends BaseDecoder {

    private static final int BATCH_SIZE = 1024;
    private static final boolean CASE_INSENSITIVE = false;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private final SchemaDefinition tracSchema;

    private final Schema arrowSchema;

    private ByteArrayFeeder feeder;
    private JsonParser lexer;
    private JsonParserBase parser;


    public JsonDecoder(BufferAllocator arrowAllocator, SchemaDefinition tracSchema) {

        this.arrowAllocator = arrowAllocator;
        this.tracSchema = tracSchema;

        // Schema cannot be inferred from JSON, so it must always be set from a TRAC schema
        this.arrowSchema = ArrowSchema.tracToArrow(tracSchema);
    }

    @Override
    protected void decodeStart() {

        try {

//            this.root = ArrowSchema.createRoot(arrowSchema, arrowAllocator, BATCH_SIZE);
//            this.unloader = new VectorUnloader(root);  // TODO: No compression support atm

            var factory = new JsonFactory();
            this.lexer = factory.createNonBlockingByteArrayParser();
            this.feeder = (ByteArrayFeeder) lexer.getNonBlockingInputFeeder();

            this.parser = new JsonTableParser(
                    arrowSchema, arrowAllocator,
                    lexer, CASE_INSENSITIVE,
                    this::dispatchBatch, BATCH_SIZE);

            outQueue.add(DataBlock.forSchema(this.arrowSchema));
        }
        catch (IOException e) {

            throw new EUnexpected(e);  // TODO
        }
    }

    @Override
    protected void decodeChunk(ByteBuf chunk) {

        try {

            if (!feeder.needMoreInput() && buffer.readableBytes() > 0)
                throw new EUnexpected(); // TODO: EDataInvalidStream

            log.info("Next chunk");

            var bytes = new byte[chunk.readableBytes()];
            chunk.readBytes(bytes);

            var text = new String(bytes, StandardCharsets.UTF_8);
            log.info(text);


            feeder.feedInput(bytes, 0, bytes.length);

            JsonToken token;

            while ((token = lexer.nextToken()) != JsonToken.NOT_AVAILABLE) {

                log.info("JSON Token: {}", token);
                parser.acceptToken(token);
            }

            log.info("Chunk done");
        }
        catch (IOException e) {

            throw new EUnexpected(e);  // TODO
        }
        finally {
            // buffer.release();
        }
    }

    @Override
    protected void decodeLastChunk() {

        // throw new RuntimeException("Not done yet");
    }

//    private void dispatchBatch(VectorSchemaRoot root) {
//
//        var batch = unloader.getRecordBatch();
//        var block = DataBlock.forRecords(batch);
//        outQueue.add(block);
//
//        // Release memory in the root
//        // Memory is still referenced by the batch, until the batch is consumed
//        root.clear();
//    }

    private void dispatchBatch(ArrowRecordBatch batch) {

        var block = DataBlock.forRecords(batch);
        outQueue.add(block);
    }
}
