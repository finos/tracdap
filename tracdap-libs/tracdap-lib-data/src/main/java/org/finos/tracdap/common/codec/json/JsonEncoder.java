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

import org.finos.tracdap.common.codec.BaseEncoder;
import org.finos.tracdap.common.codec.arrow.ArrowSchema;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.ByteOutputStream;
import org.finos.tracdap.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;


public class JsonEncoder extends BaseEncoder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private final SchemaDefinition tracSchema;

    private Schema arrowSchema;
    private VectorSchemaRoot root;
    private VectorLoader loader;
    private OutputStream out;
    private JsonGenerator generator;

    public JsonEncoder(BufferAllocator arrowAllocator, SchemaDefinition tracSchema) {

        this.arrowAllocator = arrowAllocator;
        this.tracSchema = tracSchema;
    }

    @Override
    protected void encodeSchema(Schema arrowSchema) {

        try {

            this.arrowSchema = arrowSchema;
            this.root = ArrowSchema.createRoot(arrowSchema, arrowAllocator);

            // Record batches in the TRAC intermediate data stream are always uncompressed
            // So, there is no need to use a compression codec here
            this.loader = new VectorLoader(root);

            out = new ByteOutputStream(this::emitChunk);

            var factory = new JsonFactory();
            generator = factory.createGenerator(out, JsonEncoding.UTF8);

            // Tell Jackson to start the main array of records
            generator.writeStartArray();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }

    @Override
    protected void encodeRecords(ArrowRecordBatch batch) {

        try (batch) {

            loader.load(batch);

            var nRows = batch.getLength();
            var nCols = arrowSchema.getFields().size();

            for (var row = 0; row < nRows; row++) {

                generator.writeStartObject();

                for (var col = 0; col < nCols; col++) {

                    var vector = root.getVector(col);
                    var fieldName = vector.getName();

                    generator.writeFieldName(fieldName);
                    JacksonValues.getAndGenerate(vector, row, generator);
                }

                generator.writeEndObject();
            }
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
        finally {

            root.clear();
        }
    }

    @Override
    protected void encodeDictionary(ArrowDictionaryBatch batch) {

        throw new ETracInternal("JSON dictionary encoding not supported");
    }

    @Override
    protected void encodeEos() {

        try {

            // Tell Jackson to end the main array of records
            generator.writeEndArray();

            // Flush and close output

            generator.close();
            generator = null;

            out.flush();
            out = null;
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }

    @Override
    public void close() {

        try {

            if (generator != null) {
                generator.close();
                generator = null;
            }

            if (out != null) {
                out.close();
                out = null;
            }

            if (root != null) {
                root.close();
                root = null;
            }
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error closing encoder: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
        finally {

            super.close();
        }
    }
}
