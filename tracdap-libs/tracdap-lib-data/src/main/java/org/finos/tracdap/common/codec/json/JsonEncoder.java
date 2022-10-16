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

import org.finos.tracdap.common.codec.StreamingEncoder;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.util.ByteOutputStream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;


public class JsonEncoder extends StreamingEncoder implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private VectorSchemaRoot root;
    private Schema arrowSchema;

    private OutputStream out;
    private JsonGenerator generator;

    public JsonEncoder() {

    }

    @Override
    public void onStart(VectorSchemaRoot root) {

        try {

            emitStart();

            this.root = root;
            this.arrowSchema = root.getSchema();

            out = new ByteOutputStream(this::emitChunk);

            var factory = new JsonFactory();
            generator = factory.createGenerator(out, JsonEncoding.UTF8);

            // Tell Jackson to start the main array of records
            generator.writeStartArray();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);

            close();

            throw new EUnexpected(e);
        }
    }

    @Override
    public void onNext() {

        try {

            var nRows = root.getRowCount();
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

            close();

            throw new EUnexpected(e);
        }
    }

    @Override
    public void onComplete() {

        try {

            // Tell Jackson to end the main array of records
            generator.writeEndArray();

            // Flush and close output

            generator.close();
            generator = null;

            out.flush();
            out = null;

            emitEnd();
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

        close();
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

            // Encoder does not own root, do not close it

            if (root != null) {
                root = null;
            }
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error closing encoder: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }
}
