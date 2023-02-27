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

package org.finos.tracdap.common.codec.csv;

import org.finos.tracdap.common.codec.StreamingEncoder;
import org.finos.tracdap.common.codec.json.JacksonValues;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.data.util.ByteOutputStream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;


public class CsvEncoder extends StreamingEncoder implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private VectorSchemaRoot root;
    private Schema actualSchema;

    private OutputStream out;
    private CsvGenerator generator;


    public CsvEncoder() {

    }

    @Override
    public void onStart(VectorSchemaRoot root) {

        try {

            if (log.isTraceEnabled())
                log.trace("CSV ENCODER: onStart()");

            consumer().onStart();

            this.root = root;
            this.actualSchema = root.getSchema();

            var factory = new CsvFactory()
                    // Make sure empty strings are quoted, so they can be distinguished from nulls
                    .enable(CsvGenerator.Feature.ALWAYS_QUOTE_EMPTY_STRINGS);

            var csvSchema = CsvSchemaMapping
                    .arrowToCsv(actualSchema)
                    .build()
                    .withHeader();

            out = new ByteOutputStream(bb -> consumer().onNext(bb));
            generator = factory.createGenerator(out, JsonEncoding.UTF8);
            generator.setSchema(csvSchema);

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
    public void onBatch() {

        try {

            if (log.isTraceEnabled())
                log.trace("CSV ENCODER: onNext()");

            var nRows = root.getRowCount();
            var nCols = actualSchema.getFields().size();

            for (var row = 0; row < nRows; row++) {

                generator.writeStartArray();

                for (var col = 0; col < nCols; col++) {

                    var vector = root.getVector(col);
                    JacksonValues.getAndGenerate(vector, row, generator);
                }

                generator.writeEndArray();
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

            if (log.isTraceEnabled())
                log.trace("CSV ENCODER: onComplete()");

            // Tell Jackson to end the main array of records
            generator.writeEndArray();

            // Flush and close output

            generator.close();
            generator = null;

            out.close();
            out = null;

            markAsDone();
            consumer().onComplete();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);

            close();

            throw new EUnexpected(e);
        }
    }

    @Override
    public void onError(Throwable error) {

        try {

            if (log.isTraceEnabled())
                log.trace("CSV ENCODER: onError()");

            markAsDone();
            consumer().onError(error);
        }
        finally {
            close();
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

            // Encoder does not own root, do not try to close it

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
