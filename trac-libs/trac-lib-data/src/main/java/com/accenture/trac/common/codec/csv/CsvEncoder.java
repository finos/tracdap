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

package com.accenture.trac.common.codec.csv;

import com.accenture.trac.common.codec.BaseEncoder;
import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.codec.arrow.ArrowValues;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.util.ByteOutputStream;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;


public class CsvEncoder extends BaseEncoder {

    private final SchemaDefinition tracSchema;
    private final Schema arrowSchema;

    private VectorSchemaRoot root;
    private VectorLoader loader;

    private CsvMapper mapper = null;
    private SequenceWriter outWriter;

    public CsvEncoder(BufferAllocator arrowAllocator, SchemaDefinition tracSchema) {

        this.tracSchema = tracSchema;
        this.arrowSchema = ArrowSchema.tracToArrow(this.tracSchema);

        var fields = arrowSchema.getFields();
        var vectors = new ArrayList<FieldVector>(fields.size());

        for (var field : fields)
            vectors.add(field.createVector(arrowAllocator));

        this.root = new VectorSchemaRoot(fields, vectors);
        this.loader = new VectorLoader(root);  // TODO: No compression support atm
    }

    @Override
    protected void encodeSchema(Schema arrowSchema) {

        try {

            // TODO: Compare schema to trac schema if available

            var csvSchema = CsvSchemaMapping
                    .arrowToCsv(arrowSchema)
                    .setUseHeader(true)  // tODO header
                    .build();

            var csvWriter = CsvMapper.builder().build()
                    .writerFor(Object.class)
                    .with(csvSchema);

            var outStream = new ByteOutputStream(outQueue::add);

            this.outWriter = csvWriter.writeValuesAsArray(outStream);
        }
        catch (IOException e) {

            throw new ETracInternal(e.getMessage(), e);  // TODO: Error
        }

    }

    @Override
    protected void encodeRecords(ArrowRecordBatch batch) {

        try (batch) {

            // Todo: when to call this
            if (outWriter == null)
                encodeSchema(this.arrowSchema);

            loader.load(batch);

            var nRows = root.getRowCount();
            var nCols = root.getFieldVectors().size();

            var csvValues = new Object[nCols];

            for (int row = 0; row < nRows; row++) {

                for (int col = 0; col < nCols; col++) {
                    var csvValue = ArrowValues.getValue(root, row, col);
                    csvValues[col] = csvValue;
                }

                outWriter.write(csvValues);
            }

            outWriter.flush();
        }
        catch (IOException e) {

            // TODO: Error
        }
        finally {

            root.clear();
        }
    }

    @Override
    protected void encodeDictionary(ArrowDictionaryBatch batch) {

        throw new ETracInternal("CSV Dictionary encoding not supported");
    }
}
