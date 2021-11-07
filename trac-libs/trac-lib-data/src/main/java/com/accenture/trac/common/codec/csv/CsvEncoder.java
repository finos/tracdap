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
import com.accenture.trac.common.codec.arrow.ArrowValues;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.util.ByteOutputStream;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;


public class CsvEncoder extends BaseEncoder {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private final SchemaDefinition tracSchema;

    private  Schema arrowSchema;
    private VectorSchemaRoot root;
    private VectorLoader loader;

    private OutputStream out;
    private CsvGenerator generator;


    public CsvEncoder(BufferAllocator arrowAllocator, SchemaDefinition tracSchema) {

        this.arrowAllocator = arrowAllocator;
        this.tracSchema = tracSchema;
    }

    @Override
    protected void encodeSchema(Schema arrowSchema) {

        try {

            // TODO: Compare schema to trac schema if available

            this.arrowSchema = arrowSchema;
            this.root = createRoot(arrowSchema);
            this.loader = new VectorLoader(root);  // TODO: No compression support atm

            var factory = new CsvFactory();

            var csvSchema = CsvSchemaMapping
                    .arrowToCsv(arrowSchema)
                    .setUseHeader(true)  // tODO header
                    .build()
                    .withHeader();

            out = new ByteOutputStream(outQueue::add);
            generator = factory.createGenerator(out, JsonEncoding.UTF8);
            generator.setSchema(csvSchema);
        }
        catch (IOException e) {

            throw new ETracInternal(e.getMessage(), e);  // TODO: Error
        }
    }

    @Override
    protected void encodeRecords(ArrowRecordBatch batch) {

        try (batch) {

            loader.load(batch);

            var nRows = batch.getLength();
            var nCols = arrowSchema.getFields().size();

            for (var row = 0; row < nRows; row++) {

                generator.writeStartArray();

                for (var col = 0; col < nCols; col++)
                    writeField(root, row, col);

                generator.writeEndArray();
            }
        }
        catch (IOException e) {

            log.error(e.getMessage(), e);

            // TODO: Error
        }
        finally {

            root.clear();
        }
    }

    @Override
    protected void encodeDictionary(ArrowDictionaryBatch batch) {

        throw new ETracInternal("CSV dictionary encoding not supported");
    }

    @Override
    protected void encodeEos() {

        try {

            if (arrowSchema == null)
                throw new EUnexpected();  // TODO: Data error, invalid stream, in base encoder

            //generator.writeEndArray();
            generator.close();
            generator = null;

            out.close();
            out = null;
        }
        catch (IOException e) {

            throw new ETracInternal(e.getMessage(), e);  // todo
        }
    }

    private VectorSchemaRoot createRoot(Schema arrowSchema) {

        var fields = arrowSchema.getFields();
        var vectors = new ArrayList<FieldVector>(fields.size());

        for (var field : fields)
            vectors.add(field.createVector(arrowAllocator));

        return new VectorSchemaRoot(fields, vectors);
    }

    private void writeField(VectorSchemaRoot root, int row, int col) throws IOException {

        var value = ArrowValues.getValue(root, row, col);

        if (value == null) {
            generator.writeNull();
            return;
        }

        var minorType = root.getVector(col).getMinorType();

        switch (minorType) {

            case BIT: generator.writeBoolean((boolean) value); break;

            case BIGINT: generator.writeNumber((long) value); break;
            case INT: generator.writeNumber((int) value); break;
            case SMALLINT: generator.writeNumber((short) value); break;
            case TINYINT: generator.writeNumber((byte) value); break;

            case FLOAT8: generator.writeNumber((double) value); break;
            case FLOAT4: generator.writeNumber((float) value); break;

            case DECIMAL:
            case DECIMAL256:
                var decimal = (BigDecimal) value;
                generator.writeString(decimal.toString());
                break;

            case VARCHAR:
                generator.writeString(value.toString());
                break;

            case DATEDAY:
            case DATEMILLI:
                var dateValue = (LocalDate) value;
                var dateIso = MetadataCodec.ISO_DATE_FORMAT.format(dateValue);
                generator.writeString(dateIso);
                break;

            // TODO: Datetime type

            default:

                throw new EUnexpected();  // TODO: data error, field type not supported
        }
    }
}
