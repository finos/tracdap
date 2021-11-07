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

import com.accenture.trac.common.codec.BaseEncoder;
import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.codec.arrow.ArrowValues;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.util.ByteOutputStream;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;


public class JsonEncoder extends BaseEncoder {

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
            this.loader = new VectorLoader(root);  // TODO: No compression support atm

            out = new ByteOutputStream(outQueue::add);

            var factory = new JsonFactory();
            generator = factory.createGenerator(out, JsonEncoding.UTF8);
            generator.writeStartArray();
        }
        catch (IOException e) {

            throw new ETracInternal(e.getMessage(), e);  // todo
        }
    }

    @Override
    protected void encodeRecords(ArrowRecordBatch batch) {

        try (batch) {

            if (arrowSchema == null)
                throw new EUnexpected();  // TODO: Data error, invalid stream, in base encoder

            loader.load(batch);

            var nRows = batch.getLength();
            var nCols = arrowSchema.getFields().size();

            for (var row = 0; row < nRows; row++) {

                generator.writeStartObject();

                for (var col = 0; col < nCols; col++)
                    writeField(root, row, col);

                generator.writeEndObject();
            }
        }
        catch (IOException e) {

            throw new ETracInternal(e.getMessage(), e);  // todo
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
            if (arrowSchema == null)
                throw new EUnexpected();  // TODO: Data error, invalid stream, in base encoder

            generator.writeEndArray();
            generator.close();
            generator = null;

            out.close();
            out = null;
        }
        catch (IOException e) {

            throw new ETracInternal(e.getMessage(), e);  // todo
        }
    }

    private void writeField(VectorSchemaRoot root, int row, int col) throws IOException {

        var field = arrowSchema.getFields().get(col);
        var fieldName = field.getName();

        var value = ArrowValues.getValue(root, row, col);

        if (value == null) {
            generator.writeNullField(fieldName);
            return;
        }

        var minorType = root.getVector(col).getMinorType();

        switch (minorType) {

            case BIT: generator.writeBooleanField(fieldName, (boolean) value); break;

            case BIGINT: generator.writeNumberField(fieldName, (long) value); break;
            case INT: generator.writeNumberField(fieldName, (int) value); break;
            case SMALLINT: generator.writeNumberField(fieldName, (short) value); break;
            case TINYINT: generator.writeNumberField(fieldName, (byte) value); break;

            case FLOAT8: generator.writeNumberField(fieldName, (double) value); break;
            case FLOAT4: generator.writeNumberField(fieldName, (float) value); break;

            case DECIMAL:
            case DECIMAL256:
                var decimal = (BigDecimal) value;
                generator.writeStringField(fieldName, decimal.toString());
                break;

            case VARCHAR:
                generator.writeStringField(fieldName, value.toString());
                break;

            case DATEDAY:
            case DATEMILLI:
                var dateValue = (LocalDate) value;
                var dateIso = MetadataCodec.ISO_DATE_FORMAT.format(dateValue);
                generator.writeStringField(fieldName, dateIso);
                break;

            // TODO: Datetime type

            default:

                throw new EUnexpected();  // TODO: data error, field type not supported
        }
    }
}
