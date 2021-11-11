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

import com.accenture.trac.common.codec.BaseDecoder;
import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.codec.json.JacksonValues;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.exception.EData;
import com.accenture.trac.common.exception.EDataCorruption;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;


public class CsvDecoder extends BaseDecoder {

    private static final int BATCH_SIZE = 1024;

    private static final boolean DEFAULT_HEADER_FLAG = true;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private final SchemaDefinition tracSchema;

    private final Schema arrowSchema;
    private VectorSchemaRoot root;
    private VectorUnloader unloader;

    private final boolean headerFlag = DEFAULT_HEADER_FLAG;

    public CsvDecoder(BufferAllocator arrowAllocator, SchemaDefinition schema) {

        super(BUFFERED_DECODER);

        this.arrowAllocator = arrowAllocator;
        this.tracSchema = schema;

        // Schema cannot be inferred from CSV, so it must always be set from a TRAC schema
        this.arrowSchema = ArrowSchema.tracToArrow(this.tracSchema);
    }

    @Override
    protected void decodeStart() {

        this.root = ArrowSchema.createRoot(arrowSchema, arrowAllocator, BATCH_SIZE);
        this.unloader = new VectorUnloader(root);  // TODO: No compression support atm

        emitBlock(DataBlock.forSchema(this.arrowSchema));
    }

    @Override
    protected void decodeChunk(ByteBuf chunk) {

        var csvFactory = new CsvFactory()
                .enable(CsvParser.Feature.TRIM_SPACES);

        var csvSchema =  CsvSchemaMapping
                .arrowToCsv(this.arrowSchema)
                .build()
                .withHeader();

        try (var stream = new ByteBufInputStream(chunk);
             var parser = (CsvParser) csvFactory.createParser((InputStream) stream)) {

            parser.setSchema(csvSchema);
            parser.enable(CsvParser.Feature.TRIM_SPACES);

            var nRowsTotal = 0;
            var nRowsBatch = 0;
            var nBatches = 0;
            var nCols = arrowSchema.getFields().size();
            int col = 0;

            JsonToken token;

            while ((token = parser.nextToken()) != null) {

                // For CSV files, a null field name is produced for every field
                if (token == JsonToken.FIELD_NAME)
                    continue;

                if (token.isScalarValue()) {

                    var vector = root.getVector(col);
                    JacksonValues.parseAndSet(vector, nRowsBatch, parser, token);
                    col++;

                    continue;
                }

                if (token == JsonToken.START_OBJECT) {

                    if (nRowsBatch == 0) {
                        for (var vector : root.getFieldVectors())
                            vector.allocateNew();
                    }

                    continue;
                }

                if (token == JsonToken.END_OBJECT) {

                    nRowsBatch++;
                    nRowsTotal++;
                    col = 0;

                    if (nRowsBatch == BATCH_SIZE) {

                        root.setRowCount(nRowsBatch);
                        dispatchBatch(root);

                        nRowsBatch = 0;
                        nBatches++;
                    }

                    continue;
                }

                throw new EUnexpected();  // todo
            }

            // Check if there is a final batch that needs dispatching

            if (nRowsBatch > 0 || col > 0) {

                root.setRowCount(nRowsBatch);
                dispatchBatch(root);

                nBatches++;
            }

            log.info("CSV Codec: Decoded {} rows in {} batches", nRowsTotal, nBatches);
        }
        catch (JsonParseException e) {  // In Jackson JSON is the base class, JSON error is the parent of CSV error

            log.error("CSV content could not be decoded: Line {}, {}", e.getLocation().getLineNr(), e.getMessage(), e);

            var err = new EDataCorruption(e.getMessage(), e);  // todo: err
            doTargetError(err);
        }
        catch (IOException e) {

            log.error("CSV Decode error", e);

            root.clear();

            doTargetError(e);

            // TODO: Error
        }
        catch (Throwable e)  {

            log.error("CSV Decode error", e);

            doTargetError(e);
        }
        finally {

            chunk.release();
        }
    }

    @Override
    protected void decodeLastChunk() {

        // No-op, current version of CSV decoder buffers the full input
    }

    private void dispatchBatch(VectorSchemaRoot root) {

        var batch = unloader.getRecordBatch();
        var block = DataBlock.forRecords(batch);
        emitBlock(block);

        // Release memory in the root
        // Memory is still referenced by the batch, until the batch is consumed
        root.clear();
    }
}
