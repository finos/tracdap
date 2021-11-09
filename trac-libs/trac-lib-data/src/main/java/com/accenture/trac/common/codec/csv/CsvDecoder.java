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
import com.accenture.trac.common.codec.arrow.ArrowValues;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;


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


        // TODO: Parser to String[] instead of Map
        // Map is created for every row, and updated / referenced for every field

        var csvSchema =  CsvSchemaMapping
                .arrowToCsv(this.arrowSchema)
                .build()
                .withHeader();

        var csvReader = CsvMapper.builder().build()
                .readerForMapOf(String.class)
                .with(csvSchema)
                .with(CsvParser.Feature.WRAP_AS_ARRAY);

        try (var stream = new ByteBufInputStream(chunk);
             MappingIterator<Map<String, String>> itr = csvReader.readValues((InputStream) stream)) {

            var nRowsTotal = 0;
            var nRowsBatch = 0;
            var nBatches = 0;
            var nCols = arrowSchema.getFields().size();

            while (itr.hasNextValue()) {

                // If this is a new batch, allocator memory in the root container
                // (memory of the previous batch is no longer owned by the root container)

                if (nRowsBatch == 0) {

                    for (var vector : root.getFieldVectors())
                        vector.allocateNew();
                }

                // Write the next row of values into the arrow root container

                var row = nRowsBatch;
                var csvValues = (Map<String, String>) itr.nextValue();

                for (int col = 0; col < nCols; col++) {

                    var csvCol = csvSchema.column(col);
                    var csvValue = csvValues.get(csvCol.getName());

                    if (csvCol.getType() == CsvSchema.ColumnType.NUMBER) {
                        var numericValue = NumberFormat.getInstance().parse(csvValue.trim());
                        ArrowValues.setValue(root, row, col, numericValue);
                    }
                    else
                        ArrowValues.setValue(root, row, col, csvValue);
                }

                nRowsTotal++;
                nRowsBatch++;

                // When the batch is full, dispatch it

                if (nRowsBatch == BATCH_SIZE) {

                    root.setRowCount(nRowsBatch);
                    dispatchBatch(root);

                    nRowsBatch = 0;
                    nBatches++;
                }
            }

            // Check if there is a final batch that needs dispatching

            if (nRowsBatch > 0) {

                root.setRowCount(nRowsBatch);
                dispatchBatch(root);

                nBatches++;
            }

            log.info("CSV Codec: Decoded {} rows in {} batches", nRowsTotal, nBatches);
        }
        catch (IOException | ParseException e) {

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
