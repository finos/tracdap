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

import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.concurrent.flow.CommonBaseProcessor;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.metadata.SchemaDefinition;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;


public class CsvDecoder extends CommonBaseProcessor<ByteBuf, DataBlock> {

    private static final int BATCH_SIZE = 1024;
    private static final DataBlock END_OF_STREAM = DataBlock.empty();

    private static final boolean DEFAULT_HEADER_FLAG = true;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final SchemaDefinition tracSchema;
    private final Schema arrowSchema;

    private final VectorSchemaRoot root;
    private final VectorUnloader unloader;
    private final CompositeByteBuf buffer;
    private final Queue<DataBlock> outQueue;

    private final boolean headerFlag = DEFAULT_HEADER_FLAG;

    public CsvDecoder(SchemaDefinition schema) {

        var allocator = new RootAllocator();

        this.tracSchema = schema;
        this.arrowSchema = ArrowSchema.tracToArrow(this.tracSchema);

        var fields = arrowSchema.getFields();
        var vectors = new ArrayList<FieldVector>(fields.size());

        for (var field : fields) {

            var vector = field.createVector(allocator);
            vector.setInitialCapacity(BATCH_SIZE);

            vectors.add(vector);
        }

        this.root = new VectorSchemaRoot(fields, vectors);
        this.unloader = new VectorUnloader(root);  // TODO: No compression support atm

        this.buffer = ByteBufAllocator.DEFAULT.compositeBuffer();
        this.outQueue = new ArrayDeque<>();
    }

    @Override
    protected void handleTargetRequest() {

        deliverPendingBlocks();

        if (nTargetRequested() > nTargetDelivered() && nSourceRequested() <= nSourceDelivered())
            doSourceRequest(1);
    }

    @Override
    protected void handleTargetCancel() {

        releaseBuffer();
    }

    @Override
    protected void handleSourceNext(ByteBuf chunk) {

        buffer.addComponent(chunk);
        doSourceRequest(1);
    }

    @Override
    protected void handleSourceError(Throwable error) {

        releaseBuffer();
    }

    @Override
    protected void handleSourceComplete() {

        decodeInput();
        releaseBuffer();

        deliverPendingBlocks();
    }

    private void deliverPendingBlocks() {

        while (nTargetDelivered() < nTargetRequested()) {

            var block = outQueue.poll();

            if (block == END_OF_STREAM)
                doTargetComplete();

            else if (block != null)
                doTargetNext(block);

            else
                return;
        }
    }

    private void releaseBuffer() {

        var releaseOk = buffer.release();

        if (!releaseOk && buffer.capacity() > 0)
            log.warn("CSV decode buffer was not released (this could indicate a memory leak)");
    }

    private void decodeInput() {

        outQueue.add(DataBlock.forSchema(this.tracSchema, this.arrowSchema));

        var csvSchema =  CsvSchemaMapping
                .arrowToCsv(this.arrowSchema)
                .setUseHeader(headerFlag)
                .build();

        var csvMapper = CsvMapper.builder().build()
                .readerForArrayOf(Object.class)
                .with(csvSchema);

        try (var stream = new ByteBufInputStream(buffer);
             var itr = csvMapper.readValues((InputStream) stream)) {

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
                var csvValues = (Object[]) itr.nextValue();

                for (int col = 0; col < nCols; col++)
                    setValue(row, col, csvValues[col], root);

                nRowsTotal++;
                nRowsBatch++;

                // When the batch is full, dispatch it

                if (nRowsBatch == BATCH_SIZE) {

                    dispatchBatch(root);
                    nRowsBatch = 0;
                    nBatches++;
                }
            }

            // Check if there is a final batch that needs dispatching

            if (nRowsBatch > 0) {

                dispatchBatch(root);
                nBatches++;
            }

            log.info("CSV Codec: Decoded {} rows in {} batches", nRowsTotal, nBatches);
        }
        catch (IOException e) {

            root.clear();

            // TODO: Error
        }

    }

    private void setValue(int row, int col, Object obj, VectorSchemaRoot root) {

        var vector = root.getVector(col);
        var arrowTypeId = vector.getField().getType().getTypeID();

        var isSet = (obj != null) ? 1 : 0;

        switch (arrowTypeId) {

            case Bool:

                var boolVec = (BitVector) vector;
                var boolVal = (isSet != 0 && (Boolean) obj) ? 1 : 0;

                boolVec.set(row, isSet, boolVal);

                break;

            case Int:

                var intVec = (BigIntVector) vector;
                var intVal = isSet != 0 ? (Long) obj : 0;

                intVec.set(row, isSet, intVal);

                break;

            case FloatingPoint:

                var floatVec = (Float8Vector) vector;
                var floatVal = isSet != 0 ? (Double) obj : 0;

                floatVec.set(row, isSet, floatVal);

                break;

            case Decimal:

                var decimalVec = (Decimal256Vector) vector;
                var decimalVal = (BigDecimal) obj;

                if (isSet == 0)
                    decimalVec.setNull(row);
                else
                    decimalVec.set(row, decimalVal);

                break;

            case Utf8:

                var varcharVec = (VarCharVector) vector;
                var varcharVal = (String) obj;

                if (isSet == 0)
                    varcharVec.setNull(row);
                else
                    varcharVec.set(row, varcharVal.getBytes(StandardCharsets.UTF_8));

                break;

            case Date:

                var dateVec = (DateDayVector) vector;
                var dateVal = (java.time.LocalDate) obj;
                var unixEpochDay = (int) dateVal.toEpochDay();

                dateVec.set(row, isSet, unixEpochDay);

                break;

            case Timestamp:

                var timestampVec = (TimeStampMilliVector) vector;
                var timestampVal = (java.time.OffsetDateTime) obj;

                var epochSecond = timestampVal.toEpochSecond();
                var nanos = timestampVal.getNano();
                var millis = nanos / 1000;
                var epochMillis = (epochSecond * 1000 * 1000) + millis;

                timestampVec.set(row, isSet, epochMillis);

                break;

            default:

                throw new EUnexpected();  // TODO: Error

        }
    }

    private void dispatchBatch(VectorSchemaRoot root) {

        var batch = unloader.getRecordBatch();
        var block = DataBlock.forRecords(batch);
        outQueue.add(block);

        // Release memory in the root
        // Memory is still referenced by the batch, until the batch is consumed
        root.clear();
    }
}
