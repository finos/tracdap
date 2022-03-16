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

import org.finos.tracdap.common.codec.BaseDecoder;
import org.finos.tracdap.common.codec.arrow.ArrowSchema;
import org.finos.tracdap.common.codec.json.JacksonValues;
import org.finos.tracdap.common.data.DataBlock;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.metadata.SchemaDefinition;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;


public class CsvDecoder extends BaseDecoder {

    private static final int BATCH_SIZE = 1024;

    private static final boolean DEFAULT_HEADER_FLAG = true;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private final SchemaDefinition tracSchema;

    private final Schema arrowSchema;
    private VectorSchemaRoot root;
    private VectorUnloader unloader;

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

        // Record batches in the TRAC intermediate data stream are always uncompressed
        // So, there is no need to use a compression codec here
        this.unloader = new VectorUnloader(root);

        emitBlock(DataBlock.forSchema(this.arrowSchema));
    }

    @Override
    protected void decodeChunk(ByteBuf chunk) {

        var csvFactory = new CsvFactory()
                // Require strict adherence to the schema
                .enable(CsvParser.Feature.FAIL_ON_MISSING_COLUMNS)
                // Always allow nulls during parsing (they will be rejected later for non-nullable fields)
                .enable(CsvParser.Feature.EMPTY_STRING_AS_NULL)
                // Permissive handling of extra space (strings with leading/trailing spaces must be quoted anyway)
                .enable(CsvParser.Feature.TRIM_SPACES);

        try (var stream = new ByteBufInputStream(chunk);
             var parser = (CsvParser) csvFactory.createParser((InputStream) stream)) {

            var csvSchema =  CsvSchemaMapping
                    .arrowToCsv(this.arrowSchema)
                    .build();

            csvSchema = DEFAULT_HEADER_FLAG
                    ? csvSchema.withHeader()
                    : csvSchema.withoutHeader();

            parser.setSchema(csvSchema);

            var row = 0;
            var col = 0;

            JsonToken token;

            while ((token = parser.nextToken()) != null) {

                switch (token) {

                    // For CSV files, a null field name is produced for every field
                    case FIELD_NAME:
                        continue;

                    case VALUE_NULL:

                        // Special handling to differentiate between null and empty strings

                        var nullVector = root.getVector(col);
                        var minorType = nullVector.getMinorType();

                        if (minorType == Types.MinorType.VARCHAR) {

                            // An empty string is encoded as "", i.e. token width = 2

                            var tokenEnd = parser.currentLocation();
                            var tokenStart = parser.currentTokenLocation();
                            var tokenWidth = tokenEnd.getColumnNr() - tokenStart.getColumnNr();

                            if (tokenWidth == 2) {
                                JacksonValues.setEmptyString(nullVector, row);
                                continue;
                            }
                        }

                        // If this value is not an empty string, fall through to the default handling

                    case VALUE_TRUE:
                    case VALUE_FALSE:
                    case VALUE_STRING:
                    case VALUE_NUMBER_INT:
                    case VALUE_NUMBER_FLOAT:

                        var vector = root.getVector(col);
                        JacksonValues.parseAndSet(vector, row, parser, token);
                        col++;

                        break;

                    case START_OBJECT:

                        if (row == 0)
                            for (var vector_ : root.getFieldVectors())
                                vector_.allocateNew();

                        break;

                    case END_OBJECT:

                        row++;
                        col = 0;

                        if (row == BATCH_SIZE) {

                            root.setRowCount(row);
                            dispatchBatch(root);

                            row = 0;
                        }

                        break;

                    default:

                        var msg = String.format("Unexpected token %s", token.name());
                        throw new CsvReadException(parser, msg, csvSchema);
                }
            }

            // Check if there is a final batch that needs dispatching

            if (row > 0 || col > 0) {

                root.setRowCount(row);
                dispatchBatch(root);
            }
        }
        catch (JacksonException e) {

            // This exception is a "well-behaved" parse failure, parse location and message should be meaningful

            var errorMessage = String.format("CSV decoding failed on line %d: %s",
                    e.getLocation().getLineNr(),
                    e.getOriginalMessage());

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (IOException e) {

            // Decoders work on a stream of buffers, "real" IO exceptions should not occur
            // IO exceptions here indicate parse failures, not file/socket communication errors
            // This is likely to be a more "badly-behaved" failure, or at least one that was not anticipated

            var errorMessage = "CSV decoding failed, content is garbled: " + e.getMessage();

            log.error(errorMessage, e);
            throw new EDataCorruption(errorMessage, e);
        }
        catch (Throwable e)  {

            // Ensure unexpected errors are still reported to the Flow API

            log.error("Unexpected error in CSV decoding", e);
            throw new EUnexpected(e);
        }
        finally {

            chunk.release();
        }
    }

    @Override
    protected void decodeEnd() {

        // No-op, current version of CSV decoder buffers the full input
    }

    @Override
    public void close() {

        try {

            if (root != null) {
                root.close();
                root = null;
            }
        }
        finally {

            super.close();
        }
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
