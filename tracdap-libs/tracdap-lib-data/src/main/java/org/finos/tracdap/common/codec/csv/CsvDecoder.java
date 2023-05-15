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

import org.finos.tracdap.common.codec.BufferDecoder;
import org.finos.tracdap.common.data.ArrowSchema;
import org.finos.tracdap.common.codec.json.JacksonValues;
import org.finos.tracdap.common.data.util.ByteSeekableChannel;
import org.finos.tracdap.common.data.util.Bytes;
import org.finos.tracdap.common.exception.EDataCorruption;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.Callable;


public class CsvDecoder extends BufferDecoder {

    private static final int BATCH_SIZE = 1024;
    private static final boolean DEFAULT_HEADER_FLAG = true;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator arrowAllocator;
    private final Schema arrowSchema;

    private List<ArrowBuf> buffer;
    private CsvParser csvParser;
    private CsvSchema csvSchema;
    private VectorSchemaRoot root;

    public CsvDecoder(BufferAllocator arrowAllocator, Schema arrowSchema) {

        this.arrowAllocator = arrowAllocator;

        // Schema cannot be inferred from CSV, so it must always be set from a TRAC schema
        this.arrowSchema = arrowSchema;
    }

    @Override
    public void onBuffer(List<ArrowBuf> buffer) {

        if (log.isTraceEnabled())
            log.trace("CSV DECODER: onBuffer()");

        // Sanity check, should never happen
        if (isDone() || this.buffer != null) {
            var error = new ETracInternal("CSV data parsed twice (this is a bug)");
            log.error(error.getMessage(), error);
            throw error;
        }

        // Empty file can and does happen, treat it as data corruption
        if (Bytes.readableBytes(buffer) == 0) {
            var error = new EDataCorruption("CSV data is empty");
            log.error(error.getMessage(), error);
            throw error;
        }

        handleErrors(() -> {

            // Set up all the resources needed for parsing

            this.buffer = buffer;

            var csvFactory = new CsvFactory()
                    // Require strict adherence to the schema
                    .enable(CsvParser.Feature.FAIL_ON_MISSING_COLUMNS)
                    // Always allow nulls during parsing (they will be rejected later for non-nullable fields)
                    .enable(CsvParser.Feature.EMPTY_STRING_AS_NULL)
                    // Permissive handling of extra space (strings with leading/trailing spaces must be quoted anyway)
                    .enable(CsvParser.Feature.TRIM_SPACES);

            var channel = new ByteSeekableChannel(buffer);
            var stream = Channels.newInputStream(channel);

            csvParser = csvFactory.createParser(stream);
            csvSchema = CsvSchemaMapping
                    .arrowToCsv(this.arrowSchema)
                    //.setNullValue("")
                    .build();
            csvSchema = DEFAULT_HEADER_FLAG
                    ? csvSchema.withHeader()
                    : csvSchema.withoutHeader();
            csvParser.setSchema(csvSchema);


            root = ArrowSchema.createRoot(arrowSchema, arrowAllocator, BATCH_SIZE);
            consumer().onStart(root);

            // Call the parsing function - this may result in a partial parse

            var isComplete = doParse(csvParser, csvSchema, root);

            // If the parse is done, emit the EOS signal and clean up resources
            // Otherwise, wait until a callback on pump()

            if (isComplete) {
                markAsDone();
                consumer().onComplete();
                close();
            }

            return null;
        });
    }

    @Override
    public void onError(Throwable error) {

        try {

            if (log.isTraceEnabled())
                log.trace("CSV DECODER: onError()");

            markAsDone();
            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    @Override
    public void pump() {

        // Don't try to pump if the data hasn't arrived yet, or if it has already gone
        if (csvParser == null || root == null)
            return;

        handleErrors(() -> {

            var isComplete = doParse(csvParser, csvSchema, root);

            // If the parse is done, emit the EOS signal and clean up resources
            // Otherwise, wait until a callback on pump()

            if (isComplete) {
                markAsDone();
                consumer().onComplete();
                close();
            }

            return null;
        });
    }

    boolean doParse(CsvParser parser, CsvSchema csvSchema, VectorSchemaRoot root) throws Exception {

        // CSV codec uses buffering so all the data arrives at once
        // Still, it is probably not helpful to send it all out as fast as the CPU will run!

        // This function checks consumerReady() after each batch is sent
        // If the consumer is not ready, leave the parse and come back to it on the next call to pump()
        // The parser are VSR are left with their state intact, row and col will be zero anyway for a new batch

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

                        // Null strings are encoded with no space between commas (or EOL): some_value,,next_value
                        // An empty string is encoded as "", i.e. token width = 2 (or more with padding)
                        // Using token end - token start, a gap between commas -> empty string instead of null

                        // It would be nicer to check the original bytes to see if there are quote chars in there
                        // But this is not possible with the current Jackson API

                        var tokenStart = parser.currentTokenLocation();
                        var tokenEnd = parser.currentLocation();
                        var tokenWidth = tokenEnd.getColumnNr() - tokenStart.getColumnNr();

                        if (tokenWidth > 1) {
                            JacksonValues.setEmptyString(nullVector, row);
                            col++;
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
                        consumer().onBatch();

                        if (!consumerReady())
                            return false;

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
            consumer().onBatch();
        }

        return true;
    }

    void handleErrors(Callable<Void> parseFunc) {

        try {
            parseFunc.call();
        }
        catch (ETrac e) {

            // Error has already been handled, propagate as-is

            var errorMessage = "CSV decoding failed: " + e.getMessage();

            log.error(errorMessage, e);
            throw e;
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
    }

    @Override
    public void close() {

        try {

            if (root != null) {
                root.close();
                root = null;
            }

            if (csvParser != null) {
                csvParser.close();
                csvParser = null;
            }

            if (buffer != null) {
                buffer.forEach(ArrowBuf::close);
                buffer = null;
            }
        }
        catch (IOException e) {
            throw new ETracInternal("Unexpected error shutting down the CSV parser: " + e.getMessage(), e);
        }
    }
}
