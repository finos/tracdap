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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


class JsonTableHandler implements JsonStreamParser.Handler {

    private final VectorSchemaRoot root;
    private final Consumer<Void> batchSignal;

    // Batch signal notifies the pipeline that a batch is ready to be processed
    // It is processed synchronously, after which the batch is available to be re-used
    // This is not the same thing as a Bat Signal, which summons the Bat Man
    // That's a totally different kind of signal and the two should not be confused

    private final int batchSize;
    private final Map<String, Integer> fieldMap;

    private int batchRow;

    JsonTableHandler(
            VectorSchemaRoot root, Consumer<Void> batchSignal,
            int batchSize, boolean isCaseSensitive) {

        this.root = root;
        this.batchSignal = batchSignal;

        this.batchSize = batchSize;
        this.fieldMap = buildFieldMap(root.getSchema(), isCaseSensitive);
    }

    @Override
    public void handlePushArray(JsonParser lexer, JsonStreamParser.ParseState state, JsonStreamParser.ParseState parent, int depth) throws IOException {

        if (parent.stateType != JsonStreamParser.ParseStateType.ROOT || depth != 1) {

            var msg = "Invalid JSON table: Nested arrays are not supported";
            throw new JsonParseException(lexer, msg, lexer.currentLocation());
        }
    }

    @Override
    public void handlePopArray(JsonParser lexer, JsonStreamParser.ParseState state, JsonStreamParser.ParseState parent, int depth) {

        dispatchBatch();
    }

    @Override
    public void handlePushObject(JsonParser lexer, JsonStreamParser.ParseState state, JsonStreamParser.ParseState parent, int depth) throws IOException {

        if (parent.stateType != JsonStreamParser.ParseStateType.ARRAY || depth != 2) {

            var msg = depth > 2
                ? "Invalid JSON table: Nested tables are not supported"
                : "Invalid JSON table: Root item must be an array";

            throw new JsonParseException(lexer, msg, lexer.currentLocation());
        }
    }

    @Override
    public void handlePopObject(JsonParser lexer, JsonStreamParser.ParseState state, JsonStreamParser.ParseState parent, int depth) throws IOException {

        checkRequiredFields(lexer);

        batchRow++;

        if (batchRow == batchSize) {
            dispatchBatch();
            batchRow = 0;
        }
    }

    @Override
    public void handleFieldName(JsonParser lexer, JsonStreamParser.ParseState state, JsonStreamParser.ParseState parent, int depth) {

        // No-op, wait for value
    }

    @Override
    public void handleFieldValue(JsonParser lexer, JsonStreamParser.ParseState state, int depth) throws IOException {

        var col = fieldMap.get(state.fieldName);
        var vector = root.getVector(col);

        JacksonValues.parseAndSet(vector, batchRow, lexer, lexer.currentToken());
    }

    @Override
    public void handleArrayValue(JsonParser lexer, JsonStreamParser.ParseState state, int depth) throws IOException {

        var msg = "Invalid JSON table: Root array must contain objects, not individual values";
        throw new JsonParseException(lexer, msg, lexer.currentLocation());
    }

    @Override
    public void close() {

        root.close();
    }

    private static Map<String, Integer> buildFieldMap(Schema arrowSchema, boolean isCaseSensitive) {

        var casedMap = IntStream.range(0, arrowSchema.getFields().size())
                .boxed().collect(Collectors.toMap(
                i -> arrowSchema.getFields().get(i).getName(),
                i -> i));

        if (isCaseSensitive)
            return casedMap;

        else {

            var uncasedMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
            uncasedMap.putAll(casedMap);

            return uncasedMap;
        }
    }

    private void checkRequiredFields(JsonParser lexer) throws IOException {

        for (var vector : root.getFieldVectors()) {
            if (!vector.getField().isNullable()) {

                if (vector.isNull(batchRow)) {
                    var msg = String.format("Invalid JSON table: Missing required field [%s]", vector.getName());
                    throw new JsonParseException(lexer, msg, lexer.currentLocation());
                }
            }
        }
    }

    private void dispatchBatch() {

        root.setRowCount(batchRow);
        batchSignal.accept(null);
    }
}
