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

import com.accenture.trac.common.codec.arrow.ArrowSchema;
import com.accenture.trac.common.codec.arrow.ArrowValues;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


class JsonTableParser extends JsonParserBase {

    private final JsonParser lexer;

    private final Schema arrowSchema;
    private final Map<String, Integer> fieldMap;


    private final VectorSchemaRoot root;
    private VectorUnloader unloader;
    private final Consumer<ArrowRecordBatch> batchEmitter;
    private final int batchSize;

    private int batchRow;
    private int batchCol;

    JsonTableParser(
            Schema arrowSchema, BufferAllocator arrowAllocator,
            JsonParser lexer, boolean isCaseSensitive,
            Consumer<ArrowRecordBatch> batchEmitter, int batchSize) {

        super(lexer);

        this.lexer = lexer;
        this.arrowSchema = arrowSchema;
        this.fieldMap = buildFieldMap(arrowSchema, isCaseSensitive);
        this.root = ArrowSchema.createRoot(arrowSchema, arrowAllocator, batchSize);
        this.unloader = new VectorUnloader(root);

        this.batchEmitter = batchEmitter;
        this.batchSize = batchSize;
    }

    void handlePushArray(ParseState state, ParseState parent, int depth) {

        if (parent.stateType != ParseStateType.ROOT || depth != 1)
            throw new EUnexpected();  // TODO: Nested arrays not allowed
    }

    void handlePopArray(ParseState state, ParseState parent, int depth) {

        dispatchBatch();
    }

    void handlePushObject(ParseState state, ParseState parent, int depth) {

        if (parent.stateType != ParseStateType.ARRAY || depth != 2)
            throw new EUnexpected();  // TODO: Nested objects not allowed

        if (batchRow == 0)
            allocateBatch();
    }

    void handlePopObject(ParseState state, ParseState parent, int depth) {

        checkRequiredFields();

        batchRow++;

        if (batchRow == batchSize)
            dispatchBatch();
    }

    void handleFieldName(ParseState state, ParseState parent, int depth) {

        // should be guaranteed by requirements on arrays and objects
        if (depth != 3)
            throw new EUnexpected();
    }

    void handleFieldValue(ParseState state, int depth) throws IOException {

        var col = fieldMap.get(state.fieldName);
        readField(root, batchRow, col, lexer, lexer.currentToken());
    }

    void handleArrayValue(ParseState state, int depth) {

        throw new EUnexpected();  // TODO: Primitive arrays not allowed
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

    private void readField(VectorSchemaRoot root, int row, int col, JsonParser lexer, JsonToken token) throws IOException {

        var vector = root.getVector(col);
        var fieldType = vector.getField().getFieldType();
        var minorType = vector.getMinorType();

        if (token == JsonToken.VALUE_NULL) {

            if (!fieldType.isNullable())
                throw new EUnexpected();  // todo: data validity error

            else {
                ArrowValues.setValue(root, row, col, null);
                return;
            }
        }

        if (token.isScalarValue()) {

            var value = decodeForMinorType(lexer, minorType);
            ArrowValues.setValue(root, row, col, value);
            return;
        }

        throw new EUnexpected();  // TODO: Error
    }

    private Object decodeForMinorType(JsonParser lexer, Types.MinorType minorType) throws IOException {

        switch (minorType) {

            case BIT: return lexer.getBooleanValue();

            case BIGINT: return lexer.getLongValue();
            case INT: return lexer.getIntValue();
            case SMALLINT: return lexer.getShortValue();
            case TINYINT: return lexer.getByteValue();

            case FLOAT8: return lexer.getDoubleValue();
            case FLOAT4: return lexer.getFloatValue();

            case DECIMAL:
            case DECIMAL256:
                return lexer.getDecimalValue();

            case VARCHAR:
                return lexer.getValueAsString();

            case DATEDAY:
            case DATEMILLI:
                var dateStr = lexer.getValueAsString();
                return LocalDate.parse(dateStr, MetadataCodec.ISO_DATE_FORMAT);

            default:
                throw new EUnexpected();  // todo
        }
    }

    private void allocateBatch() {

        for (var vector : root.getFieldVectors())
            vector.allocateNew();
    }

    private void checkRequiredFields() {

        for (var vector : root.getFieldVectors()) {

            if (!vector.getField().isNullable()) {
                if (vector.isNull(batchRow))
                    throw new EUnexpected();  // TODO: EDataValidity Null value for non-null field
            }
        }
    }

    private void dispatchBatch() {

        root.setRowCount(batchRow);

        var batch = unloader.getRecordBatch();
        batchEmitter.accept(batch);

        root.clear();
    }
}
