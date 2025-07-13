/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.codec.text.consumers;

import org.finos.tracdap.common.exception.EDataCorruption;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.List;


public class BatchConsumer implements IBatchConsumer {

    private final CompositeObjectConsumer recordConsumer;
    private final int batchSize;

    private VectorSchemaRoot batch;
    private List<DictionaryStagingConsumer<?>> staging;

    private int currentIndex;
    private boolean midRecord;
    private boolean gotFirstToken;
    private boolean gotLastToken;
    private boolean parseComplete;

    public BatchConsumer(
            CompositeObjectConsumer recordConsumer,
            VectorSchemaRoot batch,
            List<DictionaryStagingConsumer<?>> staging,
            int batchSize) {

        this.recordConsumer = recordConsumer;
        this.batchSize = batchSize;

        this.batch = batch;
        this.staging = staging;

        this.currentIndex = 0;
        this.midRecord = false;
        this.gotFirstToken = false;
        this.gotLastToken = false;
        this.parseComplete = false;
    }

    @Override
    public boolean consumeBatch(JsonParser parser) throws IOException {

        if (!gotFirstToken) {
            if (parser.nextToken() == JsonToken.START_ARRAY)
                parser.nextToken();
            gotFirstToken = true;
        }

        if (currentIndex >= batchSize) {
            throw new IllegalStateException("Previous batch has not been reset");
        }

        for (var token = parser.currentToken(); token != null && token != JsonToken.NOT_AVAILABLE; token = parser.nextToken()) {

            if (gotLastToken)
                throw new EDataCorruption("Unexpected token: " + token);

            if (midRecord || token.isStructStart()) {
                if (currentIndex == batchSize) {
                    break;
                }
                else if (recordConsumer.consumeElement(parser)) {
                    currentIndex++;
                    midRecord = false;
                    continue;
                }
                else {
                    midRecord = true;
                    return false;
                }
            }

            if (token == JsonToken.END_ARRAY) {
                gotLastToken = true;
                continue;
            }

            throw new EDataCorruption("Unexpected token: " + token);
        }

        if (gotLastToken || (parser.nextToken() != JsonToken.START_OBJECT && parser.nextToken() != JsonToken.NOT_AVAILABLE)) {
            parseComplete = true;
        }

        if (parseComplete || currentIndex == batchSize) {

            if (staging != null) {
                for (var staged : staging) {
                    staged.encodeVector();
                }
            }

            batch.setRowCount(currentIndex);

            return true;
        }
        else {

            return false;
        }
    }

    @Override
    public boolean endOfStream() {
        return parseComplete;
    }

    @Override
    public void resetBatch(VectorSchemaRoot batch) {

        recordConsumer.resetVectors(batch.getFieldVectors());

        this.batch = batch;
        this.currentIndex = 0;
    }
}
