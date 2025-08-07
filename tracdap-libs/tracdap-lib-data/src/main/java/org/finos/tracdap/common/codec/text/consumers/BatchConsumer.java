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
    private final List<DictionaryStagingConsumer<?>> staging;
    private final int batchSize;
    private VectorSchemaRoot batch;

    private JsonToken token;
    private boolean active;
    private boolean delegateActive;
    private int currentIndex;
    private boolean gotBatch;
    private boolean gotLastToken;

    public BatchConsumer(
            CompositeObjectConsumer recordConsumer,
            List<DictionaryStagingConsumer<?>> staging,
            VectorSchemaRoot batch, int batchSize) {

        this.recordConsumer = recordConsumer;
        this.staging = staging;
        this.batchSize = batchSize;
        this.batch = batch;

        this.active =false;
        this.delegateActive = false;
        this.currentIndex = 0;
        this.gotBatch = false;
        this.gotLastToken = false;
    }

    @Override
    public boolean consumeBatch(JsonParser parser) throws IOException {

        if (currentIndex >= batchSize) {
            throw new IllegalStateException("Previous batch has not been reset");
        }

        if (!active) {

            token = parser.nextToken();
            active = true;

            // Outer start array token is present in some formats (JSON) and not others (CSV)
            if (token == JsonToken.START_ARRAY)
                token = parser.nextToken();
        }
        else if (!delegateActive) {
            token = parser.nextToken();
        }

        if (gotLastToken && (token == null || token == JsonToken.NOT_AVAILABLE)) {
            active = false;
            return false;
        }

        while (token != null && token !=  JsonToken.NOT_AVAILABLE) {

            if (token.isStructStart() || delegateActive) {

                if (recordConsumer.consumeElement(parser)) {
                    delegateActive = false;
                    currentIndex++;
                }
                else {
                    delegateActive = true;
                    return false;
                }

                if (currentIndex == batchSize) {
                    gotBatch = true;
                    break;
                }
            }
            else if (token == JsonToken.END_ARRAY) {
                gotLastToken = true;
                break;
            }
            else {
                throw new EDataCorruption("Unexpected token: " + token);
            }

            token = parser.nextToken();
        }

        // Outer start array token is present in some formats (JSON) and not others (CSV)
        // Since CSV is synchronous only, token == null => end of stream
        if (token == null && !delegateActive) {
            gotLastToken = true;
        }

        if (gotBatch || gotLastToken) {

            if (staging != null) {
                for (var staged : staging) {
                    staged.encodeVector();
                }
            }

            batch.setRowCount(currentIndex);

            gotBatch = false;

            return true;
        }
        else {

            return false;
        }
    }

    @Override
    public boolean endOfStream() {
        return gotLastToken && !active;
    }

    @Override
    public void resetBatch(VectorSchemaRoot batch) {

        if (delegateActive)
            throw new IllegalStateException("JSON consumer reset mid-value");

        recordConsumer.resetVectors(batch.getFieldVectors());

        this.batch = batch;
        this.currentIndex = 0;
    }
}
