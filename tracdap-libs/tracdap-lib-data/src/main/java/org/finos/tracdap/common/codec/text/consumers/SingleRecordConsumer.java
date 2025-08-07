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


public class SingleRecordConsumer implements IBatchConsumer {

    private final CompositeObjectConsumer recordConsumer;
    private final List<DictionaryStagingConsumer<?>> stagingContainers;
    private VectorSchemaRoot batch;

    private JsonToken token;
    private boolean active;
    private boolean delegateActive;
    private boolean gotRecord;

    public SingleRecordConsumer(
            CompositeObjectConsumer recordConsumer,
            List<DictionaryStagingConsumer<?>>stagingContainers,
            VectorSchemaRoot batch) {

        this.recordConsumer = recordConsumer;
        this.stagingContainers = stagingContainers;
        this.batch = batch;

        token = null;
        active = false;
        delegateActive = false;
        gotRecord = false;
    }

    @Override
    public boolean consumeBatch(JsonParser parser) throws IOException {

        if (!active) {

            token = parser.nextToken();
            active = true;

            if (token != JsonToken.START_OBJECT)
                throw new EDataCorruption("Unexpected token: " + parser.getCurrentToken());
        }
        else if (!delegateActive) {

            token = parser.nextToken();
        }

        if (token == null || token == JsonToken.NOT_AVAILABLE) {
            if (gotRecord) {
                active = false;
            }
            return false;
        }

        if (recordConsumer.consumeElement(parser)) {
            delegateActive = false;
            gotRecord = true;
        }
        else {
            delegateActive = true;
            return false;
        }

        if (stagingContainers != null) {
            for (var container : stagingContainers) {
                container.encodeVector();
            }
        }

        batch.setRowCount(1);

        return true;
    }

    @Override
    public boolean endOfStream() {
        return gotRecord && !active;
    }

    @Override
    public void resetBatch(VectorSchemaRoot batch) {

        if (delegateActive)
            throw new IllegalStateException("JSON consumer reset mid-value");

        recordConsumer.resetVectors(batch.getFieldVectors());

        this.batch = batch;
    }
}
