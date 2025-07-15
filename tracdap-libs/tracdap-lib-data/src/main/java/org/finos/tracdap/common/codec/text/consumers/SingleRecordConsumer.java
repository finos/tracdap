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
    private final VectorSchemaRoot batch;
    private final List<DictionaryStagingConsumer<?>> stagingContainers;

    private boolean gotFirstToken;
    private boolean recordConsumed;

    public SingleRecordConsumer(
            CompositeObjectConsumer recordConsumer,
            VectorSchemaRoot batch,
            List<DictionaryStagingConsumer<?>>stagingContainers) {

        this.recordConsumer = recordConsumer;
        this.batch = batch;
        this.stagingContainers = stagingContainers;
    }

    @Override
    public boolean consumeBatch(JsonParser parser) throws IOException {

        if (!gotFirstToken) {

            if (parser.nextToken() != JsonToken.START_OBJECT)
                throw new EDataCorruption("Unexpected token: " + parser.getCurrentToken());

            gotFirstToken = true;
        }

        if (recordConsumed)
            throw new IllegalStateException("Record has already been consumed");

        recordConsumed = recordConsumer.consumeElement(parser);

        if (!recordConsumed)
            return false;

        var nextToken = parser.nextToken();

        if (nextToken != null && nextToken != JsonToken.NOT_AVAILABLE)
            throw new EDataCorruption("Unexpected token: " + nextToken);

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
        return recordConsumed;
    }

    @Override
    public void resetBatch(VectorSchemaRoot batch) {

        recordConsumer.resetVectors(batch.getFieldVectors());
    }
}
