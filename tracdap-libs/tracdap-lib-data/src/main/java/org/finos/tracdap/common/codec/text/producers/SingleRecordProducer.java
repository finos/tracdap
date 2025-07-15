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

package org.finos.tracdap.common.codec.text.producers;

import org.finos.tracdap.common.exception.ETracInternal;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;


public class SingleRecordProducer implements IBatchProducer {

    private final CompositeObjectProducer recordProducer;
    private int currentBatchSize;
    private boolean batchProduced;

    public SingleRecordProducer(CompositeObjectProducer recordProducer) {
        this.recordProducer = recordProducer;
    }

    @Override
    public void produceStart(JsonGenerator generator) throws IOException {

        // No op
    }

    @Override
    public void produceBatch(JsonGenerator generator) throws IOException {

        if (batchProduced)
            throw new ETracInternal("Batch already produced");

        if (currentBatchSize != 1)
            throw new ETracInternal("Batch size must be 1");

        recordProducer.produceElement(generator);
        batchProduced = true;
    }

    @Override
    public void produceEnd(JsonGenerator generator) throws IOException {

        // No op
    }

    @Override
    public void resetIndex(int index) {

        recordProducer.resetIndex(index);
    }

    @Override
    public void resetBatch(VectorSchemaRoot batch) throws IOException {

        recordProducer.resetVectors(batch.getFieldVectors());
        currentBatchSize = batch.getRowCount();
    }
}
