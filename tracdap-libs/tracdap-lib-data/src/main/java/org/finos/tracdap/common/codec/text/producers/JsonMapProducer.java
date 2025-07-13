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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.io.IOException;


public class JsonMapProducer extends BaseJsonProducer<MapVector> {

    private VarCharVector keyVector;
    private final IJsonProducer<?> valueDelegate;

    public JsonMapProducer(MapVector vector, VarCharVector keyVector, IJsonProducer<?> valueDelegate) {
        super(vector);
        this.keyVector = keyVector;
        this.valueDelegate = valueDelegate;
    }

    @Override
    public void produceElement(JsonGenerator generator) throws IOException {

        int startOffset = vector.getOffsetBuffer().getInt(currentIndex * (long) Integer.BYTES);
        int endOffset = vector.getOffsetBuffer().getInt((currentIndex + 1) * (long) Integer.BYTES);
        int nItems = endOffset - startOffset;

        generator.writeStartObject();

        for (int i = 0; i < nItems; i++) {

            var key = keyVector.getObject(startOffset + i);
            generator.writeFieldName(key.toString());

            valueDelegate.produceElement(generator);
        }

        generator.writeEndObject();

        currentIndex++;
    }

    // Do not override skipNull(), delegate will not have an entry if the list is null

    @Override
    public void resetIndex(int index) {

        if (index < 0 || index > vector.getValueCount()) {
            throw new IllegalArgumentException("Index out of bounds");
        }

        int delegateOffset = vector.getOffsetBuffer().getInt(index * (long) Integer.BYTES);
        valueDelegate.resetIndex(delegateOffset);

        super.resetIndex(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void resetVector(MapVector vector) {

        var entryVector = (StructVector) vector.getDataVector();
        var keyVector = (VarCharVector) entryVector.getChildrenFromFields().get(0);
        var valueVector = entryVector.getChildrenFromFields().get(1);

        this.keyVector = keyVector;
        ((IJsonProducer<FieldVector>) valueDelegate).resetVector(valueVector);

        super.resetVector(vector);
    }
}
