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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

import java.io.IOException;
import java.util.List;


public class JsonStructProducer extends BaseJsonProducer<StructVector> {

    private final List<String> fieldNames;
    private final List<IJsonProducer<?>> delegates;

    public JsonStructProducer(StructVector vector, List<IJsonProducer<?>> delegates) {
        super(vector);
        this.fieldNames = vector.getChildFieldNames();
        this.delegates = delegates;
    }

    @Override
    public void produceElement(JsonGenerator generator) throws IOException {

        generator.writeStartObject();

        for (int i = 0; i < fieldNames.size(); i++) {

            generator.writeFieldName(fieldNames.get(i));
            delegates.get(i).produceElement(generator);
        }

        generator.writeEndObject();
    }

    @Override
    public void resetIndex(int index) {

        for (IJsonProducer<?> delegate : delegates) {
            delegate.resetIndex(index);
        }

        super.resetIndex(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void resetVector(StructVector vector) {

        for (int i = 0; i < delegates.size(); i++) {

            var delegate = (IJsonProducer<FieldVector>) delegates.get(i);
            var child = vector.getChildrenFromFields().get(i);

            resetDelegateVector(delegate, child);
        }

        super.resetVector(vector);
    }

    private <TVector extends ValueVector> void resetDelegateVector(IJsonProducer<TVector> delegate, TVector vector) {

        if (vector.getMinorType() != delegate.getVector().getMinorType())
            throw new IllegalArgumentException();

        delegate.resetVector(vector);
    }
}
