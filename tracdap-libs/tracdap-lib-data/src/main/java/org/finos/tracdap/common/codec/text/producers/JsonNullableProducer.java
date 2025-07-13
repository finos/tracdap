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
import org.apache.arrow.vector.ValueVector;

import java.io.IOException;

public class JsonNullableProducer<TVector extends ValueVector> extends BaseJsonProducer<TVector> {

    private final IJsonProducer<TVector> delegate;

    public JsonNullableProducer(IJsonProducer<TVector> delegate) {
        super(delegate.getVector());
        this.delegate = delegate;
    }

    @Override
    public void produceElement(JsonGenerator generator) throws IOException {

        if (vector.isNull(currentIndex)) {
            generator.writeNull();
            delegate.skipNull();
        }
        else
            delegate.produceElement(generator);

        currentIndex++;
    }

    @Override
    public void skipNull() {
        // Can be called by containers of nullable types
        delegate.skipNull();
        currentIndex++;
    }

    @Override
    public void resetIndex(int index) {

        if (index < 0 || index > vector.getValueCount()) {
            throw new IllegalArgumentException("Index out of bounds");
        }

        delegate.resetIndex(index);
        super.resetIndex(index);
    }

    @Override
    public void resetVector(TVector vector) {
        delegate.resetVector(vector);
        super.resetVector(vector);
    }
}
