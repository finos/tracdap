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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import java.util.List;

abstract class BaseCompositeProducer {

    protected final List<IJsonProducer<?>> delegates;

    public BaseCompositeProducer(List<IJsonProducer<?>> delegates) {
        this.delegates = delegates;
    }

    public void resetIndex(int index) {

        for (var delegate : delegates)
            delegate.resetIndex(index);
    }

    @SuppressWarnings("unchecked")
    public void resetVectors(List<FieldVector> vectors) {

        for (int i = 0; i < vectors.size(); i++) {

            var delegate = (IJsonProducer<FieldVector>) delegates.get(i);
            var vector = vectors.get(i);

            resetDelegateVector(delegate, vector);
        }
    }

    private <TVector extends ValueVector> void resetDelegateVector(IJsonProducer<TVector> delegate, TVector vector) {

        if (vector.getMinorType() != delegate.getVector().getMinorType())
            throw new IllegalArgumentException();

        delegate.resetVector(vector);
    }
}
