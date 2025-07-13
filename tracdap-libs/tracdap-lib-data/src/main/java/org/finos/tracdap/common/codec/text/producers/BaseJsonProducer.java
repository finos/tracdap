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

import org.apache.arrow.vector.ValueVector;


abstract class BaseJsonProducer<TVector extends ValueVector> implements IJsonProducer<TVector> {

    protected TVector vector;
    protected int currentIndex;

    protected BaseJsonProducer(TVector vector) {
        this.vector = vector;
        this.currentIndex = 0;
    }

    @Override
    public void skipNull() {
        this.currentIndex++;
    }

    @Override
    public void resetIndex(int index) {
        this.currentIndex = index;
    }

    @Override
    public void resetVector(TVector vector) {
        this.vector = vector;
        this.currentIndex = 0;
    }

    @Override
    public TVector getVector() {
        return vector;
    }
}
