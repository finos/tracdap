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
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.ValueVector;

import java.io.IOException;

public class JsonDictionaryProducer extends BaseJsonProducer<BaseIntVector> {

    private final IJsonProducer<? extends ValueVector> delegate;

    public JsonDictionaryProducer(BaseIntVector vector, IJsonProducer<? extends ValueVector> delegate) {
        super(vector);
        this.delegate = delegate;
    }

    @Override
    public void produceElement(JsonGenerator generator) throws IOException {

        int dicIndex = (int) vector.getValueAsLong(currentIndex++);
        delegate.resetIndex(dicIndex);
        delegate.produceElement(generator);
    }
}
