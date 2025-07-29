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

import com.fasterxml.jackson.core.JsonParser;
import org.apache.arrow.algorithm.dictionary.DictionaryBuilder;
import org.apache.arrow.algorithm.dictionary.DictionaryEncoder;
import org.apache.arrow.algorithm.dictionary.HashTableBasedDictionaryBuilder;
import org.apache.arrow.algorithm.dictionary.HashTableDictionaryEncoder;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.ElementAddressableVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.finos.tracdap.common.exception.EDataConstraint;

import java.io.IOException;


public class DictionaryStagingConsumer<TStaging extends ElementAddressableVector>
        extends BaseJsonConsumer<BaseIntVector> {
    
    private final IJsonConsumer<TStaging> delegate;

    private final Dictionary dictionary;
    private DictionaryEncoder<BaseIntVector, TStaging> encoder;
    private final DictionaryBuilder<TStaging> builder;

    public DictionaryStagingConsumer(
            BaseIntVector vector,
            IJsonConsumer<TStaging> delegate,
            Dictionary dictionary,
            boolean dynamic) {

        super(vector);
        this.delegate = delegate;
        this.dictionary = dictionary;

        @SuppressWarnings("unchecked")
        var dictionaryVector = (TStaging) dictionary.getVector();
        this.encoder = new HashTableDictionaryEncoder<>(dictionaryVector);
        this.builder = dynamic ? new HashTableBasedDictionaryBuilder<>(dictionaryVector, false) : null;
    }

    @Override
    public boolean consumeElement(JsonParser parser) throws IOException {
        if (delegate.consumeElement(parser)) {
            currentIndex++;
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void setNull() {
        delegate.setNull();
        currentIndex++;
    }

    @Override
    public void resetVector(BaseIntVector vector) {

        TStaging stagingVector = delegate.getVector();
        if (stagingVector.getValueCapacity() < vector.getValueCapacity()) {
            stagingVector.setInitialCapacity(vector.getValueCapacity());
        }

        stagingVector.reset();
        delegate.resetVector(stagingVector);

        super.resetVector(vector);
    }

    public void encodeVector() {

        var stagingVector = delegate.getVector();
        stagingVector.setValueCount(currentIndex);

        if (builder != null) {
            int newEntries = builder.addValues(stagingVector);
            if (newEntries > 0) {
                encoder = new HashTableDictionaryEncoder<>(builder.getDictionary());
            }
        }

        try {
            encoder.encode(delegate.getVector(), vector);
        }
        catch (IllegalArgumentException e) {
            var fieldName = vector.getField().getName();
            var message = String.format("Enum field [%s] contains values that are not in the enum", fieldName);
            throw new EDataConstraint(message);
        }
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    public TStaging getStagingVector() {
        return delegate.getVector();
    }
}
