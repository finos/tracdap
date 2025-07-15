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

package org.finos.tracdap.common.data;

import org.apache.arrow.algorithm.dictionary.DictionaryBuilder;
import org.apache.arrow.algorithm.dictionary.DictionaryEncoder;
import org.apache.arrow.algorithm.dictionary.HashTableBasedDictionaryBuilder;
import org.apache.arrow.algorithm.dictionary.HashTableDictionaryEncoder;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.ElementAddressableVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;


public class ArrowVsrStaging<TStaging extends ElementAddressableVector> {

    private final TStaging stagingVector;
    private BaseIntVector targetVector;

    private final Dictionary dictionary;
    private final DictionaryBuilder<TStaging> builder;
    private DictionaryEncoder<BaseIntVector, TStaging> encoder;

    public ArrowVsrStaging(
            TStaging stagingVector,
            BaseIntVector targetVector,
            Dictionary dictionary) {

        this.stagingVector = stagingVector;
        this.targetVector = targetVector;
        this.dictionary = dictionary;

        @SuppressWarnings("unchecked")
        var dictionaryVector = (TStaging) dictionary.getVector();

        this.builder = null;
        this.encoder = new HashTableDictionaryEncoder<>(dictionaryVector);
    }

    public ArrowVsrStaging(
            TStaging stagingVector,
            BaseIntVector targetVector) {

        this.stagingVector = stagingVector;
        this.targetVector = targetVector;

        @SuppressWarnings("unchecked")
        var dictionaryVector = (TStaging) stagingVector.getField().createVector(targetVector.getAllocator());
        dictionaryVector.allocateNew();

        var encoding = targetVector.getField().getDictionary();

        this.dictionary = new Dictionary((FieldVector) dictionaryVector, encoding);
        this.builder = new HashTableBasedDictionaryBuilder<>(dictionaryVector);
        this.encoder = new HashTableDictionaryEncoder<>(dictionaryVector);
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    public TStaging getStagingVector() {
        return stagingVector;
    }

    public BaseIntVector getTargetVector() {
        return targetVector;
    }

    public void setTargetVector(BaseIntVector targetVector) {
        this.targetVector = targetVector;
    }

    public void encodeVector() {

        if (builder != null) {
            if (builder.addValues(stagingVector) > 0) {
                encoder = new HashTableDictionaryEncoder<>(builder.getDictionary());
            }
        }

        encoder.encode(stagingVector, targetVector);
    }

    public void close() {

        this.stagingVector.close();
        this.dictionary.getVector().close();
    }
}
