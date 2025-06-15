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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.stream.Collectors;


/// A working context for Arrow data being processed through the VSR framework in a data pipeline
public class ArrowVsrContext {

    private final ArrowVsrSchema schema;

    private final BufferAllocator allocator;
    private final VectorSchemaRoot front;
    private final VectorSchemaRoot back;

    private final DictionaryProvider dictionaries;
    private final FieldVector[] staging;
    private final DictionaryBuilder<ElementAddressableVector>[] builders;
    private final DictionaryEncoder<BaseIntVector, ElementAddressableVector>[] encoders;

    private final boolean ownership;

    private boolean backBufferLoaded;
    private boolean frontBufferAvailable;
    private boolean frontBufferUnloaded;

    public static ArrowVsrContext forSource(VectorSchemaRoot source, DictionaryProvider dictionaries, BufferAllocator allocator) {

        // Do not take ownership of external sources by default
        return new ArrowVsrContext(source, dictionaries, allocator, false);
    }

    public static ArrowVsrContext forSource(VectorSchemaRoot source, DictionaryProvider dictionaries, BufferAllocator allocator, boolean takeOwnership) {

        return new ArrowVsrContext(source, dictionaries, allocator, takeOwnership);
    }

    private ArrowVsrContext(VectorSchemaRoot source, DictionaryProvider dictionaries, BufferAllocator allocator, boolean takeOwnership) {

        this.schema = inferFullSchema(source.getSchema(), dictionaries);

        this.allocator = allocator;
        this.back = source;
        this.front = back;  // No double buffering yet

        this.dictionaries = dictionaries;
        this.staging = null;
        this.builders = null;
        this.encoders = null;

        this.ownership = takeOwnership;
    }

    public static ArrowVsrContext forSchema(ArrowVsrSchema schema, BufferAllocator allocator) {

        return new ArrowVsrContext(schema, allocator);
    }

    @SuppressWarnings("unchecked")
    private ArrowVsrContext(ArrowVsrSchema schema, BufferAllocator allocator) {

        this.schema = schema;
        this.allocator = allocator;

        var fields = schema.physical().getFields();
        var vectors = fields.stream().map(f -> f.createVector(allocator)).collect(Collectors.toList());

        this.back = new VectorSchemaRoot(vectors);
        this.back.allocateNew();
        this.front = back;  // No double buffering yet

        // Set up dictionary encoding (no a-priori knowledge)
        this.staging = new FieldVector[vectors.size()];
        this.builders = new DictionaryBuilder[vectors.size()];
        this.encoders = new DictionaryEncoder[vectors.size()];
        this.dictionaries =  prepareDictionaries();

        // Always take ownership if the VSR has been constructed internally
        this.ownership = true;
    }

    private ArrowVsrSchema inferFullSchema(Schema primarySchema, DictionaryProvider dictionaries) {

        var concreteFields = new ArrayList<Field>(primarySchema.getFields().size());

        for (var field : primarySchema.getFields()) {
            if (field.getDictionary() != null) {

                var dictionaryId = field.getDictionary().getId();
                var dictionary = dictionaries.lookup(dictionaryId);
                var dictionaryField = dictionary.getVector().getField();

                // Concrete field has the name of the primary field and type of the dictionary field
                // Arrow gives dictionary fields internal names, e.g. DICT0
                var concreteField = new Field(
                        field.getName(),
                        dictionaryField.getFieldType(),
                        dictionaryField.getChildren());

                concreteFields.add(concreteField);
            }
            else {
                concreteFields.add(field);
            }
        }

        return new ArrowVsrSchema(primarySchema, new Schema(concreteFields));
    }

    private DictionaryProvider prepareDictionaries() {

        var fields = schema.physical().getFields();
        var concreteFields = schema.decoded().getFields();

        var dictionaries = new DictionaryProvider.MapDictionaryProvider();

        for (int i = 0; i < fields.size(); ++i) {

            var field = fields.get(i);

            if (field.getDictionary() != null) {

                var concreteField = concreteFields.get(i);
                var encoding = field.getDictionary();

                var stagingVector = concreteField.createVector(allocator);
                var dictionaryVector = concreteField.createVector(allocator);
                var dictionary = new Dictionary(dictionaryVector, encoding);
                var builder = new HashTableBasedDictionaryBuilder<>((ElementAddressableVector) dictionaryVector);
                var encoder = new HashTableDictionaryEncoder<>((ElementAddressableVector) dictionaryVector);

                staging[i] = stagingVector;
                builders[i] = builder;
                encoders[i] = encoder;

                dictionaryVector.allocateNew();

                dictionaries.put(dictionary);
            }
        }

        return dictionaries;
    }

    public ArrowVsrSchema getSchema() {
        return schema;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public FieldVector getStagingVector(int col) {

        if (staging != null && staging[col] != null)
            return staging[col];
        else
            return back.getVector(col);
    }

    public VectorSchemaRoot getBackBuffer() {
        return back;
    }

    public VectorSchemaRoot getFrontBuffer() {
        return front;
    }

    public DictionaryProvider getDictionaries() {
        return dictionaries;
    }

    public boolean readyToLoad() {
        return ! backBufferLoaded;
    }

    public void setRowCount(int nRows) {

        back.setRowCount(nRows);

        if (staging != null) {

            for (var vector : staging)
                if (vector != null)
                    vector.setValueCount(nRows);
        }
    }

    public void encodeDictionaries() {

        for (int i = 0, n = back.getFieldVectors().size(); i < n; i++) {

            if (staging[i] == null)
                continue;

            var staged = staging[i];
            var target = back.getVector(i);

            var builder = builders[i];
            var nValues = builder.getDictionary().getValueCount();

            builder.addValues((ElementAddressableVector) staged);

            if (builder.getDictionary().getValueCount() > nValues) {
                var encoder = new HashTableDictionaryEncoder<>(builder.getDictionary());
                encoders[i] = encoder;
            }

            var encoder = encoders[i];
            encoder.encode((ElementAddressableVector) staged, (BaseIntVector) target);
        }
    }

    public void setLoaded() {
        backBufferLoaded = true;
    }

    public boolean readyToFlip() {
        return backBufferLoaded && (frontBufferUnloaded || !frontBufferAvailable);
    }

    public void flip() {
        frontBufferAvailable = true;
        frontBufferUnloaded = false;
    }

    public boolean readyToUnload() {
        return frontBufferAvailable && !frontBufferUnloaded;
    }

    public void setUnloaded() {
        frontBufferUnloaded = true;
        frontBufferAvailable = false;
        backBufferLoaded = false;
    }

    public void close() {

        if (ownership) {

            back.close();

            if (staging != null) {
                for (var vector : staging) {
                    if (vector != null)
                        vector.close();
                }
            }

            if (dictionaries != null) {
                for (var dictionaryId : dictionaries.getDictionaryIds()) {
                    var dictionary = dictionaries.lookup(dictionaryId);
                    dictionary.getVector().close();
                }
            }
        }
    }
}
