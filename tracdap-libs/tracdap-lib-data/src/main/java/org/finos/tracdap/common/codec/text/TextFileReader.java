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

package org.finos.tracdap.common.codec.text;

import org.apache.arrow.vector.ValueVector;
import org.finos.tracdap.common.codec.text.consumers.DictionaryStagingConsumer;
import org.finos.tracdap.common.codec.text.consumers.IBatchConsumer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.async.ByteBufferFeeder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TextFileReader implements DictionaryProvider {

    private final Schema schema;
    private final VectorSchemaRoot root;

    private final JsonParser parser;
    private final ByteBufferFeeder feeder;
    private final IBatchConsumer consumer;

    private final DictionaryProvider dictionaries;
    private final List<ValueVector> stagingVectors;

    public TextFileReader(
            Schema schema,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider dictionaries,
            BufferAllocator allocator,
            InputStream in,
            TextFileConfig config) throws IOException {

        this(schema, dictionaryFields, dictionaries, allocator,
                config.getJsonFactory().createParser(in),
                config);
    }

    public TextFileReader(
            Schema schema,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider prebuiltDictionaries,
            BufferAllocator allocator,
            TextFileConfig config) throws IOException {

        this(schema, dictionaryFields, prebuiltDictionaries, allocator,
                config.getJsonFactory().createNonBlockingByteBufferParser(),
                config);
    }

    public TextFileReader(
            Schema schema,
            Map<Long, Field> dictionaryFields,
            DictionaryProvider prebuiltDictionaries,
            BufferAllocator allocator,
            JsonParser parser,
            TextFileConfig config) {

        this.schema = schema;
        this.root = buildRoot(schema, allocator, config);

        this.parser = parser;
        this.feeder = (ByteBufferFeeder) parser.getNonBlockingInputFeeder();

        if (config.hasFormatSchema())
            this.parser.setSchema(config.getFormatSchema());

        var stagingFields = new ArrayList<DictionaryStagingConsumer<?>>(dictionaryFields.size());

        this.consumer = TextFileUtils.createBatchConsumer(
                this.root, dictionaryFields, prebuiltDictionaries,
                stagingFields, config);

        var dictionaries = new DictionaryProvider.MapDictionaryProvider();
        var stagingVectors = new ArrayList<ValueVector>(stagingFields.size());

        if (prebuiltDictionaries != null) {
            for (var dictionaryId : prebuiltDictionaries.getDictionaryIds()) {
                dictionaries.put(prebuiltDictionaries.lookup(dictionaryId));
            }
        }

        for (var stagingField : stagingFields) {
            dictionaries.put(stagingField.getDictionary());
            stagingVectors.add(stagingField.getStagingVector());
        }

        this.dictionaries = dictionaries;
        this.stagingVectors = stagingVectors;
    }

    private VectorSchemaRoot buildRoot(Schema schema, BufferAllocator allocator,  TextFileConfig config) {

        var vectors = new ArrayList<FieldVector>(schema.getFields().size());

        for (var field : schema.getFields()) {
            var vector = field.createVector(allocator);
            vector.allocateNew();
            vector.setInitialCapacity(config.getBatchSize());
            vectors.add(vector);
        }

        return new VectorSchemaRoot(schema, vectors, config.getBatchSize());
    }

    public JsonParser getParser() {
        return parser;
    }

    public Schema getSchema() {
        return schema;
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return root;
    }

    @Override
    public Set<Long> getDictionaryIds() {
        return dictionaries.getDictionaryIds();
    }

    @Override
    public Dictionary lookup(long id) {
        return dictionaries.lookup(id);
    }

    public void feedInput(ByteBuffer buffer) throws IOException {

        if (feeder == null)
            throw new IllegalStateException("Cannot feed more input, file reader is in blocking mode");

        if (!feeder.needMoreInput()) {
            throw new IllegalStateException("Cannot feed more input, existing data has not been consumed");
        }

        feeder.feedInput(buffer);
    }

    public void feedInput(byte[] buffer) throws IOException {

        if (feeder == null)
            throw new IllegalStateException("Cannot feed more input, file reader is in blocking mode");

        if (!feeder.needMoreInput())
            throw new IllegalStateException("Cannot feed more input, existing data has not been consumed");

        feeder.feedInput(ByteBuffer.wrap(buffer));
    }

    public boolean readBatch() throws IOException {

        return consumer.consumeBatch(parser);
    }

    public void resetBatch(VectorSchemaRoot batch) throws IOException {

        consumer.resetBatch(batch);
    }

    public boolean endOfStream() {

        return consumer.endOfStream();
    }

    public void close() throws IOException {

        parser.close();

        root.close();

        for (var dictionaryId : dictionaries.getDictionaryIds()) {
            var dictionary = dictionaries.lookup(dictionaryId);
            dictionary.getVector().close();
        }

        stagingVectors.forEach(ValueVector::close);
    }
}
