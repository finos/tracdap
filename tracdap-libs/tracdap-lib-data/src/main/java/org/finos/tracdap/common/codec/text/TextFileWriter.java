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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.finos.tracdap.common.codec.text.producers.IBatchProducer;

import java.io.IOException;
import java.io.OutputStream;

public class TextFileWriter {

    private VectorSchemaRoot root;
    private DictionaryProvider dictionaries;

    private final JsonGenerator generator;
    private final IBatchProducer producer;


    public TextFileWriter(
            VectorSchemaRoot root,
            DictionaryProvider dictionaries,
            OutputStream out,
            TextFileConfig config) throws IOException {

        this(root, dictionaries, config.getJsonFactory().createGenerator(out), config);
    }

    public TextFileWriter(
            VectorSchemaRoot root,
            DictionaryProvider dictionaries,
            JsonGenerator generator,
            TextFileConfig config) {

        this.root = root;
        this.dictionaries = dictionaries;

        this.generator = generator;
        this.generator.useDefaultPrettyPrinter();

        if (config.hasFormatSchema())
            this.generator.setSchema(config.getFormatSchema());

        this.producer = TextFileUtils.createBatchProducer(
                this.root, this.dictionaries,
                config.isSingleRecord());
    }

    public JsonGenerator getGenerator() {
        return generator;
    }

    public void resetBatch(VectorSchemaRoot batch) throws IOException {

        this.root = batch;

        producer.resetBatch(root);
    }

    public void writeStart() throws IOException {

        producer.produceStart(generator);
    }

    public void writeBatch() throws IOException {

        producer.resetIndex(0);
        producer.produceBatch(generator);
    }

    public void writeEnd() throws IOException {

        producer.produceEnd(generator);
    }

    public void flush() throws IOException {

        generator.flush();
    }

    public void close() throws IOException {

        generator.close();
    }
}
