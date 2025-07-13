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
import org.finos.tracdap.common.codec.StreamingEncoder;
import org.finos.tracdap.common.data.ArrowVsrContext;
import org.finos.tracdap.common.data.ArrowVsrSchema;
import org.finos.tracdap.common.data.util.ByteOutputStream;
import org.finos.tracdap.common.exception.EUnexpected;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.BiConsumer;


public class BaseTextEncoder extends StreamingEncoder implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final BufferAllocator allocator;
    private final TextFileConfig config;

    private final BiConsumer<JsonGenerator, ArrowVsrContext> generatorSetup;

    private ArrowVsrContext context;
    private OutputStream out;
    private TextFileWriter writer;

    public BaseTextEncoder(BufferAllocator allocator, TextFileConfig config) {
        this(allocator, config, null);
    }

    public BaseTextEncoder(
            BufferAllocator allocator, TextFileConfig config,
            BiConsumer<JsonGenerator, ArrowVsrContext> generatorSetup) {

        this.allocator = allocator;
        this.config = config;
        this.generatorSetup = generatorSetup;
    }

    @Override
    public void onStart(ArrowVsrContext context) {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON ENCODER: onStart()");

            consumer().onStart();

            this.context = context;
            this.out = new ByteOutputStream(allocator, consumer()::onNext);

            this.writer = new TextFileWriter(
                    context.getFrontBuffer(),
                    context.getDictionaries(),
                    this.out,
                    configForSchema(this.config, context.getSchema()));

            // Allow individual codecs to customize the generator
            if (generatorSetup != null)
                generatorSetup.accept(writer.getGenerator(), context);

            writer.writeStart();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);

            close();

            throw new EUnexpected(e);
        }
    }

    private TextFileConfig configForSchema(TextFileConfig baseConfig, ArrowVsrSchema schema) {

        return new TextFileConfig(
                baseConfig.getJsonFactory(),
                baseConfig.getFormatSchema(),
                baseConfig.getBatchSize(),
                schema.isSingleRecord());
    }

    @Override
    public void onBatch() {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON ENCODER: onNext()");

            var batch = context.getFrontBuffer();

            writer.resetBatch(batch);
            writer.writeBatch();

            context.setUnloaded();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);

            close();

            throw new EUnexpected(e);
        }
    }

    @Override
    public void onComplete() {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON ENCODER: onComplete()");

            writer.writeEnd();
            writer.flush();

            markAsDone();
            consumer().onComplete();
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error writing to codec buffer: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
        finally {

            close();
        }
    }

    @Override
    public void onError(Throwable error) {

        try {

            if (log.isTraceEnabled())
                log.trace("JSON ENCODER: onError()");

            markAsDone();
            consumer().onError(error);
        }
        finally {
            close();
        }
    }

    @Override
    public void close() {

        try {

            if (writer != null) {
                writer.close();
                writer = null;
                // Do not close the out stream twice
                out = null;
            }

            if (out != null) {
                out.close();
                out = null;
            }

            // Encoder does not own context, do not close it

            if (context != null) {
                context = null;
            }
        }
        catch (IOException e) {

            // Output stream is writing to memory buffers, IO errors are not expected
            log.error("Unexpected error closing encoder: {}", e.getMessage(), e);
            throw new EUnexpected(e);
        }
    }
}
