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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.finos.tracdap.common.exception.ETracInternal;

import java.util.*;


/// A working context for Arrow data being processed through the VSR framework in a data pipeline
public class ArrowVsrContext implements AutoCloseable {

    private final ArrowVsrSchema schema;

    private final BufferAllocator allocator;
    private final VectorSchemaRoot vsr;
    private final DictionaryProvider dictionaries;

    private final boolean vsrOwnership;
    private final boolean dictionariesOwnership;
    private final AutoCloseable closeableSource;

    private boolean loaded;

    public static ArrowVsrContext forSource(VectorSchemaRoot source, DictionaryProvider dictionaries, BufferAllocator allocator) {

        // Do not take ownership of external sources by default
        return new ArrowVsrContext(source, false, dictionaries, false, allocator, null);
    }

    public static ArrowVsrContext forSource(
            VectorSchemaRoot source, boolean sourceOwnership,
            DictionaryProvider dictionaries, boolean dictionariesOwnership,
            BufferAllocator allocator) {

        return new ArrowVsrContext(source, sourceOwnership, dictionaries, dictionariesOwnership, allocator, null);
    }

    public static ArrowVsrContext forSource(
            VectorSchemaRoot source, DictionaryProvider dictionaries,
            BufferAllocator allocator, AutoCloseable closeableSource) {

        return new ArrowVsrContext(source, false, dictionaries, false, allocator, closeableSource);
    }

    private ArrowVsrContext(
            VectorSchemaRoot source, boolean sourceOwnership,
            DictionaryProvider dictionaries, boolean dictionariesOwnership,
            BufferAllocator allocator, AutoCloseable closeableSource) {

        this.schema = new ArrowVsrSchema(source.getSchema(), dictionaries);

        this.allocator = allocator;
        this.vsr = source;
        this.dictionaries = dictionaries;

        this.vsrOwnership = sourceOwnership;
        this.dictionariesOwnership = dictionariesOwnership;
        this.closeableSource = closeableSource;
    }

    public static ArrowVsrContext forSchema(ArrowVsrSchema schema, BufferAllocator allocator) {

        return new ArrowVsrContext(schema, allocator);
    }

    private ArrowVsrContext(ArrowVsrSchema schema, BufferAllocator allocator) {

        this.schema = schema;
        this.allocator = allocator;

        var fields = schema.physical().getFields();
        var vectors = new  ArrayList<FieldVector>(fields.size());

        for (var field : fields) {
            var vector = field.createVector(allocator);
            vectors.add(vector);
        }

        var root = new VectorSchemaRoot(fields, vectors);
        root.allocateNew();

        this.vsr = root;

        // Use pre-defined dictionaries from the schema (if there are any)
        this.dictionaries = schema.dictionaries();

        // VSR is constructed internally, dictionaries are owned by the schema
        this.vsrOwnership = true;
        this.dictionariesOwnership = false;
        this.closeableSource = null;
    }

    public ArrowVsrSchema getSchema() {
        return schema;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public VectorSchemaRoot getVsr() {
        return vsr;
    }

    public DictionaryProvider getDictionaries() {
        return dictionaries;
    }
    public void setRowCount(int nRows) {

        vsr.setRowCount(nRows);
    }

    public void setLoaded() {
        loaded = true;
    }

    public void setUnloaded() {
        loaded = false;
    }

    public boolean readyToLoad() {
        return !loaded;
    }

    public boolean readyToUnload() {
        return loaded;
    }

    @Override
    public void close() {

        if (vsrOwnership)
            vsr.close();

        if (dictionariesOwnership) {
            if (dictionaries != null) {
                for (var dictionaryId : dictionaries.getDictionaryIds()) {
                    var dictionary = dictionaries.lookup(dictionaryId);
                    dictionary.getVector().close();
                }
            }
        }

        if (closeableSource != null) {
            try {
                closeableSource.close();
            }
            catch (Throwable error) {
                throw new ETracInternal("Error closing data source: " + error.getMessage(), error);
            }
        }
    }
}
