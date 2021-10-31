/*
 * Copyright 2021 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.accenture.trac.common.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class BatchRecycler {

    private final List<Field> fields;
    private final int batchSize;
    private final BufferAllocator allocator;

    private final Queue<VectorSchemaRoot> pool;

    public BatchRecycler(List<Field> fields, int batchSize, BufferAllocator allocator) {

        this.fields = fields;
        this.batchSize = batchSize;
        this.allocator = allocator;

        pool = new ConcurrentLinkedQueue<>();
    }

    public Supplier<VectorSchemaRoot> supplier() {
        return this::supply;
    }

    public Consumer<VectorSchemaRoot> consumer() {
        return this::consume;
    }

    public void clear() {

        var batch = pool.poll();

        while (batch != null) {

            batch.clear();
            batch = pool.poll();
        }
    }

    private VectorSchemaRoot supply() {

        var recycledBatch = pool.poll();

        return (recycledBatch != null)
                ? recycledBatch
                : newBatch();
    }

    private void consume(VectorSchemaRoot batch) {

        var nCols = batch.getFieldVectors().size();

        for (var i = 0; i < nCols; i++)
            batch.getVector(i).reset();

        pool.add(batch);
    }

    private VectorSchemaRoot newBatch() {

        var vectors = fields.stream()
                .map(f -> f.createVector(allocator))
                .collect(Collectors.toList());

        vectors.forEach(v -> {
            v.setInitialCapacity(batchSize);
            v.allocateNew();
        });

        return new VectorSchemaRoot(fields, vectors);
    }
}