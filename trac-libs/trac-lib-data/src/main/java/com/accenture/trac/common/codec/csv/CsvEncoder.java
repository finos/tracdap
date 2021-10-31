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

package com.accenture.trac.common.codec.csv;

import com.accenture.trac.metadata.TableSchema;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.netty.buffer.ByteBuf;
import io.netty.util.ConstantPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class CsvEncoder implements Flow.Processor<VectorSchemaRoot, ByteBuf> {

    //private final TableSchema tableSchema;

    private final Consumer<VectorSchemaRoot> recycler;
    private final CsvMapper mapper = null;
    private CsvGenerator generator;
    private final PartialOutputStream output = null;

    private Flow.Subscription sourceSubscription;

    public CsvEncoder(Consumer<VectorSchemaRoot> recycler) {
        this.recycler = recycler;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuf> subscriber) {

        var targetSubscription = new CsvEncoderSubscription();
        subscriber.onSubscribe(targetSubscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

        sourceSubscription = subscription;
    }

    @Override
    public void onNext(VectorSchemaRoot batch) {

        try {

            var nRows = batch.getRowCount();
            var nCols = batch.getFieldVectors().size();

            for (int i = 0; i < nRows; i++) {

                generator.writeStartArray();

                for (int j = 0; j < nCols; j++) {

                    // TODO: Type mapping

                    var value = batch.getVector(j).getObject(i);
                    generator.writeString(value.toString());
                }

                generator.writeEndArray();
            }
        }
        catch (IOException e) {

            // TODO: Error
        }
        finally {

            recycler.accept(batch);
        }
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }

    private class CsvEncoderSubscription implements Flow.Subscription {

        @Override
        public void request(long n) {

        }

        @Override
        public void cancel() {

        }
    }

    private static class PartialOutputStream extends OutputStream {

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            super.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {

            this.write(new byte[0]);
        }
    }
}
