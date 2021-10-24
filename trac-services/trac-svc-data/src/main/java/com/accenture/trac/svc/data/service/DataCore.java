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

package com.accenture.trac.svc.data.service;

import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.storage.IFileStorage;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class DataCore {

    CompletableFuture<Boolean> writeData(Flow.Publisher<ByteBuf> byteStream, IExecutionContext execCtx) {

        var signal = new CompletableFuture<Boolean>();

        var codec = (IDataCodec) null;
        var decoder = codec.decoder(null);

        var storage = (IDataStorage) null;
        var writer = storage.writeDelta(null, signal, execCtx);

        byteStream.subscribe(decoder);
        decoder.subscribe(writer);

        return signal;
    }

    Flow.Publisher<ByteBuf> readDelta(IExecutionContext execCtx) {

        var codec = (IDataCodec) null;
        var encoder = codec.encoder(null);

        var storage = (IDataStorage) null;
        var reader = storage.readDelta(null, execCtx);

        reader.subscribe(encoder);

        return encoder;
    }

    interface IDataStorage {

        Flow.Publisher<VectorSchemaRoot> readDelta(
                Supplier<VectorSchemaRoot> recycler,
                IExecutionContext execCtx);

        Flow.Subscriber<VectorSchemaRoot> writeDelta(
                Consumer<VectorSchemaRoot> recycler,
                CompletableFuture<Boolean> signal,
                IExecutionContext execCtx);
    }

    interface IDataCodec {

        Flow.Processor<VectorSchemaRoot, ByteBuf> encoder(
                Consumer<VectorSchemaRoot> recycler);

        Flow.Processor<ByteBuf, VectorSchemaRoot> decoder(
                Supplier<VectorSchemaRoot> recycler);
    }

    class CsvDataCodec implements IDataCodec {

        @Override
        public Flow.Processor<VectorSchemaRoot, ByteBuf> encoder(Consumer<VectorSchemaRoot> recycler) {
            return null;
        }

        @Override
        public Flow.Processor<ByteBuf, VectorSchemaRoot> decoder(Supplier<VectorSchemaRoot> recycler) {
            return null;
        }
    }

    static class FlatDataStorage implements IDataStorage {

        private final IFileStorage fileStorage;

        public FlatDataStorage(IFileStorage fileStorage) {
            this.fileStorage = fileStorage;
        }

        @Override
        public Flow.Publisher<VectorSchemaRoot> readDelta(
                Supplier<VectorSchemaRoot> recycler,
                IExecutionContext execCtx) {

            var storagePath = (String) null;
            var codec = (IDataCodec) null;

            var byteStream = fileStorage.reader(storagePath, execCtx);
            var decoder = codec.decoder(recycler);
            byteStream.subscribe(decoder);

            return decoder;
        }

        @Override
        public Flow.Subscriber<VectorSchemaRoot> writeDelta(
                Consumer<VectorSchemaRoot> recycler,
                CompletableFuture<Boolean> signal,
                IExecutionContext execCtx) {

            var storagePath = (String) null;
            var codec = (IDataCodec) null;

            var fileSignal = new CompletableFuture<Long>();

            fileSignal.whenComplete((result, error) -> {

                if (error != null)
                    signal.completeExceptionally(error);
                else
                    signal.complete(true);
            });

            var fileWriter = fileStorage.writer(storagePath, fileSignal, execCtx);
            var encoder = codec.encoder(recycler);
            encoder.subscribe(fileWriter);

            return encoder;
        }
    }
}
