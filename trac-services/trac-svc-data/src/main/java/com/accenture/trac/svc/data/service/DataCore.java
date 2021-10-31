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

import com.accenture.trac.api.MetadataReadRequest;
import com.accenture.trac.common.concurrent.Flows;
import com.accenture.trac.common.concurrent.Futures;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.data.BatchRecycler;
import com.accenture.trac.common.storage.IFileStorage;
import com.accenture.trac.metadata.DataDefinition;
import com.accenture.trac.metadata.StorageDefinition;
import com.accenture.trac.metadata.Tag;
import com.accenture.trac.metadata.TagSelector;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class DataCore {

    CompletableFuture<Boolean> writeDelta(
            Flow.Publisher<ByteBuf> byteStream,
            IExecutionContext execCtx) {

        var signal = new CompletableFuture<Boolean>();

        var fields = List.<Field>of();
        var batchSize = 1024;
        var allocator = new RootAllocator();

        var recycler = new BatchRecycler(fields, batchSize, allocator);

        var codec = (IDataCodec) null;
        var decoder = codec.decoder(recycler.supplier());

        var storage = (IDataStorage) null;
        var writer = storage.writeDelta(recycler.consumer(), signal, execCtx);

        byteStream.subscribe(decoder);
        decoder.subscribe(writer);

        return signal.whenComplete((result, err) -> recycler.clear());
    }

    void readPart(String tenant, TagSelector selector) {

        var state = new RequestState();

        CompletableFuture.completedFuture(null)

                .thenCompose(x -> readMetadata(tenant, selector))
                .thenAccept(obj -> state.file = obj.getDefinition().getFile())

                .thenCompose(x -> readMetadata(tenant, state.file.getStorageId()))
                .thenAccept(obj -> state.storage = obj.getDefinition().getStorage())
    }

    private Flow.Publisher<ByteBuf> readPart(
            DataDefinition data,
            StorageDefinition storage,
            IExecutionContext execCtx) {

        var part = data.getPartsOrDefault(null, null);
        var snap = part.getSnap();

        return readSnap(snap, storage, execCtx);
    }

    private Flow.Publisher<ByteBuf> readSnap(
            DataDefinition.Snap snap,
            StorageDefinition storage,
            IExecutionContext execCtx) {

        var deltas = snap.getDeltasList()
                .stream()
                .map(d -> readDelta(d, storage, execCtx))
                .collect(Collectors.toList());

        return Flows.concat(deltas);
    }

    Flow.Publisher<ByteBuf> readDelta(
            DataDefinition.Delta delta,
            StorageDefinition storage,
            IExecutionContext execCtx) {

        var fields = List.<Field>of();
        var batchSize = 1024;
        var allocator = new RootAllocator();

        var recycler = new BatchRecycler(fields, batchSize, allocator);

        var codec = (IDataCodec) null;
        var encoder = codec.encoder(recycler.consumer());

        var storage = (IDataStorage) null;
        var reader = storage.readDelta(recycler.supplier(), execCtx);

        reader.subscribe(encoder);

        return encoder;  // TODO: Concurrent.whenComplete(encoder, (result, err) -> recycler.clear());
    }

    private CompletionStage<Tag> readMetadata(String tenant, TagSelector selector) {

        var metaRequest = MetadataReadRequest.newBuilder()
                .setTenant(tenant)
                .setSelector(selector)
                .build();

        return Futures.javaFuture(metaApi.readObject(metaRequest));
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
