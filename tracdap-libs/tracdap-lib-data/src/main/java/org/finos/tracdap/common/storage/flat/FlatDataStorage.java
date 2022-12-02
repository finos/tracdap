/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.storage.flat;

import io.netty.channel.EventLoopGroup;
import org.finos.tracdap.common.codec.ICodec;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.storage.IDataStorage;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.metadata.StorageCopy;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


public class FlatDataStorage implements IDataStorage {

    private static final String CHUNK_ZERO_STORAGE_PATH = "/chunk-0.%s";

    private final IFileStorage fileStorage;
    private final ICodecManager formats;

    public FlatDataStorage(IFileStorage fileStorage, ICodecManager formats) {
        this.fileStorage = fileStorage;
        this.formats = formats;
    }

    @Override
    public void start(EventLoopGroup eventLoopGroup) {

        // No-op
    }

    @Override
    public void stop() {

        // No-op
    }

    @Override
    public DataPipeline pipelineReader(StorageCopy storageCopy, Schema requiredSchema, IDataContext dataContext) {

        var codec = formats.getCodec(storageCopy.getStorageFormat());

        var chunkPath = chunkPath(storageCopy, codec);
        var load = fileStorage.reader(chunkPath, dataContext);

        var pipeline = DataPipeline.forSource(load, dataContext);

        var options = Map.<String, String>of();
        var decoder = codec.getDecoder(dataContext.arrowAllocator(), requiredSchema, options);

        return pipeline.addStage(decoder);
    }

    @Override
    public DataPipeline pipelineWriter(
            StorageCopy storageCopy, Schema requiredSchema,
            IDataContext dataContext, DataPipeline pipeline,
            CompletableFuture<Long> signal) {

        var codec = formats.getCodec(storageCopy.getStorageFormat());
        var options = Map.<String, String>of();
        var encoder = codec.getEncoder(dataContext.arrowAllocator(), requiredSchema, options);

        pipeline = pipeline.addStage(encoder);

        var chunkPath = chunkPath(storageCopy, codec);
        var mkdir = fileStorage.mkdir(storageCopy.getStoragePath(), /* recursive = */ true, dataContext);
        var save = fileStorage.writer(chunkPath, signal, dataContext);
        var mkdirAndSave = Flows.waitForSignal(save, mkdir);

        return pipeline.addSink(mkdirAndSave);
    }

    private String chunkPath(StorageCopy storageCopy, ICodec codec) {

        var storagePath = storageCopy.getStoragePath();
        var extension = codec.defaultFileExtension();

        return storagePath + String.format(CHUNK_ZERO_STORAGE_PATH, extension);
    }
}
