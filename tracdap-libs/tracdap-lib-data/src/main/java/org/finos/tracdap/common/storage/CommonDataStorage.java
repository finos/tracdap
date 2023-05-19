/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.storage;

import io.netty.channel.EventLoopGroup;
import org.finos.tracdap.common.codec.ICodec;
import org.finos.tracdap.common.codec.ICodecManager;
import org.finos.tracdap.common.async.Flows;
import org.finos.tracdap.common.data.DataPipeline;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.data.pipeline.RangeSelector;
import org.finos.tracdap.common.exception.EStorageValidation;
import org.finos.tracdap.config.PluginConfig;
import org.finos.tracdap.metadata.StorageCopy;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


public class CommonDataStorage implements IDataStorage {

    public static final String DOWNLOAD_SIZE_LIMIT_KEY = "downloadSizeLimit";
    public static final long DOWNLOAD_SIZE_LIMIT_DEFAULT = 1073741824;

    private static final String CHUNK_ZERO_STORAGE_PATH = "/chunk-0.%s";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IFileStorage fileStorage;
    private final ICodecManager formats;

    private final long downloadSizeLimit;

    public CommonDataStorage(PluginConfig bucketConfig, IFileStorage fileStorage, ICodecManager formats) {

        this.fileStorage = fileStorage;
        this.formats = formats;

        var downloadSizeLimitSetting = bucketConfig.getPropertiesOrDefault(
                DOWNLOAD_SIZE_LIMIT_KEY,
                Long.toString(DOWNLOAD_SIZE_LIMIT_DEFAULT));

        downloadSizeLimit = Long.parseLong(downloadSizeLimitSetting);
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
    public DataPipeline pipelineReader(
            StorageCopy storageCopy, Schema requiredSchema, IDataContext dataContext,
            long offset, long limit) {

        var codec = formats.getCodec(storageCopy.getStorageFormat());

        var chunkPath = chunkPath(storageCopy, codec);
        var load = fileStorage.reader(chunkPath, dataContext);
        var checkSize = fileStorage.size(chunkPath, dataContext).thenApply(this::checkSizeBeforeLoad);
        var checkAndLoad = Flows.waitForSignal(load, checkSize);

        var pipeline = DataPipeline.forSource(checkAndLoad, dataContext);

        var options = Map.<String, String>of();
        var decoder = codec.getDecoder(dataContext.arrowAllocator(), requiredSchema, options);

        pipeline.addStage(decoder);

        if (offset != 0 || limit != 0)
            pipeline.addStage(new RangeSelector(offset, limit));

        return pipeline;
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

    private long checkSizeBeforeLoad(long fileSize) {

        // size limit of zero disables the limit
        if (fileSize > downloadSizeLimit && downloadSizeLimit != 0) {

            var message = String.format(
                    "File size of %s exceeds the configured download limit of %s; " +
                    "you can run an aggregation or ask your administrator to increase the limit",
                    humanReadableSize(fileSize),
                    humanReadableSize(downloadSizeLimit));

            log.error(message);
            throw new EStorageValidation(message);
        }

        return fileSize;
    }

    private String humanReadableSize(long size) {

        if (size < 1024)
            return String.format("%d bytes", size);

        if (size < 1024 * 1024) {
            double kb = size / 1024.0;
            return String.format("%.1f KB", kb);
        }

        if (size < 1024 * 1024 * 1024) {
            double mb = size / (1024.0 * 1024.0);
            return String.format("%.1f MB", mb);
        }

        double gb = size / (1024.0 * 1024.0 * 1024.0);
        return String.format("%.1f GB", gb);
    }
}
