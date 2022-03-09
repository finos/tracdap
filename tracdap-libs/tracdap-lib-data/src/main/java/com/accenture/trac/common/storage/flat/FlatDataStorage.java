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

package com.accenture.trac.common.storage.flat;

import com.accenture.trac.common.codec.ICodec;
import com.accenture.trac.common.codec.ICodecManager;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.data.IDataContext;
import com.accenture.trac.common.storage.IDataStorage;
import com.accenture.trac.common.storage.IFileStorage;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.StorageCopy;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;


public class FlatDataStorage implements IDataStorage {

    private static final String CHUNK_ZERO_STORAGE_PATH = "/chunk-0.%s";

    private final IFileStorage fileStorage;
    private final ICodecManager formats;

    public FlatDataStorage(IFileStorage fileStorage, ICodecManager formats) {
        this.fileStorage = fileStorage;
        this.formats = formats;
    }

    @Override
    public Flow.Publisher<DataBlock> reader(
            SchemaDefinition schemaDef,
            StorageCopy storageCopy,
            IDataContext dataContext) {

        var codec = formats.getCodec(storageCopy.getStorageFormat());
        var codecOptions = Map.<String, String>of();

        var chunkPath = chunkPath(storageCopy, codec);

        var fileReader = fileStorage.reader(chunkPath, dataContext);
        var decoder = codec.getDecoder(dataContext.arrowAllocator(), schemaDef, codecOptions);
        fileReader.subscribe(decoder);

        return decoder;
    }

    @Override
    public Flow.Subscriber<DataBlock> writer(
            SchemaDefinition schemaDef,
            StorageCopy storageCopy,
            CompletableFuture<Long> signal,
            IDataContext dataContext) {

        var codec = formats.getCodec(storageCopy.getStorageFormat());
        var codecOptions = Map.<String, String>of();

        var chunkPath = chunkPath(storageCopy, codec);

        var encoder = codec.getEncoder(dataContext.arrowAllocator(), schemaDef, codecOptions);
        var mkdir = fileStorage.mkdir(storageCopy.getStoragePath(), /* recursive = */ true, dataContext);

        mkdir.whenComplete((result, error) -> {

            if (error != null)
                signal.completeExceptionally(error);

            else {
                var fileWriter = fileStorage.writer(chunkPath, signal, dataContext);
                encoder.subscribe(fileWriter);
            }
        });

        return encoder;
    }

    private String chunkPath(StorageCopy storageCopy, ICodec codec) {

        var storagePath = storageCopy.getStoragePath();
        var extension = codec.defaultFileExtension();

        return storagePath + String.format(CHUNK_ZERO_STORAGE_PATH, extension);
    }
}
