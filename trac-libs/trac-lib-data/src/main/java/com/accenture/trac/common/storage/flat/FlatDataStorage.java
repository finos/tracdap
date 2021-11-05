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

import com.accenture.trac.common.codec.ICodecManager;
import com.accenture.trac.common.concurrent.IExecutionContext;
import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.data.IDataContext;
import com.accenture.trac.common.storage.IDataStorage;
import com.accenture.trac.common.storage.IFileStorage;
import com.accenture.trac.metadata.SchemaDefinition;
import com.accenture.trac.metadata.StorageCopy;

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;


public class FlatDataStorage implements IDataStorage {

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

        var storagePath = storageCopy.getStoragePath();
        var storageFormat = storageCopy.getStorageFormat();

        var codec = formats.getCodec(storageFormat);
        var codecOptions = Map.<String, String>of();
        var decoder = codec.getDecoder(dataContext.arrowAllocator(), schemaDef, codecOptions);

        var fileReader = fileStorage.reader(storagePath, dataContext);
        fileReader.subscribe(decoder);

        return decoder;
    }

    @Override
    public Flow.Subscriber<DataBlock> writer(
            SchemaDefinition schemaDef,
            StorageCopy storageCopy,
            CompletableFuture<Long> signal,
            IDataContext dataContext) {

        var storagePath = storageCopy.getStoragePath();
        var storageFormat = storageCopy.getStorageFormat();

        var chunkPath = storagePath + "/chunk-1.csv";  // TODO

        var codec = formats.getCodec(storageFormat);
        var codecOptions = Map.<String, String>of();
        var encoder = codec.getEncoder(dataContext.arrowAllocator(), schemaDef, codecOptions);

        var mkdir = fileStorage.mkdir(storagePath, /* recursive = */ true, dataContext);

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
}
