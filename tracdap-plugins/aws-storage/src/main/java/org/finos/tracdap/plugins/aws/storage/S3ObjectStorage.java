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

package org.finos.tracdap.plugins.aws.storage;

import io.netty.buffer.ByteBuf;
import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.storage.DirStat;
import org.finos.tracdap.common.storage.FileStat;
import org.finos.tracdap.common.storage.IFileStorage;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public class S3ObjectStorage implements IFileStorage {

    private final Properties properties;

    public S3ObjectStorage(Properties properties) {
        this.properties = properties;
    }

    @Override
    public CompletionStage<Boolean> exists(String storagePath, IExecutionContext execContext) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public CompletionStage<Long> size(String storagePath, IExecutionContext execContext) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public CompletionStage<FileStat> stat(String storagePath, IExecutionContext execContext) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public CompletionStage<DirStat> ls(String storagePath, IExecutionContext execContext) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public CompletionStage<Void> mkdir(String storagePath, boolean recursive, IExecutionContext execContext) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public CompletionStage<Void> rm(String storagePath, boolean recursive, IExecutionContext execContext) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public Flow.Publisher<ByteBuf> reader(String storagePath, IDataContext dataContext) {
        throw new ETracInternal("Not implemented yet");
    }

    @Override
    public Flow.Subscriber<ByteBuf> writer(String storagePath, CompletableFuture<Long> signal, IDataContext dataContext) {
        throw new ETracInternal("Not implemented yet");
    }
}
