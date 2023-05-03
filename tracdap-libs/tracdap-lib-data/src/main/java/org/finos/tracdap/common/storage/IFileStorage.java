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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public interface IFileStorage extends AutoCloseable {

    void start(EventLoopGroup eventLoopGroup);

    void stop();

    @Override
    default void close() { stop(); }

    CompletionStage<Boolean> exists(String storagePath, IExecutionContext execContext);

    CompletionStage<Long> size(String storagePath, IExecutionContext execContext);

    CompletionStage<FileStat> stat(String storagePath, IExecutionContext execContext);

    CompletionStage<List<FileStat>> ls(String storagePath, IExecutionContext execContext);

    CompletionStage<Void> mkdir(String storagePath, boolean recursive, IExecutionContext execContext);

    CompletionStage<Void> rm(String storagePath, IExecutionContext execContext);

    CompletionStage<Void> rmdir(String storagePath, IExecutionContext execContext);

    Flow.Publisher<ByteBuf> reader(
            String storagePath,
            IDataContext dataContext);

    Flow.Subscriber<ByteBuf> writer(
            String storagePath,
            CompletableFuture<Long> signal,
            IDataContext dataContext);
}
