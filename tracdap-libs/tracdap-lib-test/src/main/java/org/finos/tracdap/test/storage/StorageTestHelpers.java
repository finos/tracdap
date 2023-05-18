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

package org.finos.tracdap.test.storage;

import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.storage.IFileStorage;

import org.apache.arrow.memory.ArrowBuf;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.stream.Stream;


public class StorageTestHelpers {

    public static CompletableFuture<Long> makeFile(
            String storagePath, ArrowBuf content,
            IFileStorage storage, IDataContext dataContext) {

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, dataContext);

        Flows.publish(Stream.of(content)).subscribe(writer);

        return signal;
    }

    public static CompletableFuture<Long> makeSmallFile(
            String storagePath,
            IFileStorage storage,
            IDataContext dataContext) {

        var bytes = "Small file test content\n".getBytes(StandardCharsets.UTF_8);
        var content = dataContext.arrowAllocator().buffer(bytes.length);
        content.writeBytes(bytes);

        return makeFile(storagePath, content, storage, dataContext);
    }

    public static CompletionStage<ArrowBuf> readFile(
            String storagePath,
            IFileStorage storage,
            IDataContext dataContext) {

        return storage.size(storagePath, dataContext).thenCompose(size ->
                storage.readChunk(storagePath, 0, (int)(long) size, dataContext));
    }
}
