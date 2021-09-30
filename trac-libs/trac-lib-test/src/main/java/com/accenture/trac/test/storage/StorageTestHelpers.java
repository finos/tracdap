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

package com.accenture.trac.test.storage;

import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.storage.IFileStorage;
import com.accenture.trac.common.util.Concurrent;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;


public class StorageTestHelpers {

    public static CompletableFuture<Long> makeFile(
            String storagePath, ByteBuf content,
            IFileStorage storage, IExecutionContext execContext) {

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, execContext);

        Concurrent.publish(Stream.of(content)).subscribe(writer);

        return signal;
    }

    public static CompletableFuture<Long> makeSmallFile(
            String storagePath,
            IFileStorage storage,
            IExecutionContext execContext) {

        var content = ByteBufUtil.encodeString(
                ByteBufAllocator.DEFAULT,
                CharBuffer.wrap("Small file test content\n"),
                StandardCharsets.UTF_8);

        return makeFile(storagePath, content, storage, execContext);
    }

    public static void waitFor(Duration timeout, CompletionStage<?>... tasks) {

        waitFor(timeout, Arrays.asList(tasks));
    }

    public static void waitFor(Duration timeout, List<CompletionStage<?>> tasks) {

        var latch = new CountDownLatch(tasks.size());

        for (var task: tasks)
            task.whenComplete((result, error) -> latch.countDown());

        try {
            var complete = latch.await(timeout.getSeconds(), TimeUnit.SECONDS);

            if (!complete)
                throw new RuntimeException("Test timed out");
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Test interrupted", e);
        }
    }

    public static <T> T resultOf(CompletionStage<T> task) throws Exception {

        try {
            return task.toCompletableFuture().get(0, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {

            var cause = e.getCause();
            throw (cause instanceof Exception) ? (Exception) cause : e;
        }
    }
}
