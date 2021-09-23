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

package com.accenture.trac.common.storage;

import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.storage.local.LocalFileStorage;
import com.accenture.trac.common.util.Concurrent;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.concurrent.UnorderedThreadPoolEventExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;


public class FileStorageTestSuite {

    public static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    IFileStorage storage;
    IExecutionContext execContext;

    @TempDir
    Path storageDir;

    @BeforeEach
    void setupStorage() {

        // TODO: Abstract mechanism for obtaining storage impl using config

        storage = new LocalFileStorage("TEST_STORAGE", storageDir.toString());
        execContext = () -> new UnorderedThreadPoolEventExecutor(1);
    }

    @Test
    void testExists_forDir() throws Exception {

        var prepare = storage.mkdir("test_dir", false);

        var dirPresent = prepare.thenCompose(x -> storage.exists("test_dir"));
        var dirNotPresent = prepare.thenCompose(x -> storage.exists("other_dir"));

        waitFor(TEST_TIMEOUT, dirPresent, dirNotPresent);

        Assertions.assertTrue(result(dirPresent));
        Assertions.assertFalse(result(dirNotPresent));
    }

    @Test
    void testExists_forFile() throws Exception {

        var prepare = makeSmallFile("test_file.txt");

        var filePresent = prepare.thenCompose(x -> storage.exists("test_file.txt"));
        var fileNotPresent = prepare.thenCompose(x -> storage.exists("other_file.txt"));

        waitFor(TEST_TIMEOUT, filePresent, fileNotPresent);

        Assertions.assertTrue(result(filePresent));
        Assertions.assertFalse(result(fileNotPresent));
    }

    @Test
    void testExists_badPaths() {

        testBadPaths(storage::exists);
    }

    @Test
    void testExists_storageRoot() {

        failForStorageRoot(storage::exists);
    }

    <T> void failForStorageRoot(Function<String, CompletionStage<T>> testMethod) {

        var storageRootResult = testMethod.apply(".");

        waitFor(TEST_TIMEOUT, storageRootResult);

        Assertions.assertThrows(ETracInternal.class, () -> result(storageRootResult));
    }

    <T> void testBadPaths(Function<String, CompletionStage<T>> testMethod) {

        var escapingPathResult = testMethod.apply("../");

        var absolutePathResult = OS.WINDOWS.isCurrentOs()
                ? testMethod.apply("C:\\Windows")
                : testMethod.apply("/bin");

        // \0 and / are the two characters that are always illegal in posix filenames
        // But / will be interpreted as a separator
        // There are several illegal characters for filenames on Windows!

        var invalidPathResult = OS.WINDOWS.isCurrentOs()
                ? testMethod.apply("£$ N'`¬$£>.)_£\"+%")
                : testMethod.apply("nul\0char");

        waitFor(TEST_TIMEOUT,
            escapingPathResult,
            absolutePathResult,
            invalidPathResult);

        Assertions.assertThrows(ETracInternal.class, () -> result(escapingPathResult));
        Assertions.assertThrows(ETracInternal.class, () -> result(absolutePathResult));
        Assertions.assertThrows(ETracInternal.class, () -> result(invalidPathResult));
    }

    private static void waitFor(Duration timeout, CompletionStage<?>... tasks) {

        waitFor(timeout, Arrays.asList(tasks));

        var latch = new CountDownLatch(tasks.length);

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

    private static void waitFor(Duration timeout, List<CompletionStage<?>> tasks) {

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

    private static <T> T result(CompletionStage<T> task) throws Exception {

        try {
            return task.toCompletableFuture().get(0, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {

            var cause = e.getCause();
            throw (cause instanceof Exception) ? (Exception) cause : e;
        }
    }

    private CompletableFuture<Long> makeSmallFile(String storagePath) {

        var content = ByteBufUtil.encodeString(
                ByteBufAllocator.DEFAULT,
                CharBuffer.wrap("Small file test content\n"),
                StandardCharsets.UTF_8);

        var signal = new CompletableFuture<Long>();
        var writer = storage.writer(storagePath, signal, execContext);

        Concurrent.javaStreamPublisher(Stream.of(content)).subscribe(writer);

        return signal;
    }


}
