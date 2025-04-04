/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.config.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class LocalConfigLock {

    // These utilities provide a degree of safety around read / write locks for local (JKS) secrets
    // JKS secret service writes to the secret store, notifications trigger reads which can cause contention
    // Enterprise deployments are expected to use a plugin to integrate a dedicated secret management solution
    // It may still be appropriate to use local loaders for both config and secrets, which are read only

    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(1);

    static InputStream sharedReadStream(String path) throws IOException  {

        return sharedReadStream(Paths.get(path));
    }

    static InputStream sharedReadStream(Path path) throws IOException  {

        var channel = sharedReadChannel(path);
        return Channels.newInputStream(channel);
    }

    static FileChannel sharedReadChannel(Path path) throws IOException {

        try {

            var channel = FileChannel.open(path, StandardOpenOption.READ);
            var lock = CompletableFuture.supplyAsync(() -> {
                try {
                    return channel.lock(0, Long.MAX_VALUE, true);
                }
                catch (IOException e) {
                    throw new CompletionException(e);
                }
            }).orTimeout(LOCK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).get();

            if (lock == null) {
                channel.close();
                throw new IOException(String.format("Failed to acquire read lock: [%s]", path));
            }

            return channel;
        }
        catch (InterruptedException | ExecutionException e) {

            throw new IOException(String.format("Failed to acquire read lock: [%s]", path), e);
        }
    }

    static OutputStream exclusiveWriteStream(String path, boolean truncate) throws IOException  {

        return exclusiveWriteStream(Paths.get(path), truncate);
    }

    static OutputStream exclusiveWriteStream(Path path, boolean truncate) throws IOException  {

        var channel = exclusiveWriteChannel(path, truncate);
        return Channels.newOutputStream(channel);
    }

    static FileChannel exclusiveWriteChannel(Path path, boolean truncate) throws IOException  {

        try {

            var options = truncate
                ? new OpenOption [] {
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING }
                : new OpenOption [] {
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE };

            var channel = FileChannel.open(path,options);

            var lock = CompletableFuture.supplyAsync(() -> {
                try {
                    return channel.lock(0, Long.MAX_VALUE, false);
                }
                catch (IOException e) {
                    throw new CompletionException(e);
                }
            }).orTimeout(LOCK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).get();

            if (lock == null) {
                channel.close();
                throw new IOException(String.format("Failed to acquire write lock: [%s]", path));
            }

            return channel;
        }
        catch (InterruptedException | ExecutionException e) {

            throw new IOException(String.format("Failed to acquire write lock: [%s]", path), e);
        }
    }

    static void exclusiveMove(String source, String target) throws IOException  {

        exclusiveMove(Paths.get(source), Paths.get(target));
    }

    static void exclusiveMove(Path source, Path target) throws IOException  {

        try (var channel = FileChannel.open(target, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {

            var lock = CompletableFuture.supplyAsync(() -> {
                try {
                    return channel.lock(0, Long.MAX_VALUE, false);
                }
                catch (IOException e) {
                    throw new CompletionException(e);
                }
            }).orTimeout(LOCK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).get();

            if (lock != null) {
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                Files.delete(source);
            }
        }
        catch (InterruptedException | ExecutionException e) {

            throw new IOException(String.format("Failed to acquire write lock: [%s]", target), e);
        }
    }
}
