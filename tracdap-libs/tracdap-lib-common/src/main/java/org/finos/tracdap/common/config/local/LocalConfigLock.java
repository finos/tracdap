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
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;


public class LocalConfigLock {

    // These utilities provide a degree of safety around read / write locks for local (JKS) secrets
    // JKS secret service writes to the secret store, notifications trigger reads which can cause contention
    // Enterprise deployments are expected to use a plugin to integrate a dedicated secret management solution
    // The local config loader is read-only, so locking will only occur if JKS secret service is being used

    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration LOCK_RETRY = Duration.ofMillis(50);

    static FileChannel sharedReadChannel(Path path) throws IOException {

        Instant deadline = Instant.now().plus(LOCK_TIMEOUT);
        FileChannel channel;
        FileLock lock;

        try {

            channel = FileChannel.open(path, StandardOpenOption.READ);

            do {

                lock = channel.tryLock(0, Long.MAX_VALUE, true);

                if (lock == null)
                    Thread.sleep(LOCK_RETRY.toMillis());

            } while (lock == null && Instant.now().isBefore(deadline));

            if (lock == null) {
                channel.close();
                throw new IOException(String.format("Failed to acquire read lock: [%s]", path));
            }

            return channel;
        }
        catch (InterruptedException e) {

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

        Instant deadline = Instant.now().plus(LOCK_TIMEOUT);
        FileChannel channel;
        FileLock lock;

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

        try {

            channel = FileChannel.open(path,options);

            do {

                lock = channel.tryLock(0, Long.MAX_VALUE, false);

                if (lock == null)
                    Thread.sleep(LOCK_RETRY.toMillis());

            } while (lock == null && Instant.now().isBefore(deadline));

            if (lock == null) {
                channel.close();
                throw new IOException(String.format("Failed to acquire write lock: [%s]", path));
            }

            return channel;
        }
        catch (InterruptedException e) {

            throw new IOException(String.format("Failed to acquire write lock: [%s]", path), e);
        }
    }

    static void exclusiveMove(String source, String target) throws IOException  {

        exclusiveMove(Paths.get(source), Paths.get(target));
    }

    static void exclusiveMove(Path source, Path target) throws IOException {

        try (var channel = FileChannel.open(target, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {

            Instant deadline = Instant.now().plus(LOCK_TIMEOUT);
            FileLock lock;

            do {

                lock = channel.tryLock(0, Long.MAX_VALUE, false);

                if (lock == null)
                    Thread.sleep(LOCK_RETRY.toMillis());

            } while (lock == null && Instant.now().isBefore(deadline));

            if (lock == null)
                throw new IOException(String.format("Failed to acquire write lock: [%s]", target));

            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            Files.delete(source);
        }
        catch (InterruptedException e) {

            throw new IOException(String.format("Failed to acquire write lock: [%s]", target), e);
        }
    }
}
