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

package com.accenture.trac.common.storage.local;

import com.accenture.trac.common.eventloop.IExecutionContext;
import com.accenture.trac.common.exception.*;
import com.accenture.trac.common.storage.FileStat;
import com.accenture.trac.common.storage.IFileStorage;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;


public class LocalFileStorage implements IFileStorage {

    protected static final String EXISTS_OPERATION = "exists";
    protected static final String SIZE_OPERATION = "size";
    protected static final String STAT_OPERATION = "stat";
    protected static final String LS_OPERATION = "ls";
    protected static final String MKDIR_OPERATION = "mkdir";
    protected static final String RM_OPERATION = "rm";
    protected static final String WRITE_OPERATION = "write";
    protected static final String READ_OPERATION = "read";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageKey;
    private final Path rootPath;

    public LocalFileStorage(String storageKey, String storageRootPath) {

        this.storageKey = storageKey;
        this.rootPath = Paths.get(storageRootPath)
                .toAbsolutePath()
                .normalize();

        if (!Files.exists(this.rootPath)) {
            var err = String.format("Storage root path does not exist: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        if (!Files.isDirectory(this.rootPath)) {
            var err = String.format("Storage root path is not a directory: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        if (!Files.isReadable(this.rootPath) || !Files.isWritable(this.rootPath)) {

            var err = String.format(
                    "Storage root path has insufficient access (read/write required): %s [%s]",
                    storageKey, rootPath);

            log.error(err);
            throw new EStartup(err);
        }
    }

    @Override
    public CompletionStage<Boolean> exists(String storagePath) {

        try {
            var absolutePath = resolvePath(storagePath, false, EXISTS_OPERATION);

            var exists = Files.exists(absolutePath);

            return CompletableFuture.completedFuture(exists);
        }
        catch (Exception e) {

            return handleIOException(e, EXISTS_OPERATION, storagePath);
        }
    }

    @Override
    public CompletableFuture<Long> size(String storagePath) {

        try {
            var absolutePath = resolvePath(storagePath, false, SIZE_OPERATION);

            var size = Files.size(absolutePath);

            return CompletableFuture.completedFuture(size);
        }
        catch (Exception e) {

            return handleIOException(e, storagePath, SIZE_OPERATION);
        }
    }

    @Override
    public CompletionStage<FileStat> stat(String storagePath) {

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public CompletionStage<Void> ls(String storagePath) {

        var absolutePath = resolvePath(storagePath, true,  LS_OPERATION);

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public CompletionStage<Void> mkdir(String storagePath, boolean recursive) {

        try {
            var absolutePath = resolvePath(storagePath, false, MKDIR_OPERATION);

            if (recursive)
                Files.createDirectories(absolutePath);
            else
                Files.createDirectory(absolutePath);

            return CompletableFuture.completedFuture(null);
        }
        catch (Exception e) {

            return handleIOException(e, storagePath, MKDIR_OPERATION);
        }
    }

    @Override
    public CompletionStage<Void> rm(String storagePath, boolean recursive) {

        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public Flow.Publisher<ByteBuf> reader(String storagePath, IExecutionContext execContext) {
        return null;
    }

    @Override
    public Flow.Subscriber<ByteBuf> writer(
            String storagePath,
            CompletableFuture<Long> signal,
            IExecutionContext execContext) {

        var absolutePath = resolvePath(storagePath, false, WRITE_OPERATION);

        return new LocalFileWriter(absolutePath, signal, execContext.eventLoopExecutor());
    }

    private Path resolvePath(String storagePath, boolean allowRootDir, String operationName) {

        try {

            if (storagePath == null || storagePath.isBlank()) {

                var err = String.format(
                        "Requested storage path is null or blank: %s %s [%s]",
                        storageKey, operationName, storagePath);

                log.error(err);
                throw new EValidationGap(err);
            }

            var relativePath = Path.of(storagePath);

            log.info("{}", relativePath);

            if (relativePath.isAbsolute()) {

                var err = String.format(
                        "Requested storage path is not a relative path: %s %s [%s]",
                        storageKey, operationName, storagePath);

                log.error(err);
                throw new EValidationGap(err);
            }

            var absolutePath = rootPath.resolve(storagePath).normalize();

            if (absolutePath.getNameCount() < rootPath.getNameCount() || !absolutePath.startsWith(rootPath)) {

                var err = String.format(
                        "Requested storage path is outside the storage root directory: %s %s [%s]",
                        storageKey, operationName, storagePath);

                log.error(err);
                throw new EValidationGap(err);
            }

            if (absolutePath.equals(rootPath) && !allowRootDir) {

                var err = String.format(
                        "Requested operation not allowed on the storage root directory: %s %s [%s]",
                        storageKey, operationName, storagePath);

                log.error(err);
                throw new EValidationGap(err);

            }

            return absolutePath;
        }
        catch (InvalidPathException e) {

            var err = String.format(
                    "Requested storage path is invalid: %s %s [%s]",
                    storageKey, operationName, storagePath);

            log.error(err, e);
            throw new EValidationGap(err);

        }
    }

    private <T> CompletableFuture<T> handleIOException(Exception e, String storagePath, String operationName) {

        if (e instanceof ETrac)
            return CompletableFuture.failedFuture(e);

        if (e instanceof FileNotFoundException) {

            var err = String.format(
                    "File not found in storage layer: %s %s [%s]",
                    storageKey, operationName, storagePath);

            log.error(err);
            log.error(e.getMessage(), e);

            return CompletableFuture.failedFuture(new EStorageRequest(err, e));
        }

        if (e instanceof FileAlreadyExistsException) {

            var err = String.format(
                    "File already exists in storage layer: %s %s [%s]",
                    storageKey, operationName, storagePath);

            log.error(err);
            log.error(e.getMessage(), e);

            return CompletableFuture.failedFuture(new EStorageRequest(err, e));
        }

        if (e instanceof AccessDeniedException || e instanceof SecurityException) {

            var err = String.format(
                    "Access denied in storage layer: %s %s [%s]",
                    storageKey, operationName, storagePath);

            log.error(err);
            log.error(e.getMessage(), e);

            return CompletableFuture.failedFuture(new EStorageAccess(err, e));
        }

        if (e instanceof IOException) {

            var err = String.format(
                    "An IO error occurred in the storage layer: %s %s [%s]",
                    storageKey, operationName, storagePath);

            log.error(err);
            log.error(e.getMessage(), e);

            return CompletableFuture.failedFuture(new EStorageCommunication(err, e));
        }

        var err = String.format(
                "An unexpected error occurred in the storage layer: %s %s [%s]",
                storageKey, operationName, storagePath);

        log.error(err);
        log.error(e.getMessage(), e);

        return CompletableFuture.failedFuture(new ETracInternal(err, e));
    }

}
