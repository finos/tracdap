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
import com.accenture.trac.common.storage.DirStat;
import com.accenture.trac.common.storage.FileStat;
import com.accenture.trac.common.storage.FileType;
import com.accenture.trac.common.storage.IFileStorage;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
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

    private static final List<Map.Entry<Class<? extends Exception>, String>> ERROR_MAP = List.of(
            Map.entry(NoSuchFileException.class, "File not found in storage layer: %s %s [%s]"),
            Map.entry(FileAlreadyExistsException.class, "File already exists in storage layer: %s %s [%s]"),
            Map.entry(DirectoryNotEmptyException.class, "Directory is not empty in storage layer: %s %s [%s]"),
            Map.entry(AccessDeniedException.class, "Access denied in storage layer: %s %s [%s]"),
            Map.entry(SecurityException.class, "Access denied in storage layer: %s %s [%s]"),
            // IOException must be last in the list, not to obscure most specific exceptions
            Map.entry(IOException.class, "An IO error occurred in the storage layer: %s %s [%s]"));

    private static final String SIZE_OF_DIR_ERROR = "Size operation is not available for directories: %s %s [%s]";
    private static final String STAT_NOT_FILE_OR_DIR_ERROR = "Object is not a file or directory: %s %s [%s]";
    private static final String RM_DIR_RECURSIVE_ERROR =
            "Regular delete operation not available for directories (use recursive delete): %s %s [%s]";

    private static final String UNKNOWN_ERROR = "An unexpected error occurred in the storage layer: %s %s [%s]";

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

            // Size operation for non-regular files can still succeed
            // So, add an explicit check for directories (and other non-regular files)
            if (!Files.isRegularFile(absolutePath))
                return errorResult(SIZE_OF_DIR_ERROR, storagePath, SIZE_OPERATION);

            return CompletableFuture.completedFuture(size);
        }
        catch (Exception e) {

            return handleIOException(e, storagePath, SIZE_OPERATION);
        }
    }

    @Override
    public CompletionStage<FileStat> stat(String storagePath) {

        try {
            var absolutePath = resolvePath(storagePath, false, STAT_OPERATION);

            // FileStat does not currently include permissions
            // If/when they are added, there are attribute view classes that do include them
            // We'd need to check for Windows / Posix and choose an attribute view type accordingly

            var attrViewType = BasicFileAttributeView.class;
            var attrView = Files.getFileAttributeView(absolutePath, attrViewType);
            var attrs = attrView.readAttributes();

            if (!attrs.isRegularFile() && !attrs.isDirectory())
                return errorResult(STAT_NOT_FILE_OR_DIR_ERROR, storagePath, STAT_OPERATION);

            var fileName = absolutePath.getFileName().toString();
            var fileType = attrs.isRegularFile() ? FileType.FILE : FileType.DIRECTORY;
            var size = attrs.size();

            var ctime = attrs.creationTime().toInstant();
            var mtime = attrs.lastModifiedTime().toInstant();
            var atime = attrs.lastAccessTime().toInstant();

            var stat = new FileStat(
                    storagePath, fileName, fileType, size,
                    ctime, mtime, atime);

            return CompletableFuture.completedFuture(stat);
        }
        catch (Exception e) {

            return handleIOException(e, storagePath, STAT_OPERATION);
        }
    }

    @Override
    public CompletionStage<DirStat> ls(String storagePath) {

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

        try {
            var absolutePath = resolvePath(storagePath, false, RM_OPERATION);

            if (recursive) {

                Files.walkFileTree(absolutePath, new SimpleFileVisitor<>() {

                    @Override
                    public FileVisitResult visitFile(
                            Path file,
                            BasicFileAttributes attrs) throws IOException {

                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(
                            Path dir, IOException exc) throws IOException {

                        if (exc != null) throw exc;

                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            else {

                // Do not allow rm on a directory with the recursive flag set
                if (Files.isDirectory(absolutePath))
                    return errorResult(RM_DIR_RECURSIVE_ERROR, storagePath, RM_OPERATION);

                Files.delete(absolutePath);
            }

            return CompletableFuture.completedFuture(null);
        }
        catch (Exception e) {

            return handleIOException(e, storagePath, RM_OPERATION);
        }
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

        // Error of type ETrac means the error is already handled
        if (e instanceof ETrac)
            return CompletableFuture.failedFuture(e);

        // Look in the map of error types to see if e is an expected exception
        for (var error : ERROR_MAP) {

            var errorClass = error.getKey();

            if (errorClass.isInstance(e)) {

                var errorMessageTemplate = error.getValue();
                return errorResult(e, errorMessageTemplate, storagePath, operationName);
            }
        }

        // Last fallback - report an unknown error in the storage layer
        return errorResult(e, UNKNOWN_ERROR, storagePath, operationName);
    }

    private <T> CompletableFuture<T> errorResult(Exception e, String errorTemplate, String path, String operation) {

        var errorMessage = String.format(errorTemplate, storageKey, operation, path);

        log.error(errorMessage);
        log.error(e.getMessage(), e);

        return CompletableFuture.failedFuture(new EStorageRequest(errorMessage, e));
    }

    private <T> CompletableFuture<T> errorResult(String errorTemplate, String path, String operation) {

        var errorMessage = String.format(errorTemplate, storageKey, operation, path);

        log.error(errorMessage);

        return CompletableFuture.failedFuture(new EStorageRequest(errorMessage));
    }
}
