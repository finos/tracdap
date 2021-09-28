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
import io.netty.buffer.ByteBufAllocator;
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
import java.util.stream.Collectors;


public class LocalFileStorage implements IFileStorage {

    private static final String BACKSLASH = "/";

    private static final String EXISTS_OPERATION = "exists";
    private static final String SIZE_OPERATION = "size";
    private static final String STAT_OPERATION = "stat";
    private static final String LS_OPERATION = "ls";
    private static final String MKDIR_OPERATION = "mkdir";
    private static final String RM_OPERATION = "rm";
    private static final String WRITE_OPERATION = "write";
    private static final String READ_OPERATION = "read";

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

        logFsInfo();
    }

    private void logFsInfo() {

        try {
            var fileStore = Files.getFileStore(rootPath);

            log.info("Storage path: [{}]", rootPath);
            log.info("Storage volume: [{}] ({}{})",
                    fileStore.name(), fileStore.type(),
                    fileStore.isReadOnly() ? ", read-only" : "");

            var ONE_GIB = 1024 * 1024 * 1024;
            var totalCapacity = (double) fileStore.getTotalSpace() / ONE_GIB;
            var usableCapacity = (double) fileStore.getUsableSpace() / ONE_GIB;

            log.info("Storage capacity: total = {} GiB, free usable = {} GiB, block size = {} B",
                    String.format("%.1f", totalCapacity),
                    String.format("%.1f", usableCapacity),
                    fileStore.getBlockSize());
        }
        catch (IOException e) {

            var err = String.format(
                    "File store information not readable for storage root: %s [%s]",
                    storageKey, rootPath);

            log.error(err, e);
            throw new EStartup(err, e);
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

            var eStorage = handleIOException(e, EXISTS_OPERATION, storagePath);
            return CompletableFuture.failedFuture(eStorage);
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
                throw errorResult(SIZE_OF_DIR_ERROR, storagePath, SIZE_OPERATION);

            return CompletableFuture.completedFuture(size);
        }
        catch (Exception e) {

            var eStorage = handleIOException(e, storagePath, SIZE_OPERATION);
            return CompletableFuture.failedFuture(eStorage);
        }
    }

    @Override
    public CompletionStage<FileStat> stat(String storagePath) {

        try {
            var absolutePath = resolvePath(storagePath, false, STAT_OPERATION);
            var fileStat = buildFileStat(absolutePath, storagePath, STAT_OPERATION);

            return CompletableFuture.completedFuture(fileStat);
        }
        catch (Exception e) {

            var eStorage = handleIOException(e, storagePath, STAT_OPERATION);
            return CompletableFuture.failedFuture(eStorage);
        }
    }

    @Override
    public CompletionStage<DirStat> ls(String storagePath) {

        try {
            var absolutePath = resolvePath(storagePath, true, LS_OPERATION);

            try (var paths = Files.list(absolutePath)) {

                var entries = paths
                        .map(p -> buildFileStat(p, rootPath.relativize(p).toString(), LS_OPERATION))
                        .collect(Collectors.toList());

                var dirStat = new DirStat(entries);

                return CompletableFuture.completedFuture(dirStat);
            }
        }
        catch (Exception e) {

            var eStorage = handleIOException(e, storagePath, LS_OPERATION);
            return CompletableFuture.failedFuture(eStorage);
        }
    }

    // A note on atime in Windows file systems.

    // Atime on FAT is limited to one-day resolution
    // NTFS does not handle atime reliably for this test. From the docs:

    //      NTFS delays updates to the last access time for a file by up to one hour after the last access.
    //      NTFS also permits last access time updates to be disabled.
    //      Last access time is not updated on NTFS volumes by default.

    // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime?redirectedfrom=MSDN

    // In spite of these quirks, the available atime information on Windows may still be useful for housekeeping,
    // however it will cause problems for tasks that rely on precise timing and during testing.

    private FileStat buildFileStat(Path absolutePath, String storagePath, String operationName) {

        // FileStat does not currently include permissions
        // If/when they are added, there are attribute view classes that do include them
        // We'd need to check for Windows / Posix and choose an attribute view type accordingly

        try {

            var separator = FileSystems.getDefault().getSeparator();
            var storagePathWithBackslash = separator.equals(BACKSLASH)
                    ? storagePath
                    : storagePath.replace(separator, BACKSLASH);

            var attrType = BasicFileAttributes.class;
            var attrs = Files.readAttributes(absolutePath, attrType);

            if (!attrs.isRegularFile() && !attrs.isDirectory())
                throw errorResult(STAT_NOT_FILE_OR_DIR_ERROR, storagePath, STAT_OPERATION);

            var fileName = absolutePath.getFileName().toString();
            var fileType = attrs.isRegularFile() ? FileType.FILE : FileType.DIRECTORY;

            // Do not report a size for directories, behavior for doing so is wildly inconsistent!
            var size = attrs.isRegularFile()
                ? attrs.size()
                : 0;

            var ctime = attrs.creationTime().toInstant();
            var mtime = attrs.lastModifiedTime().toInstant();
            var atime = attrs.lastAccessTime().toInstant();

            return new FileStat(
                    storagePathWithBackslash,
                    fileName, fileType, size,
                    ctime, mtime, atime);
        }
        catch (IOException e) {

            throw handleIOException(e, storagePath, operationName);
        }
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

            var eStorage = handleIOException(e, storagePath, MKDIR_OPERATION);
            return CompletableFuture.failedFuture(eStorage);
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
                    throw errorResult(RM_DIR_RECURSIVE_ERROR, storagePath, RM_OPERATION);

                Files.delete(absolutePath);
            }

            return CompletableFuture.completedFuture(null);
        }
        catch (Exception e) {

            var eStorage = handleIOException(e, storagePath, RM_OPERATION);
            return CompletableFuture.failedFuture(eStorage);
        }
    }

    @Override
    public Flow.Publisher<ByteBuf> reader(String storagePath, IExecutionContext execContext) {

        var absolutePath = resolvePath(storagePath, false, READ_OPERATION);

        return new LocalFileReader(
                absolutePath, ByteBufAllocator.DEFAULT,
                execContext.eventLoopExecutor());
    }

    @Override
    public Flow.Subscriber<ByteBuf> writer(
            String storagePath,
            CompletableFuture<Long> signal,
            IExecutionContext execContext) {

        var absolutePath = resolvePath(storagePath, false, WRITE_OPERATION);

        return new LocalFileWriter(
                absolutePath, signal,
                execContext.eventLoopExecutor());
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

    private ETrac handleIOException(Exception e, String storagePath, String operationName) {

        // Error of type ETrac means the error is already handled
        if (e instanceof ETrac)
            return (ETrac) e;

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

    private EStorage errorResult(Exception e, String errorTemplate, String path, String operation) {

        var errorMessage = String.format(errorTemplate, storageKey, operation, path);

        log.error(errorMessage);
        log.error(e.getMessage(), e);

        return new EStorageRequest(errorMessage, e);
    }

    private EStorage errorResult(String errorTemplate, String path, String operation) {

        var errorMessage = String.format(errorTemplate, storageKey, operation, path);

        log.error(errorMessage);

        return new EStorageRequest(errorMessage);
    }
}
