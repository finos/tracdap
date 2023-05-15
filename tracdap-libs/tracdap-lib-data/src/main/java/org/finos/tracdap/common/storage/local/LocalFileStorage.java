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

package org.finos.tracdap.common.storage.local;

import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.storage.*;

import io.netty.channel.EventLoopGroup;
import org.apache.arrow.memory.ArrowBuf;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.READ;
import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public class LocalFileStorage extends CommonFileStorage {

    public static final String CONFIG_ROOT_PATH = "rootPath";

    private final Path rootPath;

    public LocalFileStorage(String storageKey, Properties properties) {

        super(FILE_SEMANTICS, storageKey, properties, new LocalStorageErrors(storageKey));

        // TODO: Robust config handling

        var rootDirProp = properties.getProperty(CONFIG_ROOT_PATH);
        this.rootPath = Paths.get(rootDirProp)
                .toAbsolutePath()
                .normalize();
    }

    @Override
    public void start(EventLoopGroup eventLoopGroup) {

        // These are just checks, no need to initialize anything

        if (!Files.exists(rootPath)) {
            var err = String.format("Storage root path does not exist: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        if (!Files.isDirectory(rootPath)) {
            var err = String.format("Storage root path is not a directory: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        if (!Files.isReadable(rootPath)) {
            var err = String.format("Storage root path is not readable: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        if(!readOnly && !Files.isWritable(rootPath)) {
            var err = String.format("Storage root path is not writable: %s [%s]", storageKey, rootPath);
            log.error(err);
            throw new EStartup(err);
        }

        logFsInfo();
    }

    @Override
    public void stop() {

        // No-op
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
    protected CompletionStage<Boolean> fsExists(String storagePath, IExecutionContext execContext) {

        var absolutePath = resolvePath(storagePath);
        var exists = Files.exists(absolutePath);

        return CompletableFuture.completedFuture(exists);
    }

    @Override
    protected CompletionStage<Boolean> fsDirExists(String storagePath, IExecutionContext execContext) {

        var absolutePath = resolvePath(storagePath);
        var exists = Files.isDirectory(absolutePath);

        return CompletableFuture.completedFuture(exists);
    }

    @Override
    protected CompletionStage<FileStat> fsGetFileInfo(String storagePath, IExecutionContext execContext) {

        try {
            var absolutePath = resolvePath(storagePath);
            var fileStat = buildFileStat(absolutePath, storagePath);

            return CompletableFuture.completedFuture(fileStat);
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(new CompletionException(e));
        }
    }

    @Override
    protected CompletionStage<FileStat> fsGetDirInfo(String storagePath, IExecutionContext execContext) {

        return fsGetFileInfo(storagePath, execContext);
    }

    @Override
    protected CompletionStage<List<FileStat>> fsListContents(
            String storagePath, String startAfter, int maxKeys, boolean recursive,
            IExecutionContext execContext) {

        try {
            var absolutePath = resolvePath(storagePath);
            var stat = buildFileStat(absolutePath, storagePath);

            if (stat.fileType != FileType.DIRECTORY) {
                var listing = List.of(stat);
                return CompletableFuture.completedFuture(listing);
            }

            try (var paths = Files.list(absolutePath)) {

                var entries = paths
                        .map(p -> buildFileStatChecked(p, rootPath.relativize(p).toString()))
                        .collect(Collectors.toList());

                return CompletableFuture.completedFuture(entries);
            }
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(new CompletionException(e));
        }
        catch (CompletionException e) {
            return CompletableFuture.failedFuture(e);
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

    private FileStat buildFileStat(Path absolutePath, String storagePath) throws IOException {

        // FileStat does not currently include permissions
        // If/when they are added, there are attribute view classes that do include them
        // We'd need to check for Windows / Posix and choose an attribute view type accordingly

        // Fix separator on Windows FS

        var separator = FileSystems.getDefault().getSeparator();

        if (!separator.equals(BACKSLASH))
            storagePath = storagePath.replace(separator, BACKSLASH);

        // Fix storage path for the storage root and directory entries

        storagePath =
                storagePath.isEmpty() ? "." :
                storagePath.endsWith(BACKSLASH) ? storagePath.substring(0, storagePath.length() - 1) :
                storagePath;

        var attrType = BasicFileAttributes.class;
        var attrs = Files.readAttributes(absolutePath, attrType);

        if (!attrs.isRegularFile() && !attrs.isDirectory())
            throw errors.explicitError(STAT_OPERATION, storagePath, NOT_A_FILE_OR_DIRECTORY);

        // Special handling for the root directory - do not return the name of the storage root folder!
        var fileName = absolutePath.equals(rootPath)
            ?   "." : absolutePath.getFileName().toString();

        var fileType = attrs.isRegularFile() ? FileType.FILE : FileType.DIRECTORY;

        // Do not report a size for directories, behavior for doing so is wildly inconsistent!
        var size = attrs.isRegularFile()
            ? attrs.size()
            : 0;

        var mtime = attrs.lastModifiedTime().toInstant();
        var atime = attrs.lastAccessTime().toInstant();

        return new FileStat(
                storagePath,
                fileName, fileType, size,
                mtime, atime);
    }

    private FileStat buildFileStatChecked(Path absolutePath, String storagePath) {

        try {
            return buildFileStat(absolutePath, storagePath);
        }
        catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    @Override
    protected CompletionStage<Void> fsCreateDir(String storagePath, IExecutionContext execContext) {

        try {
            var absolutePath = resolvePath(storagePath);
            Files.createDirectories(absolutePath);

            return CompletableFuture.completedFuture(null);
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(new CompletionException(e));
        }
    }

    @Override
    protected CompletionStage<Void> fsDeleteFile(String storagePath, IExecutionContext execContext) {

        try {
            var absolutePath = resolvePath(storagePath);
            Files.delete(absolutePath);

            return CompletableFuture.completedFuture(null);
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(new CompletionException(e));
        }
    }

    @Override
    protected CompletionStage<Void> fsDeleteDir(String storagePath, IExecutionContext execContext) {

        try {
            var absolutePath = resolvePath(storagePath);

            Files.walkFileTree(absolutePath, new SimpleFileVisitor<>() {

                @Override
                public FileVisitResult visitFile(
                        Path file, BasicFileAttributes attrs) throws IOException {

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

            return CompletableFuture.completedFuture(null);
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(new CompletionException(e));
        }
    }

    @Override
    protected CompletionStage<ArrowBuf> fsReadChunk(String storagePath, long offset, int size, IDataContext ctx) {

        try {

            var absolutePath = resolvePath(storagePath);

            @SuppressWarnings("resource")
            var channel = AsynchronousFileChannel.open(absolutePath, Set.of(READ), ctx.eventLoopExecutor());
            var read = new CompletableFuture<Integer>();

            @SuppressWarnings("resource")
            var allocator = ctx.arrowAllocator();
            var buffer = allocator.buffer(size);

            channel.read(buffer.nioBuffer(0, size), offset, null, new CompletionHandler<>() {
                @Override public void completed(Integer result, Object attachment) { read.complete(result); }
                @Override public void failed(Throwable exc, Object attachment) { read.completeExceptionally(exc); }
            });

            return read.handle((bytesRead, error) ->
                    fsReadChunkCallback(storagePath, size, channel, buffer, bytesRead, error));
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(new CompletionException(e));
        }
    }

    private ArrowBuf fsReadChunkCallback(
            String storagePath, long size,
            Channel channel, ArrowBuf buffer,
            long bytesRead, Throwable error) {

        try {

            if (error == null && bytesRead == size) {
                buffer.writerIndex(size);
                return buffer;
            }

            buffer.close();

            if (error == null) {
                log.warn("Requested: {}, read: {}", size, bytesRead);
                throw errors.explicitError(READ_OPERATION, storagePath, OBJECT_SIZE_TOO_SMALL);
            }

            if (error instanceof CompletionException)
                throw (CompletionException) error;

            throw new CompletionException(error);
        }
        finally {

            try {
                channel.close();
            }
            catch (IOException e) {
                log.warn("{} {} [{}]: Read channel did not close cleanly", READ_OPERATION, storageKey, storagePath, e);
            }
        }
    }

    @Override
    protected Flow.Publisher<ArrowBuf>
    fsOpenInputStream(String storagePath, IDataContext dataContext) {

        var absolutePath = resolvePath(storagePath);

        return new LocalFileReader(storagePath, absolutePath, dataContext, errors);
    }

    @Override
    protected Flow.Subscriber<ArrowBuf>
    fsOpenOutputStream(String storagePath, CompletableFuture<Long> signal, IDataContext dataContext) {

        var absolutePath = resolvePath(storagePath);

        return new LocalFileWriter(
                storagePath, absolutePath, signal,
                dataContext.eventLoopExecutor(),
                errors);
    }

    private Path resolvePath(String storagePath) {

        if (storagePath.isEmpty())
            return rootPath;
        else
            return rootPath.resolve(storagePath);
    }
}
