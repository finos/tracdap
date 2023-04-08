/*
 * Copyright 2023 Accenture Global Solutions Limited
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

import io.netty.channel.EventLoopGroup;
import org.finos.tracdap.common.concurrent.IExecutionContext;
import org.finos.tracdap.common.data.IDataContext;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


public abstract class CommonFileStorage implements IFileStorage {

    protected abstract CompletionStage<Boolean> objectExists(String objectKey, IExecutionContext ctx);
    protected abstract CompletionStage<Boolean> prefixExists(String prefix, IExecutionContext ctx);

    protected abstract CompletionStage<FileStat> objectStat(String objectKey, IExecutionContext ctx);
    protected abstract CompletionStage<FileStat> prefixStat(String prefix, IExecutionContext ctx);

    protected abstract CompletionStage<List<FileStat>> prefixLs(
            String prefix, String startAfter, int maxKeys, boolean recursive,
            IExecutionContext ctx);

    protected abstract CompletionStage<Void> prefixMkdir(String prefix, IExecutionContext ctx);





    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storageType;
    private final String storageKey;
    private final String rootPath;

    private final IFileStorage fs;
    private final StorageErrors errors;

    protected CommonFileStorage(
            String storageType, String storageKey, String rootPath,
            IFileStorage fs, StorageErrors errors) {

        this.storageType = storageType;
        this.storageKey = storageKey;
        this.rootPath = rootPath;

        this.fs = fs;
        this.errors = errors;
    }

    @Override
    public void start(EventLoopGroup eventLoopGroup) {

        log.info("INIT [{}]: Common file storage, fs = [{}], root = [{}]", storageKey, storageType, rootPath);

        fs.start(eventLoopGroup);
    }

    @Override
    public void stop() {

        fs.stop();
    }

    @Override
    public CompletionStage<Boolean>
    exists(String storagePath, IExecutionContext ctx) {

        return wrapOperation("EXISTS", storagePath, (op, path) -> _exists(op, path, ctx));
    }

    private CompletionStage<Boolean>
    _exists(String operationName, String storagePath, IExecutionContext ctx) {

        var objectKey = resolvePath(operationName, storagePath, true);
        var prefix = objectKey + BACKSLASH;

        if (storagePath.endsWith(BACKSLASH)) {

            return prefixExists(prefix, ctx).thenCompose(
                exists -> exists
                    ? CompletableFuture.completedFuture(true)
                    : objectExists(objectKey, ctx));
        }
        else {

            return objectExists(objectKey, ctx).thenCompose(
                exists -> exists
                    ? CompletableFuture.completedFuture(true)
                    : prefixExists(prefix, ctx));
        }
    }

    @Override
    public CompletionStage<Long>
    size(String storagePath, IExecutionContext ctx) {

        return wrapOperation("SIZE", storagePath, (op, path) -> _size(op, path, ctx));
    }

    private CompletionStage<Long>
    _size(String operationName, String storagePath, IExecutionContext ctx) {

        var _stat = _stat(operationName, storagePath, ctx);

        return _stat.thenApply(stat -> {

            if (stat.fileType != FileType.FILE)
                throw errors.explicitError(NOT_A_FILE, storagePath, operationName);  // Todo

            return stat.size;
        });
    }

    @Override
    public CompletionStage<FileStat>
    stat(String storagePath, IExecutionContext ctx) {

        return wrapOperation("STAT", storagePath, (op, path) -> _stat(op, path, ctx));
    }

    private CompletionStage<FileStat>
    _stat(String operationName, String storagePath, IExecutionContext ctx) {

        var objectKey = resolvePath(operationName, storagePath, true);
        var prefix = objectKey + BACKSLASH;

        return prefixExists(prefix, ctx).thenCompose(isDir -> isDir
                ? prefixStat(prefix, ctx)
                : objectStat(objectKey, ctx));
    }

    @Override
    public CompletionStage<List<FileStat>>
    ls(String storagePath, IExecutionContext ctx) {

        return wrapOperation("LS", storagePath, (op, path) -> _ls(op, path, false, ctx));
    }

    private CompletionStage<List<FileStat>>
    _ls(String operationName, String storagePath, boolean recursive, IExecutionContext ctx) {

        var _stat = _stat(operationName, storagePath, ctx);

        return _stat.thenCompose(stat -> {

            if (stat.fileType == FileType.FILE)
                return CompletableFuture.completedFuture(List.of(stat));

            var objectKey = resolvePath(operationName, storagePath, true);
            var prefix = objectKey + BACKSLASH;

            return prefixLs(prefix, null, 1000, false, ctx);
        });
    }

    @Override
    public CompletionStage<Void>
    mkdir(String storagePath, boolean recursive, IExecutionContext ctx) {

        return wrapOperation("MKDIR", storagePath, (op, path) -> _mkdir(op, path, recursive, ctx));
    }

    private CompletionStage<Void>
    _mkdir(String operationName, String storagePath, Boolean recursive, IExecutionContext ctx) {

        // TODO: check parent if not recursive

        var objectKey = resolvePath(operationName, storagePath, false);
        var prefix = objectKey + BACKSLASH;

        var objectExists = objectExists(objectKey, ctx);
        var dirExists =  prefixExists(prefix, ctx);

        var fileExists = objectExists.thenCombine(dirExists, (obj, dir) -> obj && !dir);

        return fileExists.thenCompose(exists -> {

            if (exists)
                throw errors.explicitError(OBJECT_ALREADY_EXISTS, storagePath, operationName);

            return prefixMkdir(prefix, ctx);
        });
    }

    @Override
    public CompletionStage<Void>
    rm(String storagePath, boolean recursive, IExecutionContext ctx) {

        return wrapOperation("RM", storagePath, (op, path) -> _rm(op, path, recursive, ctx));
    }

    private CompletionStage<Void>
    _rm(String operationName, String storagePath, boolean recursive, IExecutionContext ctx) {

        var resolvedPath = resolvePath(operationName, storagePath, false);

        return null;
    }

    @Override
    public Flow.Publisher<ByteBuf> reader(String storagePath, IDataContext dataContext) {
        return null;
    }

    @Override
    public Flow.Subscriber<ByteBuf> writer(String storagePath, CompletableFuture<Long> signal, IDataContext dataContext) {
        return null;
    }

    private <TResult> CompletionStage<TResult>
    wrapOperation(String operationName, String storagePath, FsOperation<TResult> func) {

        var storageKey = "";
        var operation = String.format("%s %s [%s]", operationName, storageKey, storagePath);

        try {

            log.info("STORAGE OPERATION: {}", operation);

            var result = func.call(operationName, storagePath);

            return result.exceptionally(e -> {

                var error = errors.handleException(e, storagePath, operationName);
                log.error("{}: {}", operation, error.getMessage());

                throw error;
            });
        }
        catch (Exception e) {

            var error = errors.handleException(e, storagePath, operationName);
            log.error("{}: {}", operation, error.getMessage());

            return CompletableFuture.failedFuture(error);
        }
    }

    protected String resolvePath(String operationName, String storagePath, boolean allowRootDir) {


        try {

            var isWindows = System.getProperty("os.name").toLowerCase().contains("win");

            if (storagePath == null || storagePath.isBlank())
                throw errors.explicitError(STORAGE_PATH_NULL_OR_BLANK, storagePath, operationName);

//            if self._ILLEGAL_PATH_CHARS.match(storage_path):
//            raise self._explicit_error(self.ExplicitError.STORAGE_PATH_INVALID, operation_name, storage_path)

            var relativePath = Paths.get(storagePath);

            if (relativePath.isAbsolute())
                throw errors.explicitError(STORAGE_PATH_NOT_RELATIVE, storagePath, operationName);

            var rootPath = isWindows ? Paths.get("C:\\root") : Paths.get("/root");
            var absolutePath = rootPath.resolve(relativePath).normalize();

            if (absolutePath.equals(rootPath)) {

                if (!allowRootDir)
                    throw errors.explicitError(STORAGE_PATH_IS_ROOT, storagePath, operationName);

                return "";
            }
            else {

                if (!absolutePath.startsWith(rootPath))
                    throw errors.explicitError(STORAGE_PATH_OUTSIDE_ROOT, storagePath, operationName);

                var resolvedPath = rootPath.relativize(absolutePath);

                var sections = new ArrayList<String>(resolvedPath.getNameCount());
                resolvedPath.forEach(section -> sections.add(section.toString()));

                return String.join(BACKSLASH, sections);
            }
        }
        catch (InvalidPathException e) {

            throw errors.exception(STORAGE_PATH_INVALID, e, storagePath, operationName);
        }
    }

    public static <TResult> CompletionStage<TResult> useContext(IExecutionContext execCtx, CompletionStage<TResult> promise) {

        var ctxPromise = new CompletableFuture<TResult>();

        promise.whenComplete((result, error) -> {

            if (execCtx.eventLoopExecutor().inEventLoop()) {

                if (error != null)
                    ctxPromise.completeExceptionally(error);
                else
                    ctxPromise.complete(result);
            }
            else {

                if (error != null)
                    execCtx.eventLoopExecutor().execute(() -> ctxPromise.completeExceptionally(error));
                else
                    ctxPromise.completeAsync(() -> result, execCtx.eventLoopExecutor());
            }
        });

        return ctxPromise;
    }

    @FunctionalInterface
    private interface FsOperation<TResult> {

        CompletionStage<TResult> call(String operationName, String storagePath);
    }
}
