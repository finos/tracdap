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
import org.finos.tracdap.common.exception.ETrac;

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


public class CommonFileStorage implements IFileStorage {

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

        var resolvedPath = resolvePath(operationName, storagePath, true);
        var exists = fs.exists(resolvedPath, ctx);

        return useContext(ctx, exists);
    }

    @Override
    public CompletionStage<Long>
    size(String storagePath, IExecutionContext ctx) {

        return wrapOperation("SIZE", storagePath, (op, path) -> _size(op, path, ctx));
    }

    private CompletionStage<Long>
    _size(String operationName, String storagePath, IExecutionContext ctx) {

        var resolvedPath = resolvePath(operationName, storagePath, true);
        var stat = fs.stat(resolvedPath, ctx);

        return useContext(ctx, stat).thenApply(stat_ -> {

            if (stat_.fileType != FileType.FILE)
                throw errors.explicitError(SIZE_OF_DIR, storagePath, operationName);  // Todo

            return stat_.size;
        });
    }

    @Override
    public CompletionStage<FileStat>
    stat(String storagePath, IExecutionContext ctx) {

        return wrapOperation("STAT", storagePath, (op, path) -> _stat(op, path, ctx));
    }

    private CompletionStage<FileStat>
    _stat(String operationName, String storagePath, IExecutionContext ctx) {

        var resolvedPath = resolvePath(operationName, storagePath, true);
        var stat = fs.stat(resolvedPath, ctx);

        return useContext(ctx, stat);
    }


    @Override
    public CompletionStage<List<FileStat>>
    ls(String storagePath, IExecutionContext ctx) {

        return wrapOperation("LS", storagePath, (op, path) -> _ls(op, path, false, ctx));
    }

    private CompletionStage<List<FileStat>>
    _ls(String operationName, String storagePath, boolean recursive, IExecutionContext ctx) {

        var resolvedPath = resolvePath(operationName, storagePath, true);
        var stat = fs.stat(resolvedPath, ctx);

        return useContext(ctx, stat).thenCompose(fi -> {

            if (fi.fileType == FileType.FILE)
                return CompletableFuture.completedFuture(List.of(fi));

            var ls = fs.ls(resolvedPath, ctx);  // todo recursive

            return useContext(ctx, ls);
        });
    }

    @Override
    public CompletionStage<Void> mkdir(String storagePath, boolean recursive, IExecutionContext ctx) {

        return wrapOperation("MKDIR", storagePath, (op, path) -> _mkdir(op, path, recursive, ctx));
    }

    private CompletionStage<Void>
    _mkdir(String operationName, String storagePath, Boolean recursive, IExecutionContext ctx) {

        var resolvedPath = resolvePath(operationName, storagePath, false);
        var exists = fs.exists(resolvedPath, ctx);

        return useContext(ctx, exists).thenCompose(exists_ -> {

            if (exists_)
                throw errors.explicitError(FILE_ALREADY_EXISTS_EXCEPTION, storagePath, operationName);

            var mkdir = fs.mkdir(resolvedPath, recursive, ctx);

            return useContext(ctx, mkdir);
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

            log.info(operation);

            return func.call(operationName, storagePath);
        }
        catch (ETrac e) {
            log.error("{}: {}", operation, e.getMessage());
            throw e;
        }
        catch (Exception e) {
            var error = errors.handleException(e, storagePath, operationName);
            log.error("{}: {}", operation, error.getMessage());
            throw error;
        }
    }

    private String resolvePath(String operationName, String storagePath, boolean allowRootDir) {


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
