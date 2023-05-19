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

import org.finos.tracdap.common.concurrent.Flows;
import org.finos.tracdap.common.data.IExecutionContext;
import org.finos.tracdap.common.config.ConfigHelpers;
import org.finos.tracdap.common.data.IDataContext;
import org.finos.tracdap.common.exception.ETrac;

import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.finos.tracdap.common.storage.StorageErrors.ExplicitError.*;


/**
 * A common implementation of the file storage interface,
 * based on a set of abstract low-level operations
 *
 * <p>This is similar to the approach in Python, which provides a common wrapper
 * around the NativeFile interface in Apache Arrow. Although Arrow does not provide
 * an FS implementation in Java, it is still helpful to centralize core storage logic
 * to reduce duplication and increase compatibility. The abstract interface for
 * low-level operations is based on Arrow's NativeFile interface</p>
 */
public abstract class CommonFileStorage implements IFileStorage {

    public static final String BACKSLASH = "/";
    public static final String DOT = ".";
    public static final String DOUBLE_DOT = "..";

    public static final Pattern ILLEGAL_PATH_CHARS = Pattern.compile(".*[<>:'\"|?*\\\\].*");
    public static final Pattern UNICODE_CONTROL_CHARS = Pattern.compile(".*[\u0000-\u001f\u007f\u0080-\u009f].*");

    public static final String EXISTS_OPERATION = "exists";
    public static final String SIZE_OPERATION = "size";
    public static final String STAT_OPERATION = "stat";
    public static final String LS_OPERATION = "ls";
    public static final String MKDIR_OPERATION = "mkdir";
    public static final String RM_OPERATION = "rm";
    public static final String RMDIR_OPERATION = "rmdir";
    public static final String WRITE_OPERATION = "write";
    public static final String READ_OPERATION = "read";

    public static final String READ_ONLY_CONFIG_KEY = "readOnly";
    public static final boolean READ_ONLY_CONFIG_DEFAULT = false;

    protected static final boolean BUCKET_SEMANTICS = true;
    protected static final boolean FILE_SEMANTICS = false;

    // Abstract interface for low-level FS operations

    protected abstract CompletionStage<Boolean> fsExists(String objectKey, IExecutionContext ctx);
    protected abstract CompletionStage<Boolean> fsDirExists(String prefix, IExecutionContext ctx);
    protected abstract CompletionStage<FileStat> fsGetFileInfo(String objectKey, IExecutionContext ctx);
    protected abstract CompletionStage<FileStat> fsGetDirInfo(String prefix, IExecutionContext ctx);
    protected abstract CompletionStage<List<FileStat>> fsListContents(
            String prefix, String startAfter, int maxKeys, boolean recursive,
            IExecutionContext ctx);

    protected abstract CompletionStage<Void> fsCreateDir(String prefix, IExecutionContext ctx);
    protected abstract CompletionStage<Void> fsDeleteFile(String objectKey, IExecutionContext ctx);
    protected abstract CompletionStage<Void> fsDeleteDir(String directoryKey, IExecutionContext ctx);

    protected abstract CompletionStage<ArrowBuf> fsReadChunk(String objectKey, long offset, int size, IDataContext ctx);
    protected abstract Flow.Publisher<ArrowBuf> fsOpenInputStream(String objectKey, IDataContext ctx);
    protected abstract Flow.Subscriber<ArrowBuf> fsOpenOutputStream(String objectKey, CompletableFuture<Long> signal, IDataContext ctx);

    // Expose final member variables to avoid duplication in child classes

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final boolean bucketSemantics;
    protected final String storageKey;
    protected final boolean readOnly;

    protected final StorageErrors errors;

    protected CommonFileStorage(
            boolean bucketSemantics,
            String storageKey,
            Properties properties,
            StorageErrors errors) {

        this.bucketSemantics = bucketSemantics;
        this.storageKey = storageKey;
        this.readOnly = ConfigHelpers.optionalBoolean(
                storageKey, properties,
                READ_ONLY_CONFIG_KEY,
                READ_ONLY_CONFIG_DEFAULT);

        this.errors = errors;
    }


    @Override
    public CompletionStage<Boolean>
    exists(String storagePath, IExecutionContext ctx) {

        return wrapOperation(EXISTS_OPERATION, storagePath, (op, path) -> exists(op, path, ctx));
    }

    private CompletionStage<Boolean>
    exists(String operationName, String storagePath, IExecutionContext ctx) {

        var objectKey = resolveObjectKey(operationName, storagePath, true);
        var prefix = resolveDirPrefix(objectKey);

        if (storagePath.endsWith(BACKSLASH)) {

            return fsDirExists(prefix, ctx).thenCompose(
                exists -> exists
                    ? CompletableFuture.completedFuture(true)
                    : fsExists(objectKey, ctx));
        }
        else {

            return fsExists(objectKey, ctx).thenCompose(
                exists -> exists
                    ? CompletableFuture.completedFuture(true)
                    : fsDirExists(prefix, ctx));
        }
    }

    @Override
    public CompletionStage<FileStat>
    stat(String storagePath, IExecutionContext ctx) {

        return wrapOperation(STAT_OPERATION, storagePath, (op, path) -> stat(op, path, ctx));
    }

    private CompletionStage<FileStat>
    stat(String operationName, String storagePath, IExecutionContext ctx) {

        var objectKey = resolveObjectKey(operationName, storagePath, true);
        var prefix = resolveDirPrefix(objectKey);

        return fsDirExists(prefix, ctx).thenCompose(isDir -> isDir
                ? fsGetDirInfo(prefix, ctx)
                : fsGetFileInfo(objectKey, ctx));
    }

    @Override
    public CompletionStage<Long>
    size(String storagePath, IExecutionContext ctx) {

        return wrapOperation(SIZE_OPERATION, storagePath, (op, path) -> size(op, path, ctx));
    }

    private CompletionStage<Long>
    size(String operationName, String storagePath, IExecutionContext ctx) {

        var _stat = stat(operationName, storagePath, ctx);

        return _stat.thenApply(stat -> {

            if (stat.fileType != FileType.FILE)
                throw errors.explicitError(operationName, storagePath, NOT_A_FILE);

            return stat.size;
        });
    }

    @Override
    public CompletionStage<List<FileStat>>
    ls(String storagePath, IExecutionContext ctx) {

        return wrapOperation(LS_OPERATION, storagePath, (op, path) -> ls(op, path, false, ctx));
    }

    private CompletionStage<List<FileStat>>
    ls(String operationName, String storagePath, boolean recursive, IExecutionContext ctx) {

        var _stat = stat(operationName, storagePath, ctx);

        return _stat.thenCompose(stat -> {

            if (stat.fileType == FileType.FILE)
                return CompletableFuture.completedFuture(List.of(stat));

            var objectKey = resolveObjectKey(operationName, storagePath, true);
            var prefix = resolveDirPrefix(objectKey);

            return fsListContents(prefix, null, 1000, false, ctx);
        });
    }

    @Override
    public CompletionStage<Void>
    mkdir(String storagePath, boolean recursive, IExecutionContext ctx) {

        return wrapOperation(MKDIR_OPERATION, storagePath, (op, path) -> mkdir(op, path, recursive, ctx));
    }

    private CompletionStage<Void>
    mkdir(String operationName, String storagePath, Boolean recursive, IExecutionContext ctx) {

        var path = resolveObjectKey(operationName, storagePath, false);

        if (readOnly)
            throw errors.explicitError(operationName, storagePath, ACCESS_DENIED);

        var parent = path.contains(BACKSLASH) ? path.substring(0, path.lastIndexOf(BACKSLASH)) : null;
        var checkParent = path.contains(BACKSLASH) && !recursive
                ? fsDirExists(parent, ctx)
                : CompletableFuture.completedFuture(true);

        return checkParent.thenCompose(parentOk -> {

            if (!parentOk)
                throw errors.explicitError(operationName, storagePath, OBJECT_NOT_FOUND);

            return mkdirParentOk(operationName, storagePath, path, ctx);
        });
    }

    private CompletionStage<Void>
    mkdirParentOk(String operationName, String storagePath, String resolvedPath, IExecutionContext ctx) {

        var prefix = resolveDirPrefix(resolvedPath);

        var objectExists = fsExists(resolvedPath, ctx);
        var dirExists =  fsDirExists(prefix, ctx);
        var fileExists = objectExists.thenCombine(dirExists, (obj, dir) -> obj && !dir);

        return fileExists.thenCompose(exists -> {

            if (exists)
                throw errors.explicitError(operationName, storagePath, OBJECT_ALREADY_EXISTS);

            return fsCreateDir(prefix, ctx);
        });
    }

    @Override
    public CompletionStage<Void>
    rm(String storagePath, IExecutionContext ctx) {

        return wrapOperation(RM_OPERATION, storagePath, (op, path) -> rm(op, path, ctx));
    }

    private CompletionStage<Void>
    rm(String operationName, String storagePath, IExecutionContext ctx) {

        var objectKey = resolveObjectKey(operationName, storagePath, false);

        if (readOnly)
            throw errors.explicitError(operationName, storagePath, ACCESS_DENIED);

        var fileInfo = stat(operationName, storagePath, ctx);

        return fileInfo.thenCompose(fi -> {

            if (fi.fileType != FileType.FILE)
                throw errors.explicitError(operationName, storagePath, NOT_A_FILE);

            return fsDeleteFile(objectKey, ctx);
        });
    }

    @Override
    public CompletionStage<Void>
    rmdir(String storagePath, IExecutionContext ctx) {

        return wrapOperation(RMDIR_OPERATION, storagePath, (op, path) -> rmdir(op, path, ctx));
    }

    private CompletionStage<Void>
    rmdir(String operationName, String storagePath, IExecutionContext ctx) {

        var objectKey = resolveObjectKey(operationName, storagePath, false);
        var dirPrefix = resolveDirPrefix(objectKey);

        if (readOnly)
            throw errors.explicitError(operationName, storagePath, ACCESS_DENIED);

        var fileInfo = stat(operationName, storagePath, ctx);

        return fileInfo.thenCompose(fi -> {

            if (fi.fileType != FileType.DIRECTORY)
                throw errors.explicitError(operationName, storagePath, NOT_A_DIRECTORY);

            return fsDeleteDir(dirPrefix, ctx);
        });
    }

    @Override
    public CompletionStage<ArrowBuf> readChunk(String storagePath, long offset, int size, IDataContext ctx) {

        return wrapOperation(READ_OPERATION, storagePath, (op, path) -> readChunk(op, path, offset, size, ctx));
    }

    private CompletionStage<ArrowBuf> readChunk(String operationName, String storagePath, long offset, int size, IDataContext ctx) {

        var objectKey = resolveObjectKey(operationName, storagePath, false);

        if (offset < 0 || size <= 0) {
            var detail = String.format("offset = %d, size = %d", offset, size);
            throw errors.explicitError(operationName, storagePath, STORAGE_PARAMS_INVALID, detail);
        }

        var checkFile = stat(operationName, storagePath, ctx).thenAccept(fi -> {

            if (fi.fileType != FileType.FILE)
                throw errors.explicitError(operationName, storagePath, NOT_A_FILE);
        });

        return checkFile.thenCompose(x -> fsReadChunk(objectKey, offset, size, ctx));
    }

    @Override
    public Flow.Publisher<ArrowBuf>
    reader(String storagePath, IDataContext ctx) {

        return wrapStreamOperation(
                READ_OPERATION, storagePath,
                (op, path) -> reader(op, path, ctx),
                err -> { throw err; });
    }

    private Flow.Publisher<ArrowBuf>
    reader(String operationName, String storagePath, IDataContext dataContext) {

        var objectKey = resolveObjectKey(operationName, storagePath, false);

        return fsOpenInputStream(objectKey, dataContext);
    }

    @Override
    public Flow.Subscriber<ArrowBuf>
    writer(String storagePath, CompletableFuture<Long> signal, IDataContext ctx) {

        return wrapStreamOperation(
                WRITE_OPERATION, storagePath,
                (op, path) -> writer(op, path, signal, ctx),
                err -> { throw err; });
    }

    protected Flow.Subscriber<ArrowBuf>
    writer(String operationName, String storagePath, CompletableFuture<Long> signal, IDataContext ctx) {

        var objectKey = resolveObjectKey(operationName, storagePath, false);
        var parent = objectKey.contains(BACKSLASH) ? objectKey.substring(0, objectKey.lastIndexOf(BACKSLASH)) : null;

        // Before opening the write stream several checks are needed

        // Check storage is not readOnly - if it is, return an error before anything else is attempted
        var prepare = CompletableFuture.completedFuture(true).thenApply(x -> {

            if (readOnly)
                throw errors.explicitError(operationName, storagePath, ACCESS_DENIED);

            return true;

        // Check whether a parent directory is needed (in bucket semantics parents are never needed)
        }).thenCompose(x -> {

            if (parent == null || bucketSemantics)
                return CompletableFuture.completedFuture(true);
            else
                return fsDirExists(parent, ctx);

        // If a parent dir is needed, create one (this makes file semantics behave like buckets)
        }).thenCompose(parentOk -> {

            if (!parentOk)
                return fsCreateDir(parent, ctx);
            else
                return CompletableFuture.completedFuture(null);

        // Make sure the file being writen to is not a directory
        // With bucket semantics, a file and a directory can have the same name
        // This explicit check tries to prevent that confusion
        // Race conditions are possible, but the structured way TRAC uses storage makes that unlikely in practice
        }).thenCompose(x -> fsDirExists(objectKey + BACKSLASH, ctx)).thenApply(isDir -> {

            if (isDir)
                throw errors.explicitError(operationName, storagePath, OBJECT_ALREADY_EXISTS);

            return true;
        });

        // Create the output stream - it will not activate until it is subscribed to a source
        var outputStream = fsOpenOutputStream(objectKey, fromContext(ctx, signal), ctx);

        // Return a delayed subscriber, that waits for the prepare step to finish before starting
        return Flows.waitForSignal(outputStream, toContext(ctx, prepare));
    }

    private <TResult> CompletionStage<TResult>
    wrapOperation(String operationName, String storagePath, FsOperation<TResult> func) {

        var operation = String.format("%s %s [%s]", operationName, storageKey, storagePath);

        try {

            log.info(operation);

            var result = func.call(operationName, storagePath);

            return result.exceptionally(e -> {

                var error = errors.handleException(operationName, storagePath, e);
                log.error("{}: {}", operation, error.getMessage(), error);

                throw error;
            });
        }
        catch (Exception e) {

            var error = errors.handleException(operationName, storagePath, e);
            log.error("{}: {}", operation, error.getMessage(), error);

            return CompletableFuture.failedFuture(error);
        }
    }

    private <TResult> TResult
    wrapStreamOperation(
            String operationName, String storagePath,
            FsStreamOperation<TResult> func,
            Function<ETrac, TResult> errFunc) {

        var operation = String.format("%s %s [%s]", operationName, storageKey, storagePath);

        try {

            log.info(operation);

            return func.call(operationName, storagePath);
        }
        catch (Exception e) {

            var error = errors.handleException(operationName, storagePath, e);
            log.error("{}: {}", operation, error.getMessage(), error);

            return errFunc.apply(error);
        }
    }

    private String resolveObjectKey(String operationName, String storagePath, boolean allowRootDir) {

        if (storagePath == null || storagePath.isBlank())
            throw errors.explicitError(operationName, storagePath, STORAGE_PATH_NULL_OR_BLANK);

        if (ILLEGAL_PATH_CHARS.matcher(storagePath).matches())
            throw errors.explicitError(operationName, storagePath, STORAGE_PATH_INVALID);

        if (UNICODE_CONTROL_CHARS.matcher(storagePath).matches())
            throw errors.explicitError(operationName, storagePath, STORAGE_PATH_INVALID);

        // Absolute paths of the form C:\foo, \foo or \\foo\bar are not possible,
        // because ':' and '\' are in ILLEGAL_PATH_CHARS

        if (storagePath.startsWith(BACKSLASH))
            throw errors.explicitError(operationName, storagePath, STORAGE_PATH_NOT_RELATIVE);

        var resolvedPath = normalizeStoragePath(operationName, storagePath);

        if (resolvedPath.isEmpty() && !allowRootDir)
            throw errors.explicitError(operationName, storagePath, STORAGE_PATH_IS_ROOT);

        return resolvedPath;
    }

    private String normalizeStoragePath(String operationName, String storagePath) {

        var rawSections = storagePath.split(BACKSLASH);
        var normalSections = new ArrayList<String>(rawSections.length);

        for (var section : rawSections) {

            if (section.equals(DOT))
                continue;

            if (section.equals(DOUBLE_DOT)) {

                if (normalSections.isEmpty())
                    throw errors.explicitError(operationName, storagePath, STORAGE_PATH_OUTSIDE_ROOT);
                else
                    normalSections.remove(normalSections.size() - 1);
            }
            else {
                normalSections.add(section);
            }
        }

        return String.join(BACKSLASH, normalSections);
    }

    protected String resolveDirPrefix(String objectKey) {

        if (objectKey.isEmpty() || objectKey.endsWith(BACKSLASH))
            return objectKey;
        else
            return objectKey + BACKSLASH;
    }

    protected <TResult> CompletableFuture<TResult> toContext(IExecutionContext execCtx, CompletionStage<TResult> promise) {

        return execCtx.toContext(promise);
    }

    protected <TResult> CompletableFuture<TResult> fromContext(IExecutionContext execCtx, CompletableFuture<TResult> ctxPromise) {

        return execCtx.fromContext(ctxPromise);
    }

    @FunctionalInterface
    private interface FsOperation<TResult> {

        CompletionStage<TResult> call(String operationName, String storagePath);
    }

    @FunctionalInterface
    private interface FsStreamOperation<TResult> {

        TResult call(String operationName, String storagePath);
    }
}
